#include "internal.h"

#include <limits.h>
#include <stddef.h>
#include <string.h>

enum {
    CD_DEFAULT_QUEUE_CAPACITY = 1024,
    CD_DEFAULT_SUBSCRIPTION_CAPACITY = 256
};

static bool size_mul_overflow(size_t a, size_t b, size_t *out_result)
{
    if (a > 0u && b > SIZE_MAX / a) {
        return true;
    }

    *out_result = a * b;
    return false;
}

static bool kind_mask_valid(uint32_t kind_mask)
{
    return kind_mask != 0u && (kind_mask & ~CD_MESSAGE_KIND_ALL_MASK) == 0u;
}

static cd_message_id_t next_message_id(cd_bus_t *bus)
{
    cd_message_id_t id;

    id = bus->next_message_id++;
    if (id == 0) {
        id = bus->next_message_id++;
    }

    return id;
}

static bool subscription_matches_message(
    const cd_subscription_entry_t *entry,
    const cd_envelope_t *message
)
{
    uint32_t message_mask;

    if (!entry->in_use) {
        return false;
    }

    message_mask = CD_MESSAGE_KIND_MASK(message->kind);
    if ((entry->desc.kind_mask & message_mask) == 0u) {
        return false;
    }
    if (entry->desc.topic != message->topic) {
        return false;
    }
    if (message->kind != CD_MESSAGE_EVENT &&
        message->target_endpoint != CD_ENDPOINT_NONE &&
        entry->desc.endpoint != message->target_endpoint) {
        return false;
    }

    return true;
}

static bool dispatch_target_less(
    const cd_bus_t *bus,
    const cd_dispatch_target_t *lhs,
    const cd_dispatch_target_t *rhs
)
{
    const cd_subscription_entry_t *left_entry;
    const cd_subscription_entry_t *right_entry;

    if (lhs->subscription_id != rhs->subscription_id) {
        return lhs->subscription_id < rhs->subscription_id;
    }

    left_entry = &bus->subscriptions[lhs->index];
    right_entry = &bus->subscriptions[rhs->index];
    if (left_entry->desc.endpoint != right_entry->desc.endpoint) {
        return left_entry->desc.endpoint < right_entry->desc.endpoint;
    }

    return lhs->index < rhs->index;
}

static size_t collect_dispatch_targets(cd_bus_t *bus, const cd_envelope_t *message)
{
    size_t i;
    size_t target_count;

    target_count = 0u;
    for (i = 0; i < bus->subscription_capacity; ++i) {
        cd_dispatch_target_t target;
        size_t position;

        if (!subscription_matches_message(&bus->subscriptions[i], message)) {
            continue;
        }

        target.index = i;
        target.subscription_id = bus->subscriptions[i].subscription_id;

        position = target_count;
        while (position > 0u &&
               dispatch_target_less(bus, &target, &bus->dispatch_targets[position - 1u])) {
            bus->dispatch_targets[position] = bus->dispatch_targets[position - 1u];
            position -= 1u;
        }
        bus->dispatch_targets[position] = target;
        target_count += 1u;
    }

    return target_count;
}

static cd_status_t dispatch_message_to_subscribers(
    cd_bus_t *bus,
    const cd_envelope_t *message,
    cd_status_t current_status
)
{
    size_t i;
    size_t target_count;

    target_count = collect_dispatch_targets(bus, message);
    for (i = 0; i < target_count; ++i) {
        const cd_dispatch_target_t *target;
        const cd_subscription_entry_t *entry;
        cd_status_t callback_status;

        target = &bus->dispatch_targets[i];
        entry = &bus->subscriptions[target->index];
        if (!entry->in_use || entry->subscription_id != target->subscription_id) {
            continue;
        }

        callback_status = entry->desc.handler(entry->desc.user_data, message);
        if (current_status == CD_STATUS_OK && callback_status != CD_STATUS_OK) {
            current_status = callback_status;
        }
    }

    return current_status;
}

static cd_status_t enqueue_message(
    cd_bus_t *bus,
    cd_message_kind_t kind,
    cd_endpoint_id_t source_endpoint,
    cd_endpoint_id_t target_endpoint,
    cd_topic_t topic,
    uint32_t schema_id,
    uint16_t schema_version,
    uint16_t flags,
    const void *payload,
    size_t payload_size,
    cd_message_id_t *out_message_id
)
{
    size_t slot_index;
    cd_queued_message_t *slot;
    void *payload_copy;

    if (out_message_id != NULL) {
        *out_message_id = 0;
    }

    if (bus == NULL || (payload_size > 0u && payload == NULL)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    if (bus->queue_count == bus->queue_capacity) {
        return CD_STATUS_QUEUE_FULL;
    }

    slot_index = (bus->queue_head + bus->queue_count) % bus->queue_capacity;
    slot = &bus->queue[slot_index];

    payload_copy = NULL;
    if (payload_size > 0u) {
        payload_copy = cd_context_alloc(bus->context, payload_size);
        if (payload_copy == NULL) {
            return CD_STATUS_ALLOCATION_FAILED;
        }
        memcpy(payload_copy, payload, payload_size);
    }

    slot->payload_copy = payload_copy;
    slot->envelope.message_id = next_message_id(bus);
    slot->envelope.correlation_id = 0;
    slot->envelope.kind = kind;
    slot->envelope.topic = topic;
    slot->envelope.source_endpoint = source_endpoint;
    slot->envelope.target_endpoint = target_endpoint;
    slot->envelope.schema_id = schema_id;
    slot->envelope.schema_version = schema_version;
    slot->envelope.flags = flags;
    slot->envelope.timestamp_ns = cd_context_now_ns(bus->context);
    slot->envelope.payload = payload_copy;
    slot->envelope.payload_size = payload_size;

    bus->queue_count += 1u;

    if (out_message_id != NULL) {
        *out_message_id = slot->envelope.message_id;
    }

    return CD_STATUS_OK;
}

cd_status_t cd_bus_create(cd_context_t *context, const cd_bus_config_t *config, cd_bus_t **out_bus)
{
    cd_bus_t *bus;
    size_t queue_capacity;
    size_t subscription_capacity;
    size_t queue_alloc_size;
    size_t subscription_alloc_size;
    size_t dispatch_target_alloc_size;

    if (context == NULL || out_bus == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    *out_bus = NULL;

    queue_capacity = CD_DEFAULT_QUEUE_CAPACITY;
    subscription_capacity = CD_DEFAULT_SUBSCRIPTION_CAPACITY;

    if (config != NULL) {
        if (config->max_queued_messages > 0u) {
            queue_capacity = config->max_queued_messages;
        }
        if (config->max_subscriptions > 0u) {
            subscription_capacity = config->max_subscriptions;
        }
    }

    if (size_mul_overflow(queue_capacity, sizeof(*bus->queue), &queue_alloc_size) ||
        size_mul_overflow(
            subscription_capacity,
            sizeof(*bus->subscriptions),
            &subscription_alloc_size
        ) ||
        size_mul_overflow(
            subscription_capacity,
            sizeof(*bus->dispatch_targets),
            &dispatch_target_alloc_size
        )) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    bus = (cd_bus_t *)cd_context_alloc(context, sizeof(*bus));
    if (bus == NULL) {
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(bus, 0, sizeof(*bus));

    bus->context = context;
    bus->queue_capacity = queue_capacity;
    bus->subscription_capacity = subscription_capacity;
    bus->next_subscription_id = 1u;
    bus->next_message_id = 1u;

    bus->queue = (cd_queued_message_t *)cd_context_alloc(context, queue_alloc_size);
    if (bus->queue == NULL) {
        cd_context_free(context, bus);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(bus->queue, 0, queue_alloc_size);

    bus->subscriptions = (cd_subscription_entry_t *)cd_context_alloc(context, subscription_alloc_size);
    if (bus->subscriptions == NULL) {
        cd_context_free(context, bus->queue);
        cd_context_free(context, bus);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(bus->subscriptions, 0, subscription_alloc_size);

    bus->dispatch_targets = (cd_dispatch_target_t *)cd_context_alloc(context, dispatch_target_alloc_size);
    if (bus->dispatch_targets == NULL) {
        cd_context_free(context, bus->subscriptions);
        cd_context_free(context, bus->queue);
        cd_context_free(context, bus);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(bus->dispatch_targets, 0, dispatch_target_alloc_size);

    *out_bus = bus;
    return CD_STATUS_OK;
}

void cd_bus_destroy(cd_bus_t *bus)
{
    size_t i;

    if (bus == NULL) {
        return;
    }

    for (i = 0; i < bus->queue_capacity; ++i) {
        if (bus->queue[i].payload_copy != NULL) {
            cd_context_free(bus->context, bus->queue[i].payload_copy);
            bus->queue[i].payload_copy = NULL;
        }
    }

    cd_context_free(bus->context, bus->queue);
    cd_context_free(bus->context, bus->subscriptions);
    cd_context_free(bus->context, bus->dispatch_targets);
    cd_context_free(bus->context, bus);
}

cd_status_t cd_bus_pump(cd_bus_t *bus, size_t max_messages, size_t *out_processed)
{
    size_t processed;
    size_t limit;
    size_t queue_snapshot_count;
    cd_status_t dispatch_status;

    if (out_processed != NULL) {
        *out_processed = 0u;
    }

    if (bus == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    queue_snapshot_count = bus->queue_count;
    if (max_messages == 0u) {
        limit = queue_snapshot_count;
    } else if (max_messages < queue_snapshot_count) {
        limit = max_messages;
    } else {
        limit = queue_snapshot_count;
    }

    processed = 0u;
    dispatch_status = CD_STATUS_OK;
    while (processed < limit && bus->queue_count > 0u) {
        cd_queued_message_t *message;

        message = &bus->queue[bus->queue_head];
        dispatch_status = dispatch_message_to_subscribers(bus, &message->envelope, dispatch_status);

        if (message->payload_copy != NULL) {
            cd_context_free(bus->context, message->payload_copy);
        }
        memset(message, 0, sizeof(*message));

        bus->queue_head = (bus->queue_head + 1u) % bus->queue_capacity;
        bus->queue_count -= 1u;
        processed += 1u;
    }

    if (out_processed != NULL) {
        *out_processed = processed;
    }

    return dispatch_status;
}

cd_status_t cd_publish(cd_bus_t *bus, const cd_publish_params_t *params, cd_message_id_t *out_message_id)
{
    if (out_message_id != NULL) {
        *out_message_id = 0;
    }

    if (params == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    return enqueue_message(
        bus,
        CD_MESSAGE_EVENT,
        params->source_endpoint,
        CD_ENDPOINT_NONE,
        params->topic,
        params->schema_id,
        params->schema_version,
        params->flags,
        params->payload,
        params->payload_size,
        out_message_id
    );
}

cd_status_t cd_send_command(
    cd_bus_t *bus,
    const cd_command_params_t *params,
    cd_message_id_t *out_message_id
)
{
    if (out_message_id != NULL) {
        *out_message_id = 0;
    }

    if (params == NULL || params->target_endpoint == CD_ENDPOINT_NONE) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    return enqueue_message(
        bus,
        CD_MESSAGE_COMMAND,
        params->source_endpoint,
        params->target_endpoint,
        params->topic,
        params->schema_id,
        params->schema_version,
        params->flags,
        params->payload,
        params->payload_size,
        out_message_id
    );
}

cd_status_t cd_request_async(
    cd_bus_t *bus,
    const cd_request_params_t *params,
    cd_request_token_t *out_token
)
{
    if (out_token != NULL) {
        *out_token = 0;
    }

    if (bus == NULL || params == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    return CD_STATUS_NOT_IMPLEMENTED;
}

cd_status_t cd_poll_reply(
    cd_bus_t *bus,
    cd_request_token_t token,
    cd_reply_t *out_reply,
    int *out_ready
)
{
    if (out_reply != NULL) {
        memset(out_reply, 0, sizeof(*out_reply));
    }
    (void)token;
    if (out_ready != NULL) {
        *out_ready = 0;
    }

    if (bus == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    return CD_STATUS_NOT_IMPLEMENTED;
}

cd_status_t cd_subscribe(
    cd_bus_t *bus,
    const cd_subscription_desc_t *desc,
    cd_subscription_id_t *out_subscription_id
)
{
    size_t i;
    uint32_t directed_message_mask;

    if (out_subscription_id != NULL) {
        *out_subscription_id = 0;
    }

    if (bus == NULL || desc == NULL || desc->handler == NULL || !kind_mask_valid(desc->kind_mask)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    directed_message_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_COMMAND) |
                            CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST) |
                            CD_MESSAGE_KIND_MASK(CD_MESSAGE_REPLY);
    if (desc->endpoint == CD_ENDPOINT_NONE && (desc->kind_mask & directed_message_mask) != 0u) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    for (i = 0; i < bus->subscription_capacity; ++i) {
        cd_subscription_entry_t *entry;

        entry = &bus->subscriptions[i];
        if (entry->in_use) {
            continue;
        }

        entry->in_use = true;
        entry->subscription_id = bus->next_subscription_id++;
        if (entry->subscription_id == 0u) {
            entry->subscription_id = bus->next_subscription_id++;
        }
        entry->desc = *desc;

        if (out_subscription_id != NULL) {
            *out_subscription_id = entry->subscription_id;
        }
        return CD_STATUS_OK;
    }

    return CD_STATUS_CAPACITY_REACHED;
}

cd_status_t cd_unsubscribe(cd_bus_t *bus, cd_subscription_id_t subscription_id)
{
    size_t i;

    if (bus == NULL || subscription_id == 0u) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    for (i = 0; i < bus->subscription_capacity; ++i) {
        cd_subscription_entry_t *entry;

        entry = &bus->subscriptions[i];
        if (!entry->in_use || entry->subscription_id != subscription_id) {
            continue;
        }

        memset(entry, 0, sizeof(*entry));
        return CD_STATUS_OK;
    }

    return CD_STATUS_NOT_FOUND;
}
