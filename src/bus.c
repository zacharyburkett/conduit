#include "internal.h"

#include <limits.h>
#include <string.h>

enum {
    CD_DEFAULT_QUEUE_CAPACITY = 1024,
    CD_DEFAULT_SUBSCRIPTION_CAPACITY = 256
};

static cd_message_id_t next_message_id(cd_bus_t *bus)
{
    cd_message_id_t id;

    id = bus->next_message_id++;
    if (id == 0) {
        id = bus->next_message_id++;
    }

    return id;
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

    if (context == NULL || out_bus == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

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

    bus->queue = (cd_queued_message_t *)cd_context_alloc(context, queue_capacity * sizeof(*bus->queue));
    if (bus->queue == NULL) {
        cd_context_free(context, bus);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(bus->queue, 0, queue_capacity * sizeof(*bus->queue));

    bus->subscriptions = (cd_subscription_entry_t *)cd_context_alloc(
        context,
        subscription_capacity * sizeof(*bus->subscriptions)
    );
    if (bus->subscriptions == NULL) {
        cd_context_free(context, bus->queue);
        cd_context_free(context, bus);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(bus->subscriptions, 0, subscription_capacity * sizeof(*bus->subscriptions));

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
    cd_context_free(bus->context, bus);
}

cd_status_t cd_bus_pump(cd_bus_t *bus, size_t max_messages, size_t *out_processed)
{
    size_t processed;
    size_t limit;
    cd_status_t dispatch_status;

    if (bus == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    limit = max_messages;
    if (limit == 0u) {
        limit = SIZE_MAX;
    }

    processed = 0u;
    dispatch_status = CD_STATUS_OK;
    while (bus->queue_count > 0u && processed < limit) {
        cd_queued_message_t *message;
        size_t i;

        message = &bus->queue[bus->queue_head];

        for (i = 0; i < bus->subscription_capacity; ++i) {
            const cd_subscription_entry_t *entry;
            uint32_t message_mask;
            cd_status_t callback_status;

            entry = &bus->subscriptions[i];
            if (!entry->in_use) {
                continue;
            }

            message_mask = CD_MESSAGE_KIND_MASK(message->envelope.kind);
            if ((entry->desc.kind_mask & message_mask) == 0u) {
                continue;
            }
            if (entry->desc.topic != message->envelope.topic) {
                continue;
            }
            if (message->envelope.kind != CD_MESSAGE_EVENT &&
                message->envelope.target_endpoint != CD_ENDPOINT_NONE &&
                entry->desc.endpoint != message->envelope.target_endpoint) {
                continue;
            }

            callback_status = entry->desc.handler(entry->desc.user_data, &message->envelope);
            if (dispatch_status == CD_STATUS_OK && callback_status != CD_STATUS_OK) {
                dispatch_status = callback_status;
            }
        }

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
    (void)bus;
    (void)params;
    if (out_token != NULL) {
        *out_token = 0;
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
    (void)bus;
    (void)token;
    (void)out_reply;
    if (out_ready != NULL) {
        *out_ready = 0;
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

    if (bus == NULL || desc == NULL || desc->handler == NULL || desc->kind_mask == 0u) {
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
