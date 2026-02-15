#include "internal.h"

#include <stddef.h>
#include <string.h>

typedef struct cd_inproc_message {
    cd_envelope_t envelope;
    void *payload_copy;
} cd_inproc_message_t;

typedef struct cd_inproc_peer {
    struct cd_inproc_hub *hub;
    size_t peer_index;
    bool in_use;
    cd_inproc_message_t *queue;
    size_t queue_capacity;
    size_t queue_head;
    size_t queue_count;
} cd_inproc_peer_t;

struct cd_inproc_hub {
    cd_context_t *context;
    cd_inproc_peer_t *peers;
    size_t peer_capacity;
    size_t per_peer_queue_capacity;
};

enum {
    CD_INPROC_DEFAULT_MAX_PEERS = 8,
    CD_INPROC_DEFAULT_MAX_QUEUED_PER_PEER = 256
};

static bool size_mul_overflow(size_t a, size_t b, size_t *out_result)
{
    if (a > 0u && b > SIZE_MAX / a) {
        return true;
    }

    *out_result = a * b;
    return false;
}

static void inproc_clear_message(cd_context_t *context, cd_inproc_message_t *message)
{
    if (message->payload_copy != NULL) {
        cd_context_free(context, message->payload_copy);
    }
    memset(message, 0, sizeof(*message));
}

static void inproc_peer_reset_queue(cd_inproc_peer_t *peer)
{
    size_t i;

    if (peer->queue == NULL) {
        return;
    }

    for (i = 0; i < peer->queue_capacity; ++i) {
        inproc_clear_message(peer->hub->context, &peer->queue[i]);
    }

    cd_context_free(peer->hub->context, peer->queue);
    peer->queue = NULL;
    peer->queue_capacity = 0u;
    peer->queue_head = 0u;
    peer->queue_count = 0u;
}

static cd_status_t inproc_peer_enqueue(cd_inproc_peer_t *peer, const cd_envelope_t *message)
{
    size_t slot_index;
    cd_inproc_message_t *slot;
    void *payload_copy;

    if (!peer->in_use || peer->queue == NULL || message == NULL ||
        (message->payload_size > 0u && message->payload == NULL)) {
        return CD_STATUS_TRANSPORT_UNAVAILABLE;
    }
    if (peer->queue_count == peer->queue_capacity) {
        return CD_STATUS_QUEUE_FULL;
    }

    slot_index = (peer->queue_head + peer->queue_count) % peer->queue_capacity;
    slot = &peer->queue[slot_index];

    payload_copy = NULL;
    if (message->payload_size > 0u) {
        payload_copy = cd_context_alloc(peer->hub->context, message->payload_size);
        if (payload_copy == NULL) {
            return CD_STATUS_ALLOCATION_FAILED;
        }
        memcpy(payload_copy, message->payload, message->payload_size);
    }

    slot->payload_copy = payload_copy;
    slot->envelope = *message;
    slot->envelope.payload = payload_copy;
    peer->queue_count += 1u;
    return CD_STATUS_OK;
}

static cd_status_t inproc_send(void *impl, const cd_envelope_t *message)
{
    cd_inproc_peer_t *sender;
    cd_inproc_hub_t *hub;
    cd_status_t status;
    size_t i;

    sender = (cd_inproc_peer_t *)impl;
    if (sender == NULL || message == NULL || !sender->in_use) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    hub = sender->hub;
    status = CD_STATUS_OK;
    for (i = 0; i < hub->peer_capacity; ++i) {
        cd_inproc_peer_t *peer;
        cd_status_t enqueue_status;

        peer = &hub->peers[i];
        if (!peer->in_use || peer == sender) {
            continue;
        }

        enqueue_status = inproc_peer_enqueue(peer, message);
        if (status == CD_STATUS_OK && enqueue_status != CD_STATUS_OK) {
            status = enqueue_status;
        }
    }

    return status;
}

static cd_status_t inproc_poll(
    void *impl,
    cd_transport_receive_fn receive_fn,
    void *receive_user_data,
    size_t max_messages,
    size_t *out_polled_count
)
{
    cd_inproc_peer_t *peer;
    size_t limit;
    size_t processed;

    peer = (cd_inproc_peer_t *)impl;
    if (out_polled_count != NULL) {
        *out_polled_count = 0u;
    }

    if (peer == NULL || !peer->in_use || receive_fn == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    limit = max_messages;
    if (limit == 0u || limit > peer->queue_count) {
        limit = peer->queue_count;
    }

    processed = 0u;
    while (processed < limit && peer->queue_count > 0u) {
        cd_inproc_message_t *message;
        cd_status_t receive_status;

        message = &peer->queue[peer->queue_head];
        receive_status = receive_fn(receive_user_data, &message->envelope);

        inproc_clear_message(peer->hub->context, message);
        peer->queue_head = (peer->queue_head + 1u) % peer->queue_capacity;
        peer->queue_count -= 1u;
        processed += 1u;

        if (receive_status != CD_STATUS_OK) {
            if (out_polled_count != NULL) {
                *out_polled_count = processed;
            }
            return receive_status;
        }
    }

    if (out_polled_count != NULL) {
        *out_polled_count = processed;
    }
    return CD_STATUS_OK;
}

static cd_status_t inproc_flush(void *impl)
{
    cd_inproc_peer_t *peer;

    peer = (cd_inproc_peer_t *)impl;
    if (peer == NULL || !peer->in_use) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    return CD_STATUS_OK;
}

static void inproc_shutdown(void *impl)
{
    cd_inproc_peer_t *peer;

    peer = (cd_inproc_peer_t *)impl;
    if (peer == NULL || !peer->in_use) {
        return;
    }

    inproc_peer_reset_queue(peer);
    peer->in_use = false;
}

cd_status_t cd_inproc_hub_init(
    cd_context_t *context,
    const cd_inproc_hub_config_t *config,
    cd_inproc_hub_t **out_hub
)
{
    cd_inproc_hub_t *hub;
    size_t max_peers;
    size_t max_queued_per_peer;
    size_t peers_alloc_size;

    if (out_hub == NULL || context == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    *out_hub = NULL;

    max_peers = CD_INPROC_DEFAULT_MAX_PEERS;
    max_queued_per_peer = CD_INPROC_DEFAULT_MAX_QUEUED_PER_PEER;
    if (config != NULL) {
        if (config->max_peers > 0u) {
            max_peers = config->max_peers;
        }
        if (config->max_queued_messages_per_peer > 0u) {
            max_queued_per_peer = config->max_queued_messages_per_peer;
        }
    }

    if (size_mul_overflow(max_peers, sizeof(*hub->peers), &peers_alloc_size)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    hub = (cd_inproc_hub_t *)cd_context_alloc(context, sizeof(*hub));
    if (hub == NULL) {
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(hub, 0, sizeof(*hub));

    hub->context = context;
    hub->peer_capacity = max_peers;
    hub->per_peer_queue_capacity = max_queued_per_peer;
    hub->peers = (cd_inproc_peer_t *)cd_context_alloc(context, peers_alloc_size);
    if (hub->peers == NULL) {
        cd_context_free(context, hub);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(hub->peers, 0, peers_alloc_size);

    *out_hub = hub;
    return CD_STATUS_OK;
}

void cd_inproc_hub_shutdown(cd_inproc_hub_t *hub)
{
    size_t i;

    if (hub == NULL) {
        return;
    }

    for (i = 0; i < hub->peer_capacity; ++i) {
        if (hub->peers[i].in_use) {
            inproc_shutdown(&hub->peers[i]);
        }
    }

    cd_context_free(hub->context, hub->peers);
    cd_context_free(hub->context, hub);
}

cd_status_t cd_inproc_hub_create_transport(cd_inproc_hub_t *hub, cd_transport_t *out_transport)
{
    size_t i;
    size_t queue_alloc_size;

    if (hub == NULL || out_transport == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    memset(out_transport, 0, sizeof(*out_transport));

    if (size_mul_overflow(
            hub->per_peer_queue_capacity,
            sizeof(cd_inproc_message_t),
            &queue_alloc_size
        )) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    for (i = 0; i < hub->peer_capacity; ++i) {
        cd_inproc_peer_t *peer;

        peer = &hub->peers[i];
        if (peer->in_use) {
            continue;
        }

        peer->hub = hub;
        peer->peer_index = i;
        peer->queue_capacity = hub->per_peer_queue_capacity;
        peer->queue_head = 0u;
        peer->queue_count = 0u;
        peer->queue = (cd_inproc_message_t *)cd_context_alloc(hub->context, queue_alloc_size);
        if (peer->queue == NULL) {
            memset(peer, 0, sizeof(*peer));
            return CD_STATUS_ALLOCATION_FAILED;
        }
        memset(peer->queue, 0, queue_alloc_size);
        peer->in_use = true;

        out_transport->impl = peer;
        out_transport->send = inproc_send;
        out_transport->poll = inproc_poll;
        out_transport->flush = inproc_flush;
        out_transport->shutdown = inproc_shutdown;
        return CD_STATUS_OK;
    }

    return CD_STATUS_CAPACITY_REACHED;
}

void cd_inproc_transport_close(cd_transport_t *transport)
{
    if (transport == NULL) {
        return;
    }

    if (transport->shutdown != NULL) {
        transport->shutdown(transport->impl);
    }
    memset(transport, 0, sizeof(*transport));
}
