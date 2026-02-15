#ifndef CONDUIT_BUS_H
#define CONDUIT_BUS_H

#include "conduit/context.h"
#include "conduit/transport.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cd_bus cd_bus_t;

typedef struct cd_bus_config {
    /* 0 uses implementation default. */
    size_t max_queued_messages;
    /* 0 uses implementation default. */
    size_t max_subscriptions;
    /* 0 uses implementation default. */
    size_t max_inflight_requests;
    /* 0 uses implementation default. */
    size_t max_transports;
} cd_bus_config_t;

typedef cd_status_t (*cd_message_handler_fn)(void *user_data, const cd_envelope_t *message);

typedef struct cd_subscription_desc {
    /*
     * Endpoint identity for this subscriber.
     * Must be non-zero when subscribing to command/request/reply kinds.
     */
    cd_endpoint_id_t endpoint;
    cd_topic_t topic;
    uint32_t kind_mask;
    cd_message_handler_fn handler;
    void *user_data;
} cd_subscription_desc_t;

typedef struct cd_publish_params {
    cd_endpoint_id_t source_endpoint;
    cd_topic_t topic;
    uint32_t schema_id;
    uint16_t schema_version;
    uint16_t flags;
    const void *payload;
    size_t payload_size;
} cd_publish_params_t;

typedef struct cd_command_params {
    cd_endpoint_id_t source_endpoint;
    cd_endpoint_id_t target_endpoint;
    cd_topic_t topic;
    uint32_t schema_id;
    uint16_t schema_version;
    uint16_t flags;
    const void *payload;
    size_t payload_size;
} cd_command_params_t;

typedef struct cd_request_params {
    cd_endpoint_id_t source_endpoint;
    cd_endpoint_id_t target_endpoint;
    cd_topic_t topic;
    uint32_t schema_id;
    uint16_t schema_version;
    uint16_t flags;
    uint64_t timeout_ns;
    const void *payload;
    size_t payload_size;
} cd_request_params_t;

typedef struct cd_reply_params {
    cd_endpoint_id_t source_endpoint;
    cd_endpoint_id_t target_endpoint;
    cd_message_id_t correlation_id;
    cd_topic_t topic;
    uint32_t schema_id;
    uint16_t schema_version;
    uint16_t flags;
    const void *payload;
    size_t payload_size;
} cd_reply_params_t;

typedef struct cd_reply {
    cd_message_id_t message_id;
    uint32_t schema_id;
    uint16_t schema_version;
    const void *payload;
    size_t payload_size;
} cd_reply_t;

cd_status_t cd_bus_create(cd_context_t *context, const cd_bus_config_t *config, cd_bus_t **out_bus);
void cd_bus_destroy(cd_bus_t *bus);

/*
 * Attach/detach transport endpoints to a bus.
 *
 * Transports are non-owning references; caller controls transport lifetime.
 * Attach rejects duplicates and enforces max_transports capacity.
 */
cd_status_t cd_bus_attach_transport(cd_bus_t *bus, cd_transport_t *transport);
cd_status_t cd_bus_detach_transport(cd_bus_t *bus, cd_transport_t *transport);

/*
 * Drain queued messages from a stable queue snapshot taken at pump start.
 *
 * max_messages == 0 drains the full snapshot.
 * max_messages > 0 drains up to that many messages from the snapshot.
 * Messages published during callback execution are left for a later pump.
 */
cd_status_t cd_bus_pump(cd_bus_t *bus, size_t max_messages, size_t *out_processed);

cd_status_t cd_publish(cd_bus_t *bus, const cd_publish_params_t *params, cd_message_id_t *out_message_id);

cd_status_t cd_send_command(
    cd_bus_t *bus,
    const cd_command_params_t *params,
    cd_message_id_t *out_message_id
);

cd_status_t cd_request_async(
    cd_bus_t *bus,
    const cd_request_params_t *params,
    cd_request_token_t *out_token
);

cd_status_t cd_send_reply(
    cd_bus_t *bus,
    const cd_reply_params_t *params,
    cd_message_id_t *out_message_id
);

/*
 * Polls request status by token.
 *
 * Returns:
 * - CD_STATUS_OK with out_ready=0 when still waiting.
 * - CD_STATUS_OK with out_ready=1 and out_reply set when reply arrived.
 * - CD_STATUS_TIMEOUT with out_ready=1 when request expired.
 * - CD_STATUS_NOT_FOUND for unknown token.
 *
 * When out_reply is provided and a reply is ready, payload is copied into
 * bus-owned allocation and must be released with cd_reply_dispose.
 */
cd_status_t cd_poll_reply(
    cd_bus_t *bus,
    cd_request_token_t token,
    cd_reply_t *out_reply,
    int *out_ready
);

/*
 * Releases payload memory returned by cd_poll_reply.
 * Safe to call with zeroed/empty replies.
 */
void cd_reply_dispose(cd_bus_t *bus, cd_reply_t *reply);

cd_status_t cd_subscribe(
    cd_bus_t *bus,
    const cd_subscription_desc_t *desc,
    cd_subscription_id_t *out_subscription_id
);

cd_status_t cd_unsubscribe(cd_bus_t *bus, cd_subscription_id_t subscription_id);

#ifdef __cplusplus
}
#endif

#endif
