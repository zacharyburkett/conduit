#ifndef CONDUIT_BUS_H
#define CONDUIT_BUS_H

#include "conduit/context.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cd_bus cd_bus_t;

typedef struct cd_bus_config {
    size_t max_queued_messages;
    size_t max_subscriptions;
} cd_bus_config_t;

typedef cd_status_t (*cd_message_handler_fn)(void *user_data, const cd_envelope_t *message);

typedef struct cd_subscription_desc {
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
 * Drain queued messages. max_messages == 0 drains all currently queued messages.
 */
cd_status_t cd_bus_pump(cd_bus_t *bus, size_t max_messages, size_t *out_processed);

cd_status_t cd_publish(cd_bus_t *bus, const cd_publish_params_t *params, cd_message_id_t *out_message_id);

cd_status_t cd_send_command(
    cd_bus_t *bus,
    const cd_command_params_t *params,
    cd_message_id_t *out_message_id
);

/*
 * Reserved API for Phase 2. Currently returns CD_STATUS_NOT_IMPLEMENTED.
 */
cd_status_t cd_request_async(
    cd_bus_t *bus,
    const cd_request_params_t *params,
    cd_request_token_t *out_token
);

/*
 * Reserved API for Phase 2. Currently returns CD_STATUS_NOT_IMPLEMENTED.
 */
cd_status_t cd_poll_reply(
    cd_bus_t *bus,
    cd_request_token_t token,
    cd_reply_t *out_reply,
    int *out_ready
);

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
