#ifndef CONDUIT_INTERNAL_H
#define CONDUIT_INTERNAL_H

#include "conduit/conduit.h"

#include <stdbool.h>

struct cd_context {
    cd_alloc_fn alloc_fn;
    cd_free_fn free_fn;
    void *alloc_user_data;
    cd_now_ns_fn now_ns_fn;
    void *clock_user_data;
};

typedef struct cd_queued_message {
    cd_envelope_t envelope;
    void *payload_copy;
} cd_queued_message_t;

typedef struct cd_subscription_entry {
    bool in_use;
    cd_subscription_id_t subscription_id;
    cd_subscription_desc_t desc;
} cd_subscription_entry_t;

typedef struct cd_dispatch_target {
    size_t index;
    cd_subscription_id_t subscription_id;
} cd_dispatch_target_t;

typedef enum cd_request_state {
    CD_REQUEST_WAITING = 0,
    CD_REQUEST_READY = 1,
    CD_REQUEST_TIMED_OUT = 2
} cd_request_state_t;

typedef struct cd_inflight_request {
    bool in_use;
    cd_request_state_t state;
    cd_request_token_t token;
    cd_message_id_t correlation_id;
    cd_endpoint_id_t requester_endpoint;
    uint64_t expires_at_ns;
    cd_message_id_t reply_message_id;
    uint32_t reply_schema_id;
    uint16_t reply_schema_version;
    void *reply_payload;
    size_t reply_payload_size;
} cd_inflight_request_t;

struct cd_bus {
    cd_context_t *context;
    cd_queued_message_t *queue;
    size_t queue_capacity;
    size_t queue_head;
    size_t queue_count;
    cd_subscription_entry_t *subscriptions;
    size_t subscription_capacity;
    cd_dispatch_target_t *dispatch_targets;
    cd_inflight_request_t *inflight_requests;
    size_t inflight_request_capacity;
    cd_subscription_id_t next_subscription_id;
    cd_message_id_t next_message_id;
    cd_request_token_t next_request_token;
};

void *cd_context_alloc(cd_context_t *context, size_t size);
void cd_context_free(cd_context_t *context, void *ptr);
uint64_t cd_context_now_ns(const cd_context_t *context);

#endif
