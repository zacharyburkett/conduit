#include "conduit/conduit.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ASSERT_TRUE(condition)                                                      \
    do {                                                                            \
        if (!(condition)) {                                                         \
            fprintf(stderr, "Assertion failed at %s:%d: %s\n", __FILE__, __LINE__, \
                    #condition);                                                    \
            return 1;                                                               \
        }                                                                           \
    } while (0)

#define ASSERT_STATUS(actual, expected)                                             \
    do {                                                                            \
        cd_status_t status_result = (actual);                                       \
        if (status_result != (expected)) {                                          \
            fprintf(stderr, "Unexpected status at %s:%d: got %s expected %s\n",    \
                    __FILE__, __LINE__, cd_status_string(status_result),            \
                    cd_status_string((expected)));                                  \
            return 1;                                                               \
        }                                                                           \
    } while (0)

typedef struct test_state {
    int event_hits;
    int command_hits_a;
    int command_hits_b;
    int last_reply_ready;
    char last_payload[32];
} test_state_t;

static cd_status_t on_event(void *user_data, const cd_envelope_t *message)
{
    test_state_t *state;
    size_t copy_size;

    state = (test_state_t *)user_data;
    if (message == NULL || message->kind != CD_MESSAGE_EVENT || state == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    state->event_hits += 1;
    memset(state->last_payload, 0, sizeof(state->last_payload));
    copy_size = message->payload_size;
    if (copy_size > sizeof(state->last_payload) - 1u) {
        copy_size = sizeof(state->last_payload) - 1u;
    }
    if (copy_size > 0u && message->payload != NULL) {
        memcpy(state->last_payload, message->payload, copy_size);
    }

    return CD_STATUS_OK;
}

static cd_status_t on_command_a(void *user_data, const cd_envelope_t *message)
{
    test_state_t *state;

    state = (test_state_t *)user_data;
    if (message == NULL || message->kind != CD_MESSAGE_COMMAND || state == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    state->command_hits_a += 1;
    return CD_STATUS_OK;
}

static cd_status_t on_command_b(void *user_data, const cd_envelope_t *message)
{
    test_state_t *state;

    state = (test_state_t *)user_data;
    if (message == NULL || message->kind != CD_MESSAGE_COMMAND || state == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    state->command_hits_b += 1;
    return CD_STATUS_OK;
}

int main(void)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_subscription_desc_t subscription;
    cd_subscription_desc_t command_sub_a;
    cd_subscription_desc_t command_sub_b;
    cd_publish_params_t publish_params;
    cd_command_params_t command_params;
    cd_request_params_t request_params;
    cd_reply_t reply;
    cd_request_token_t token;
    cd_subscription_id_t event_subscription_id;
    cd_subscription_id_t command_subscription_a;
    cd_subscription_id_t command_subscription_b;
    cd_message_id_t message_id;
    size_t processed;
    int ready;
    test_state_t state;
    const char payload[] = "hello-bus";

    ASSERT_STATUS(cd_context_init(NULL, NULL), CD_STATUS_INVALID_ARGUMENT);

    context = NULL;
    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(context != NULL);

    bus_config.max_queued_messages = 4u;
    bus_config.max_subscriptions = 8u;
    bus = NULL;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);
    ASSERT_TRUE(bus != NULL);

    memset(&state, 0, sizeof(state));
    memset(&subscription, 0, sizeof(subscription));
    subscription.endpoint = 1u;
    subscription.topic = 42u;
    subscription.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    subscription.handler = on_event;
    subscription.user_data = &state;
    ASSERT_STATUS(cd_subscribe(bus, &subscription, &event_subscription_id), CD_STATUS_OK);

    memset(&publish_params, 0, sizeof(publish_params));
    publish_params.source_endpoint = 99u;
    publish_params.topic = 42u;
    publish_params.schema_id = 1001u;
    publish_params.schema_version = 1u;
    publish_params.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    publish_params.payload = payload;
    publish_params.payload_size = sizeof(payload) - 1u;
    ASSERT_STATUS(cd_publish(bus, &publish_params, &message_id), CD_STATUS_OK);
    ASSERT_TRUE(message_id != 0);

    ASSERT_STATUS(cd_bus_pump(bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(state.event_hits == 1);
    ASSERT_TRUE(strcmp(state.last_payload, payload) == 0);

    memset(&command_sub_a, 0, sizeof(command_sub_a));
    command_sub_a.endpoint = 10u;
    command_sub_a.topic = 7u;
    command_sub_a.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_COMMAND);
    command_sub_a.handler = on_command_a;
    command_sub_a.user_data = &state;
    ASSERT_STATUS(cd_subscribe(bus, &command_sub_a, &command_subscription_a), CD_STATUS_OK);

    memset(&command_sub_b, 0, sizeof(command_sub_b));
    command_sub_b.endpoint = 11u;
    command_sub_b.topic = 7u;
    command_sub_b.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_COMMAND);
    command_sub_b.handler = on_command_b;
    command_sub_b.user_data = &state;
    ASSERT_STATUS(cd_subscribe(bus, &command_sub_b, &command_subscription_b), CD_STATUS_OK);

    memset(&command_params, 0, sizeof(command_params));
    command_params.source_endpoint = 99u;
    command_params.target_endpoint = 10u;
    command_params.topic = 7u;
    command_params.schema_id = 2002u;
    command_params.schema_version = 1u;
    command_params.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    ASSERT_STATUS(cd_send_command(bus, &command_params, NULL), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_pump(bus, 0u, NULL), CD_STATUS_OK);
    ASSERT_TRUE(state.command_hits_a == 1);
    ASSERT_TRUE(state.command_hits_b == 0);

    bus_config.max_queued_messages = 1u;
    bus_config.max_subscriptions = 2u;
    cd_bus_destroy(bus);
    bus = NULL;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);

    memset(&subscription, 0, sizeof(subscription));
    subscription.endpoint = 1u;
    subscription.topic = 9u;
    subscription.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    subscription.handler = on_event;
    subscription.user_data = &state;
    ASSERT_STATUS(cd_subscribe(bus, &subscription, NULL), CD_STATUS_OK);

    memset(&publish_params, 0, sizeof(publish_params));
    publish_params.source_endpoint = 1u;
    publish_params.topic = 9u;
    publish_params.payload = payload;
    publish_params.payload_size = sizeof(payload) - 1u;
    ASSERT_STATUS(cd_publish(bus, &publish_params, NULL), CD_STATUS_OK);
    ASSERT_STATUS(cd_publish(bus, &publish_params, NULL), CD_STATUS_QUEUE_FULL);
    ASSERT_STATUS(cd_bus_pump(bus, 0u, NULL), CD_STATUS_OK);

    ASSERT_STATUS(cd_unsubscribe(bus, 123456u), CD_STATUS_NOT_FOUND);
    ASSERT_STATUS(cd_unsubscribe(bus, 0u), CD_STATUS_INVALID_ARGUMENT);

    memset(&request_params, 0, sizeof(request_params));
    request_params.source_endpoint = 1u;
    request_params.target_endpoint = 2u;
    request_params.topic = 99u;
    request_params.timeout_ns = 1000000u;
    token = 0u;
    ASSERT_STATUS(cd_request_async(bus, &request_params, &token), CD_STATUS_NOT_IMPLEMENTED);
    ASSERT_TRUE(token == 0u);

    memset(&reply, 0, sizeof(reply));
    ready = 0;
    ASSERT_STATUS(cd_poll_reply(bus, token, &reply, &ready), CD_STATUS_NOT_IMPLEMENTED);
    ASSERT_TRUE(ready == 0);

    state.last_reply_ready = ready;
    ASSERT_TRUE(state.last_reply_ready == 0);

    cd_bus_destroy(bus);
    cd_context_shutdown(context);

    return 0;
}
