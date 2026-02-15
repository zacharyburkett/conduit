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

#define RUN_TEST(fn)                                                                \
    do {                                                                            \
        int fn_result = (fn)();                                                     \
        if (fn_result != 0) {                                                       \
            fprintf(stderr, "Test failed: %s\n", #fn);                              \
            return fn_result;                                                       \
        }                                                                           \
    } while (0)

typedef struct test_runtime {
    cd_context_t *context;
    cd_bus_t *bus;
} test_runtime_t;

typedef struct capture_state {
    int count;
    int markers[64];
    cd_message_kind_t kinds[64];
    cd_message_id_t message_ids[64];
    char payloads[64][32];
} capture_state_t;

typedef struct handler_state {
    capture_state_t *capture;
    int marker;
    cd_status_t return_status;
    cd_bus_t *republish_bus;
    cd_publish_params_t republish_params;
    int republish_once;
    int republished;
} handler_state_t;

static void test_free_adapter(void *user_data, void *ptr)
{
    (void)user_data;
    free(ptr);
}

static int setup_runtime(size_t max_queued_messages, size_t max_subscriptions, test_runtime_t *runtime)
{
    cd_bus_config_t bus_config;

    if (runtime == NULL) {
        return 1;
    }

    runtime->context = NULL;
    runtime->bus = NULL;
    ASSERT_STATUS(cd_context_init(&runtime->context, NULL), CD_STATUS_OK);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = max_queued_messages;
    bus_config.max_subscriptions = max_subscriptions;
    ASSERT_STATUS(cd_bus_create(runtime->context, &bus_config, &runtime->bus), CD_STATUS_OK);

    return 0;
}

static void teardown_runtime(test_runtime_t *runtime)
{
    if (runtime == NULL) {
        return;
    }

    cd_bus_destroy(runtime->bus);
    cd_context_shutdown(runtime->context);
    runtime->bus = NULL;
    runtime->context = NULL;
}

static cd_status_t generic_handler(void *user_data, const cd_envelope_t *message)
{
    handler_state_t *state;
    capture_state_t *capture;
    size_t payload_len;

    state = (handler_state_t *)user_data;
    if (state == NULL || message == NULL || state->capture == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    capture = state->capture;
    if (capture->count < (int)(sizeof(capture->markers) / sizeof(capture->markers[0]))) {
        size_t idx;

        idx = (size_t)capture->count;
        capture->markers[idx] = state->marker;
        capture->kinds[idx] = message->kind;
        capture->message_ids[idx] = message->message_id;
        memset(capture->payloads[idx], 0, sizeof(capture->payloads[idx]));
        payload_len = message->payload_size;
        if (payload_len > sizeof(capture->payloads[idx]) - 1u) {
            payload_len = sizeof(capture->payloads[idx]) - 1u;
        }
        if (payload_len > 0u && message->payload != NULL) {
            memcpy(capture->payloads[idx], message->payload, payload_len);
        }
    }
    capture->count += 1;

    if (state->republish_once && !state->republished) {
        cd_status_t publish_status;

        state->republished = 1;
        publish_status = cd_publish(state->republish_bus, &state->republish_params, NULL);
        if (publish_status != CD_STATUS_OK) {
            return publish_status;
        }
    }

    return state->return_status;
}

static int test_basic_event_delivery_and_payload_copy(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub;
    cd_publish_params_t publish;
    cd_message_id_t message_id;
    size_t processed;
    capture_state_t capture;
    handler_state_t handler;
    char payload[] = "hello-bus";

    ASSERT_TRUE(setup_runtime(8u, 8u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 1;
    handler.return_status = CD_STATUS_OK;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 100u;
    sub.topic = 42u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = generic_handler;
    sub.user_data = &handler;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 7u;
    publish.topic = 42u;
    publish.schema_id = 11u;
    publish.schema_version = 1u;
    publish.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    publish.payload = payload;
    publish.payload_size = strlen(payload);

    message_id = 0u;
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, &message_id), CD_STATUS_OK);
    ASSERT_TRUE(message_id != 0u);

    payload[0] = 'X';
    processed = 99u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 1);
    ASSERT_TRUE(capture.kinds[0] == CD_MESSAGE_EVENT);
    ASSERT_TRUE(capture.message_ids[0] == message_id);
    ASSERT_TRUE(strcmp(capture.payloads[0], "hello-bus") == 0);

    teardown_runtime(&runtime);
    return 0;
}

static int test_command_target_routing(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub_a;
    cd_subscription_desc_t sub_b;
    cd_command_params_t command;
    capture_state_t capture;
    handler_state_t handler_a;
    handler_state_t handler_b;

    ASSERT_TRUE(setup_runtime(8u, 8u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler_a, 0, sizeof(handler_a));
    memset(&handler_b, 0, sizeof(handler_b));
    handler_a.capture = &capture;
    handler_b.capture = &capture;
    handler_a.marker = 10;
    handler_b.marker = 11;
    handler_a.return_status = CD_STATUS_OK;
    handler_b.return_status = CD_STATUS_OK;

    memset(&sub_a, 0, sizeof(sub_a));
    sub_a.endpoint = 10u;
    sub_a.topic = 9u;
    sub_a.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_COMMAND);
    sub_a.handler = generic_handler;
    sub_a.user_data = &handler_a;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub_a, NULL), CD_STATUS_OK);

    memset(&sub_b, 0, sizeof(sub_b));
    sub_b.endpoint = 11u;
    sub_b.topic = 9u;
    sub_b.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_COMMAND);
    sub_b.handler = generic_handler;
    sub_b.user_data = &handler_b;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub_b, NULL), CD_STATUS_OK);

    memset(&command, 0, sizeof(command));
    command.source_endpoint = 1u;
    command.target_endpoint = 11u;
    command.topic = 9u;
    command.schema_id = 22u;
    command.schema_version = 1u;
    command.payload = "cmd";
    command.payload_size = 3u;
    ASSERT_STATUS(cd_send_command(runtime.bus, &command, NULL), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, NULL), CD_STATUS_OK);

    ASSERT_TRUE(capture.count == 1);
    ASSERT_TRUE(capture.markers[0] == 11);
    ASSERT_TRUE(capture.kinds[0] == CD_MESSAGE_COMMAND);

    teardown_runtime(&runtime);
    return 0;
}

static int test_deterministic_order_after_slot_reuse(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub_a;
    cd_subscription_desc_t sub_b;
    cd_subscription_desc_t sub_c;
    cd_subscription_id_t sub_a_id;
    capture_state_t capture;
    handler_state_t handler_a;
    handler_state_t handler_b;
    handler_state_t handler_c;
    cd_publish_params_t publish;

    ASSERT_TRUE(setup_runtime(8u, 8u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler_a, 0, sizeof(handler_a));
    memset(&handler_b, 0, sizeof(handler_b));
    memset(&handler_c, 0, sizeof(handler_c));
    handler_a.capture = &capture;
    handler_b.capture = &capture;
    handler_c.capture = &capture;
    handler_a.marker = 1;
    handler_b.marker = 2;
    handler_c.marker = 3;
    handler_a.return_status = CD_STATUS_OK;
    handler_b.return_status = CD_STATUS_OK;
    handler_c.return_status = CD_STATUS_OK;

    memset(&sub_a, 0, sizeof(sub_a));
    sub_a.endpoint = 100u;
    sub_a.topic = 55u;
    sub_a.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub_a.handler = generic_handler;
    sub_a.user_data = &handler_a;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub_a, &sub_a_id), CD_STATUS_OK);

    memset(&sub_b, 0, sizeof(sub_b));
    sub_b.endpoint = 101u;
    sub_b.topic = 55u;
    sub_b.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub_b.handler = generic_handler;
    sub_b.user_data = &handler_b;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub_b, NULL), CD_STATUS_OK);

    ASSERT_STATUS(cd_unsubscribe(runtime.bus, sub_a_id), CD_STATUS_OK);

    memset(&sub_c, 0, sizeof(sub_c));
    sub_c.endpoint = 102u;
    sub_c.topic = 55u;
    sub_c.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub_c.handler = generic_handler;
    sub_c.user_data = &handler_c;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub_c, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 5u;
    publish.topic = 55u;
    publish.payload = "x";
    publish.payload_size = 1u;
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, NULL), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, NULL), CD_STATUS_OK);

    ASSERT_TRUE(capture.count == 2);
    ASSERT_TRUE(capture.markers[0] == 2);
    ASSERT_TRUE(capture.markers[1] == 3);

    teardown_runtime(&runtime);
    return 0;
}

static int test_pump_snapshot_with_reentrant_publish(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub;
    cd_publish_params_t publish;
    capture_state_t capture;
    handler_state_t handler;
    size_t processed;

    ASSERT_TRUE(setup_runtime(8u, 8u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 1;
    handler.return_status = CD_STATUS_OK;
    handler.republish_bus = runtime.bus;
    handler.republish_once = 1;
    memset(&handler.republish_params, 0, sizeof(handler.republish_params));
    handler.republish_params.source_endpoint = 1u;
    handler.republish_params.topic = 88u;
    handler.republish_params.payload = "inner";
    handler.republish_params.payload_size = 5u;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 1u;
    sub.topic = 88u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = generic_handler;
    sub.user_data = &handler;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 1u;
    publish.topic = 88u;
    publish.payload = "outer";
    publish.payload_size = 5u;
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, NULL), CD_STATUS_OK);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 1);
    ASSERT_TRUE(strcmp(capture.payloads[0], "outer") == 0);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 2);
    ASSERT_TRUE(strcmp(capture.payloads[1], "inner") == 0);

    teardown_runtime(&runtime);
    return 0;
}

static int test_pump_limit_and_message_id_order(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub;
    cd_publish_params_t publish;
    cd_message_id_t id1;
    cd_message_id_t id2;
    cd_message_id_t id3;
    capture_state_t capture;
    handler_state_t handler;
    size_t processed;

    ASSERT_TRUE(setup_runtime(8u, 8u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 7;
    handler.return_status = CD_STATUS_OK;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 2u;
    sub.topic = 90u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = generic_handler;
    sub.user_data = &handler;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 2u;
    publish.topic = 90u;
    publish.payload = "a";
    publish.payload_size = 1u;
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, &id1), CD_STATUS_OK);
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, &id2), CD_STATUS_OK);
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, &id3), CD_STATUS_OK);
    ASSERT_TRUE(id1 != 0u && id2 != 0u && id3 != 0u);
    ASSERT_TRUE(id1 < id2 && id2 < id3);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 2u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 2u);
    ASSERT_TRUE(capture.count == 2);
    ASSERT_TRUE(capture.message_ids[0] == id1);
    ASSERT_TRUE(capture.message_ids[1] == id2);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 2u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 3);
    ASSERT_TRUE(capture.message_ids[2] == id3);

    processed = 999u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 0u);

    teardown_runtime(&runtime);
    return 0;
}

static int test_subscription_capacity_and_validation(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub;
    cd_subscription_id_t id1;
    cd_subscription_id_t id2;
    cd_subscription_id_t id3;
    cd_subscription_id_t invalid_sub_id;
    capture_state_t capture;
    handler_state_t handler;

    ASSERT_TRUE(setup_runtime(8u, 2u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 1;
    handler.return_status = CD_STATUS_OK;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 1u;
    sub.topic = 1u;
    sub.kind_mask = 0u;
    sub.handler = generic_handler;
    sub.user_data = &handler;
    invalid_sub_id = 777u;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, &invalid_sub_id), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(invalid_sub_id == 0u);

    sub.kind_mask = 0x80000000u;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_INVALID_ARGUMENT);

    sub.endpoint = CD_ENDPOINT_NONE;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_COMMAND);
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_INVALID_ARGUMENT);

    sub.endpoint = 1u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, &id1), CD_STATUS_OK);

    sub.endpoint = 2u;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, &id2), CD_STATUS_OK);

    sub.endpoint = 3u;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_CAPACITY_REACHED);

    ASSERT_STATUS(cd_unsubscribe(runtime.bus, 9999u), CD_STATUS_NOT_FOUND);
    ASSERT_STATUS(cd_unsubscribe(runtime.bus, id1), CD_STATUS_OK);
    ASSERT_STATUS(cd_unsubscribe(runtime.bus, id1), CD_STATUS_NOT_FOUND);

    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, &id3), CD_STATUS_OK);
    ASSERT_TRUE(id3 > id2);

    teardown_runtime(&runtime);
    return 0;
}

static int test_pump_returns_first_callback_error(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub_a;
    cd_subscription_desc_t sub_b;
    cd_publish_params_t publish;
    capture_state_t capture;
    handler_state_t handler_a;
    handler_state_t handler_b;
    size_t processed;

    ASSERT_TRUE(setup_runtime(8u, 8u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler_a, 0, sizeof(handler_a));
    memset(&handler_b, 0, sizeof(handler_b));
    handler_a.capture = &capture;
    handler_b.capture = &capture;
    handler_a.marker = 1;
    handler_b.marker = 2;
    handler_a.return_status = CD_STATUS_SCHEMA_MISMATCH;
    handler_b.return_status = CD_STATUS_OK;

    memset(&sub_a, 0, sizeof(sub_a));
    sub_a.endpoint = 11u;
    sub_a.topic = 66u;
    sub_a.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub_a.handler = generic_handler;
    sub_a.user_data = &handler_a;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub_a, NULL), CD_STATUS_OK);

    memset(&sub_b, 0, sizeof(sub_b));
    sub_b.endpoint = 12u;
    sub_b.topic = 66u;
    sub_b.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub_b.handler = generic_handler;
    sub_b.user_data = &handler_b;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub_b, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 2u;
    publish.topic = 66u;
    publish.payload = "x";
    publish.payload_size = 1u;
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, NULL), CD_STATUS_OK);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_SCHEMA_MISMATCH);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 2);
    ASSERT_TRUE(capture.markers[0] == 1);
    ASSERT_TRUE(capture.markers[1] == 2);

    teardown_runtime(&runtime);
    return 0;
}

static int test_queue_and_argument_edges(void)
{
    test_runtime_t runtime;
    cd_publish_params_t publish;
    cd_command_params_t command;
    cd_request_params_t request;
    cd_reply_t reply;
    cd_request_token_t token;
    cd_message_id_t invalid_message_id;
    size_t processed;

    ASSERT_TRUE(setup_runtime(1u, 4u, &runtime) == 0);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 1u;
    publish.topic = 3u;
    publish.payload = NULL;
    publish.payload_size = 1u;
    invalid_message_id = 123u;
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, &invalid_message_id), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(invalid_message_id == 0u);

    publish.payload = "x";
    publish.payload_size = 1u;
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, NULL), CD_STATUS_OK);
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, NULL), CD_STATUS_QUEUE_FULL);

    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, NULL), CD_STATUS_OK);

    memset(&command, 0, sizeof(command));
    command.source_endpoint = 1u;
    command.target_endpoint = CD_ENDPOINT_NONE;
    command.topic = 4u;
    invalid_message_id = 234u;
    ASSERT_STATUS(cd_send_command(runtime.bus, &command, &invalid_message_id), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(invalid_message_id == 0u);

    ASSERT_STATUS(cd_bus_pump(NULL, 0u, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_publish(NULL, &publish, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_send_command(NULL, &command, NULL), CD_STATUS_INVALID_ARGUMENT);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 1u;
    request.target_endpoint = 2u;
    request.topic = 8u;
    request.timeout_ns = 1000u;

    token = 99u;
    ASSERT_STATUS(cd_request_async(NULL, &request, &token), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(token == 0u);
    ASSERT_STATUS(cd_request_async(runtime.bus, NULL, &token), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(token == 0u);
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token), CD_STATUS_NOT_IMPLEMENTED);
    ASSERT_TRUE(token == 0u);

    memset(&reply, 0xAB, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(NULL, 1u, &reply, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(reply.message_id == 0u);
    ASSERT_STATUS(cd_poll_reply(runtime.bus, 1u, &reply, NULL), CD_STATUS_NOT_IMPLEMENTED);
    ASSERT_TRUE(reply.message_id == 0u);

    processed = 123u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 0u);

    teardown_runtime(&runtime);
    return 0;
}

static int test_context_validation_edges(void)
{
    cd_context_t *context;
    cd_context_config_t config;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;

    ASSERT_STATUS(cd_context_init(NULL, NULL), CD_STATUS_INVALID_ARGUMENT);

    memset(&config, 0, sizeof(config));
    config.allocator.alloc = NULL;
    config.allocator.free = test_free_adapter;
    config.allocator.user_data = NULL;
    ASSERT_STATUS(cd_context_init(&context, &config), CD_STATUS_INVALID_ARGUMENT);

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(context != NULL);

    memset(&bus_config, 0, sizeof(bus_config));
    bus = NULL;
    ASSERT_STATUS(cd_bus_create(NULL, &bus_config, &bus), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, NULL), CD_STATUS_INVALID_ARGUMENT);

    cd_context_shutdown(context);
    return 0;
}

int main(void)
{
    RUN_TEST(test_context_validation_edges);
    RUN_TEST(test_basic_event_delivery_and_payload_copy);
    RUN_TEST(test_command_target_routing);
    RUN_TEST(test_deterministic_order_after_slot_reuse);
    RUN_TEST(test_pump_snapshot_with_reentrant_publish);
    RUN_TEST(test_pump_limit_and_message_id_order);
    RUN_TEST(test_subscription_capacity_and_validation);
    RUN_TEST(test_pump_returns_first_callback_error);
    RUN_TEST(test_queue_and_argument_edges);

    return 0;
}
