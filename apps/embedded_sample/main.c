#include "conduit/conduit.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

enum {
    CD_TOPIC_FRAME_BEGIN = 0x00010001u,
    CD_TOPIC_MAP_GENERATED = 0x00010002u,
    CD_TOPIC_ENTITY_SPAWN_REQUEST = 0x00020001u
};

enum {
    CD_SCHEMA_FRAME_BEGIN_V1 = 0x00001001u,
    CD_SCHEMA_MAP_GENERATED_V1 = 0x00001002u,
    CD_SCHEMA_ENTITY_SPAWN_REQUEST_V1 = 0x00002001u,
    CD_SCHEMA_ENTITY_SPAWN_REPLY_V1 = 0x00002002u
};

enum {
    CD_ENDPOINT_FRAME_LOOP = 1u,
    CD_ENDPOINT_MAP_SYSTEM = 10u,
    CD_ENDPOINT_GAMEPLAY_SYSTEM = 11u,
    CD_ENDPOINT_ENTITY_SERVICE = 20u
};

typedef struct frame_begin_payload {
    uint32_t frame_index;
} frame_begin_payload_t;

typedef struct map_generated_payload {
    uint32_t source_frame;
    uint32_t room_count;
} map_generated_payload_t;

typedef struct entity_spawn_request_payload {
    uint32_t archetype_id;
    int32_t tile_x;
    int32_t tile_y;
} entity_spawn_request_payload_t;

typedef struct entity_spawn_reply_payload {
    uint32_t entity_id;
    uint8_t accepted;
    int32_t tile_x;
    int32_t tile_y;
} entity_spawn_reply_payload_t;

typedef struct sample_state {
    cd_bus_t *bus;
    bool map_generation_requested;
    bool spawn_request_sent;
    bool spawn_completed;
    uint32_t map_generated_events;
    uint32_t spawn_requests_handled;
    uint32_t completed_frame;
    uint32_t spawned_entity_id;
    cd_request_token_t spawn_token;
} sample_state_t;

static void print_status_error(const char *operation, cd_status_t status)
{
    fprintf(
        stderr,
        "Sample error: %s failed with status '%s'.\n",
        operation,
        cd_status_string(status)
    );
}

static cd_status_t on_frame_begin(void *user_data, const cd_envelope_t *message)
{
    sample_state_t *state;
    const frame_begin_payload_t *frame;
    map_generated_payload_t map_generated;
    cd_publish_params_t publish;

    state = (sample_state_t *)user_data;
    if (state == NULL || message == NULL || message->kind != CD_MESSAGE_EVENT ||
        message->topic != CD_TOPIC_FRAME_BEGIN ||
        message->payload_size != sizeof(frame_begin_payload_t) || message->payload == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    frame = (const frame_begin_payload_t *)message->payload;
    printf("[sample] frame.begin frame=%u\n", frame->frame_index);

    if (state->map_generation_requested) {
        return CD_STATUS_OK;
    }

    state->map_generation_requested = true;
    memset(&map_generated, 0, sizeof(map_generated));
    map_generated.source_frame = frame->frame_index;
    map_generated.room_count = 12u;

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = CD_ENDPOINT_MAP_SYSTEM;
    publish.topic = CD_TOPIC_MAP_GENERATED;
    publish.schema_id = CD_SCHEMA_MAP_GENERATED_V1;
    publish.schema_version = 1u;
    publish.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    publish.payload = &map_generated;
    publish.payload_size = sizeof(map_generated);
    return cd_publish(state->bus, &publish, NULL);
}

static cd_status_t on_map_generated(void *user_data, const cd_envelope_t *message)
{
    sample_state_t *state;
    const map_generated_payload_t *payload;
    entity_spawn_request_payload_t spawn_request;
    cd_request_params_t request;
    cd_status_t status;

    state = (sample_state_t *)user_data;
    if (state == NULL || message == NULL || message->kind != CD_MESSAGE_EVENT ||
        message->topic != CD_TOPIC_MAP_GENERATED ||
        message->payload_size != sizeof(map_generated_payload_t) || message->payload == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    payload = (const map_generated_payload_t *)message->payload;
    state->map_generated_events += 1u;
    printf(
        "[sample] map.generated frame=%u rooms=%u\n",
        payload->source_frame,
        payload->room_count
    );

    if (state->spawn_request_sent) {
        return CD_STATUS_OK;
    }

    memset(&spawn_request, 0, sizeof(spawn_request));
    spawn_request.archetype_id = 42u;
    spawn_request.tile_x = 8;
    spawn_request.tile_y = 6;

    memset(&request, 0, sizeof(request));
    request.source_endpoint = CD_ENDPOINT_GAMEPLAY_SYSTEM;
    request.target_endpoint = CD_ENDPOINT_ENTITY_SERVICE;
    request.topic = CD_TOPIC_ENTITY_SPAWN_REQUEST;
    request.schema_id = CD_SCHEMA_ENTITY_SPAWN_REQUEST_V1;
    request.schema_version = 1u;
    request.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    request.timeout_ns = 1000000000ull;
    request.payload = &spawn_request;
    request.payload_size = sizeof(spawn_request);

    status = cd_request_async(state->bus, &request, &state->spawn_token);
    if (status == CD_STATUS_OK) {
        state->spawn_request_sent = true;
    }
    return status;
}

static cd_status_t on_entity_spawn_request(void *user_data, const cd_envelope_t *message)
{
    sample_state_t *state;
    const entity_spawn_request_payload_t *request_payload;
    entity_spawn_reply_payload_t reply_payload;
    cd_reply_params_t reply;

    state = (sample_state_t *)user_data;
    if (state == NULL || message == NULL || message->kind != CD_MESSAGE_REQUEST ||
        message->topic != CD_TOPIC_ENTITY_SPAWN_REQUEST ||
        message->payload_size != sizeof(entity_spawn_request_payload_t) ||
        message->payload == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    request_payload = (const entity_spawn_request_payload_t *)message->payload;
    state->spawn_requests_handled += 1u;
    printf(
        "[sample] entity.spawn.request archetype=%u at=(%d,%d)\n",
        request_payload->archetype_id,
        request_payload->tile_x,
        request_payload->tile_y
    );

    memset(&reply_payload, 0, sizeof(reply_payload));
    reply_payload.accepted = 1u;
    reply_payload.entity_id = 1000u + state->spawn_requests_handled;
    reply_payload.tile_x = request_payload->tile_x;
    reply_payload.tile_y = request_payload->tile_y;

    memset(&reply, 0, sizeof(reply));
    reply.source_endpoint = CD_ENDPOINT_ENTITY_SERVICE;
    reply.target_endpoint = message->source_endpoint;
    reply.correlation_id = message->message_id;
    reply.topic = CD_TOPIC_ENTITY_SPAWN_REQUEST;
    reply.schema_id = CD_SCHEMA_ENTITY_SPAWN_REPLY_V1;
    reply.schema_version = 1u;
    reply.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    reply.payload = &reply_payload;
    reply.payload_size = sizeof(reply_payload);
    return cd_send_reply(state->bus, &reply, NULL);
}

int main(void)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_subscription_desc_t sub;
    sample_state_t state;
    cd_status_t status;
    uint32_t frame;

    context = NULL;
    bus = NULL;
    memset(&state, 0, sizeof(state));

    status = cd_context_init(&context, NULL);
    if (status != CD_STATUS_OK) {
        print_status_error("cd_context_init", status);
        return 1;
    }

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 128u;
    bus_config.max_subscriptions = 32u;
    bus_config.max_inflight_requests = 32u;
    status = cd_bus_create(context, &bus_config, &bus);
    if (status != CD_STATUS_OK) {
        print_status_error("cd_bus_create", status);
        cd_context_shutdown(context);
        return 1;
    }

    state.bus = bus;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = CD_ENDPOINT_MAP_SYSTEM;
    sub.topic = CD_TOPIC_FRAME_BEGIN;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = on_frame_begin;
    sub.user_data = &state;
    status = cd_subscribe(bus, &sub, NULL);
    if (status != CD_STATUS_OK) {
        print_status_error("cd_subscribe(frame.begin)", status);
        cd_bus_destroy(bus);
        cd_context_shutdown(context);
        return 1;
    }

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = CD_ENDPOINT_GAMEPLAY_SYSTEM;
    sub.topic = CD_TOPIC_MAP_GENERATED;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = on_map_generated;
    sub.user_data = &state;
    status = cd_subscribe(bus, &sub, NULL);
    if (status != CD_STATUS_OK) {
        print_status_error("cd_subscribe(map.generated)", status);
        cd_bus_destroy(bus);
        cd_context_shutdown(context);
        return 1;
    }

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = CD_ENDPOINT_ENTITY_SERVICE;
    sub.topic = CD_TOPIC_ENTITY_SPAWN_REQUEST;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    sub.handler = on_entity_spawn_request;
    sub.user_data = &state;
    status = cd_subscribe(bus, &sub, NULL);
    if (status != CD_STATUS_OK) {
        print_status_error("cd_subscribe(entity.spawn.request)", status);
        cd_bus_destroy(bus);
        cd_context_shutdown(context);
        return 1;
    }

    for (frame = 0u; frame < 8u && !state.spawn_completed; ++frame) {
        frame_begin_payload_t frame_payload;
        cd_publish_params_t frame_begin;
        size_t processed;

        memset(&frame_payload, 0, sizeof(frame_payload));
        frame_payload.frame_index = frame;

        memset(&frame_begin, 0, sizeof(frame_begin));
        frame_begin.source_endpoint = CD_ENDPOINT_FRAME_LOOP;
        frame_begin.topic = CD_TOPIC_FRAME_BEGIN;
        frame_begin.schema_id = CD_SCHEMA_FRAME_BEGIN_V1;
        frame_begin.schema_version = 1u;
        frame_begin.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
        frame_begin.payload = &frame_payload;
        frame_begin.payload_size = sizeof(frame_payload);

        status = cd_publish(bus, &frame_begin, NULL);
        if (status != CD_STATUS_OK) {
            print_status_error("cd_publish(frame.begin)", status);
            cd_bus_destroy(bus);
            cd_context_shutdown(context);
            return 1;
        }

        processed = 0u;
        status = cd_bus_pump(bus, 0u, &processed);
        if (status != CD_STATUS_OK) {
            print_status_error("cd_bus_pump", status);
            cd_bus_destroy(bus);
            cd_context_shutdown(context);
            return 1;
        }

        printf("[sample] pump frame=%u processed=%zu\n", frame, processed);

        if (state.spawn_request_sent && !state.spawn_completed) {
            cd_reply_t reply;
            int ready;

            memset(&reply, 0, sizeof(reply));
            ready = 0;
            status = cd_poll_reply(bus, state.spawn_token, &reply, &ready);
            if (status == CD_STATUS_TIMEOUT) {
                fprintf(stderr, "Sample error: spawn request timed out.\n");
                cd_reply_dispose(bus, &reply);
                cd_bus_destroy(bus);
                cd_context_shutdown(context);
                return 1;
            }
            if (status != CD_STATUS_OK) {
                print_status_error("cd_poll_reply", status);
                cd_reply_dispose(bus, &reply);
                cd_bus_destroy(bus);
                cd_context_shutdown(context);
                return 1;
            }

            if (ready == 1) {
                const entity_spawn_reply_payload_t *reply_payload;

                if (reply.payload == NULL ||
                    reply.payload_size != sizeof(entity_spawn_reply_payload_t)) {
                    fprintf(stderr, "Sample error: invalid spawn reply payload.\n");
                    cd_reply_dispose(bus, &reply);
                    cd_bus_destroy(bus);
                    cd_context_shutdown(context);
                    return 1;
                }

                reply_payload = (const entity_spawn_reply_payload_t *)reply.payload;
                if (reply_payload->accepted == 0u) {
                    fprintf(stderr, "Sample error: spawn was rejected.\n");
                    cd_reply_dispose(bus, &reply);
                    cd_bus_destroy(bus);
                    cd_context_shutdown(context);
                    return 1;
                }

                state.spawn_completed = true;
                state.completed_frame = frame;
                state.spawned_entity_id = reply_payload->entity_id;
                printf(
                    "[sample] entity.spawn.reply entity_id=%u at=(%d,%d)\n",
                    reply_payload->entity_id,
                    reply_payload->tile_x,
                    reply_payload->tile_y
                );
            }

            cd_reply_dispose(bus, &reply);
        }
    }

    if (!state.spawn_completed) {
        fprintf(stderr, "Sample error: spawn flow did not complete.\n");
        cd_bus_destroy(bus);
        cd_context_shutdown(context);
        return 1;
    }
    if (state.map_generated_events != 1u || state.spawn_requests_handled != 1u) {
        fprintf(
            stderr,
            "Sample error: unexpected counts map_generated=%u spawn_requests=%u.\n",
            state.map_generated_events,
            state.spawn_requests_handled
        );
        cd_bus_destroy(bus);
        cd_context_shutdown(context);
        return 1;
    }
    if (state.completed_frame != 3u) {
        fprintf(
            stderr,
            "Sample error: expected completion on frame 3, got frame %u.\n",
            state.completed_frame
        );
        cd_bus_destroy(bus);
        cd_context_shutdown(context);
        return 1;
    }

    printf(
        "[sample] success entity_id=%u completed_frame=%u\n",
        state.spawned_entity_id,
        state.completed_frame
    );

    cd_bus_destroy(bus);
    cd_context_shutdown(context);
    return 0;
}
