#include "conduit/conduit.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

enum {
    CD_BROKER_DEFAULT_MAX_CLIENTS = 8,
    CD_BROKER_DEFAULT_MAX_TOPIC_ROUTES = 256,
    CD_BROKER_DEFAULT_MAX_ENDPOINT_ROUTES = 512,
    CD_BROKER_DEFAULT_METRICS_INTERVAL_MS = 1000u,
    CD_BROKER_MAX_TOPIC_ENDPOINTS = 32,
    CD_BROKER_DIAGNOSTICS_ENDPOINT = 0xFFFFFFFEu,
    CD_BROKER_DIAGNOSTICS_TOPIC_METRICS = 0x00B00001u,
    CD_BROKER_DIAGNOSTICS_SCHEMA_METRICS_TEXT = 0x00B00001u
};

typedef struct broker_config {
    const char *socket_path;
    const char *routes_file;
    size_t max_clients;
    size_t max_topic_routes;
    size_t max_endpoint_routes;
    uint64_t metrics_interval_ms;
    uint64_t run_ms;
} broker_config_t;

typedef struct broker_client {
    bool in_use;
    cd_transport_t transport;
} broker_client_t;

typedef struct broker_topic_route {
    bool in_use;
    cd_topic_t topic;
    uint64_t recipient_mask;
    bool has_configured_endpoints;
    cd_endpoint_id_t configured_endpoints[CD_BROKER_MAX_TOPIC_ENDPOINTS];
    size_t configured_endpoint_count;
} broker_topic_route_t;

typedef struct broker_endpoint_route {
    bool in_use;
    cd_endpoint_id_t endpoint;
    size_t client_index;
} broker_endpoint_route_t;

typedef struct broker_metrics {
    uint64_t published;
    uint64_t delivered;
    uint64_t dropped;
    uint64_t timeouts;
    uint64_t transport_errors;
} broker_metrics_t;

typedef struct broker_state {
    cd_context_t *context;
    broker_client_t *clients;
    size_t client_capacity;
    broker_topic_route_t *topic_routes;
    size_t topic_route_capacity;
    broker_endpoint_route_t *endpoint_routes;
    size_t endpoint_route_capacity;
    bool explicit_topic_routes_only;
    broker_metrics_t metrics;
    cd_message_id_t next_generated_message_id;
} broker_state_t;

typedef struct broker_receive_context {
    broker_state_t *state;
    size_t source_client_index;
} broker_receive_context_t;

static volatile sig_atomic_t g_broker_running = 1;

static size_t active_client_count(const broker_state_t *state);

static void broker_signal_handler(int signum)
{
    (void)signum;
    g_broker_running = 0;
}

static uint64_t broker_now_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0u;
    }

    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)(ts.tv_nsec / 1000000u);
}

static void broker_sleep_ms(uint64_t delay_ms)
{
    struct timespec req;

    req.tv_sec = (time_t)(delay_ms / 1000u);
    req.tv_nsec = (long)((delay_ms % 1000u) * 1000000u);
    while (nanosleep(&req, &req) != 0 && errno == EINTR) {
    }
}

static bool parse_u64_arg(const char *text, uint64_t *out_value)
{
    unsigned long long value;
    char *end_ptr;

    if (text == NULL || out_value == NULL) {
        return false;
    }

    errno = 0;
    end_ptr = NULL;
    value = strtoull(text, &end_ptr, 10);
    if (errno != 0 || end_ptr == text || end_ptr == NULL || *end_ptr != '\0') {
        return false;
    }

    *out_value = (uint64_t)value;
    return true;
}

static bool parse_u64_token(const char *text, uint64_t *out_value)
{
    unsigned long long value;
    char *end_ptr;

    if (text == NULL || out_value == NULL) {
        return false;
    }

    errno = 0;
    end_ptr = NULL;
    value = strtoull(text, &end_ptr, 0);
    if (errno != 0 || end_ptr == text || end_ptr == NULL || *end_ptr != '\0') {
        return false;
    }

    *out_value = (uint64_t)value;
    return true;
}

static void print_usage(const char *program_name)
{
    printf(
        "Usage: %s --socket <path> [options]\n"
        "Options:\n"
        "  --max-clients <n>          default %u (max 64)\n"
        "  --max-topic-routes <n>     default %u\n"
        "  --max-endpoint-routes <n>  default %u\n"
        "  --metrics-interval-ms <n>  default %u (0 disables periodic print)\n"
        "  --routes-file <path>       explicit topic route config\n"
        "  --run-ms <n>               stop after n milliseconds (0 = run until signal)\n"
        "  --help\n",
        program_name,
        (unsigned)CD_BROKER_DEFAULT_MAX_CLIENTS,
        (unsigned)CD_BROKER_DEFAULT_MAX_TOPIC_ROUTES,
        (unsigned)CD_BROKER_DEFAULT_MAX_ENDPOINT_ROUTES,
        (unsigned)CD_BROKER_DEFAULT_METRICS_INTERVAL_MS
    );
}

static bool parse_args(int argc, char **argv, broker_config_t *out_config)
{
    broker_config_t config;
    int i;

    if (out_config == NULL) {
        return false;
    }

    memset(&config, 0, sizeof(config));
    config.max_clients = CD_BROKER_DEFAULT_MAX_CLIENTS;
    config.max_topic_routes = CD_BROKER_DEFAULT_MAX_TOPIC_ROUTES;
    config.max_endpoint_routes = CD_BROKER_DEFAULT_MAX_ENDPOINT_ROUTES;
    config.metrics_interval_ms = CD_BROKER_DEFAULT_METRICS_INTERVAL_MS;
    config.run_ms = 0u;

    i = 1;
    while (i < argc) {
        uint64_t value;
        const char *arg;

        arg = argv[i];
        if (strcmp(arg, "--help") == 0) {
            print_usage(argv[0]);
            return false;
        }
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", arg);
            print_usage(argv[0]);
            return false;
        }

        if (strcmp(arg, "--socket") == 0) {
            config.socket_path = argv[i + 1];
        } else if (strcmp(arg, "--max-clients") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value) || value == 0u || value > 64u) {
                fprintf(stderr, "Invalid --max-clients value: %s\n", argv[i + 1]);
                return false;
            }
            config.max_clients = (size_t)value;
        } else if (strcmp(arg, "--max-topic-routes") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value) || value == 0u) {
                fprintf(stderr, "Invalid --max-topic-routes value: %s\n", argv[i + 1]);
                return false;
            }
            config.max_topic_routes = (size_t)value;
        } else if (strcmp(arg, "--max-endpoint-routes") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value) || value == 0u) {
                fprintf(stderr, "Invalid --max-endpoint-routes value: %s\n", argv[i + 1]);
                return false;
            }
            config.max_endpoint_routes = (size_t)value;
        } else if (strcmp(arg, "--metrics-interval-ms") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value)) {
                fprintf(stderr, "Invalid --metrics-interval-ms value: %s\n", argv[i + 1]);
                return false;
            }
            config.metrics_interval_ms = value;
        } else if (strcmp(arg, "--routes-file") == 0) {
            config.routes_file = argv[i + 1];
        } else if (strcmp(arg, "--run-ms") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value)) {
                fprintf(stderr, "Invalid --run-ms value: %s\n", argv[i + 1]);
                return false;
            }
            config.run_ms = value;
        } else {
            fprintf(stderr, "Unknown option: %s\n", arg);
            print_usage(argv[0]);
            return false;
        }

        i += 2;
    }

    if (config.socket_path == NULL || config.socket_path[0] == '\0') {
        fprintf(stderr, "--socket is required.\n");
        print_usage(argv[0]);
        return false;
    }

    *out_config = config;
    return true;
}

static bool install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = broker_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, NULL) != 0) {
        return false;
    }
    if (sigaction(SIGTERM, &sa, NULL) != 0) {
        return false;
    }

    return true;
}

static int create_listener_socket(const char *socket_path)
{
    int listener_fd;
    int flags;
    struct sockaddr_un address;

    if (socket_path == NULL || strlen(socket_path) >= sizeof(address.sun_path)) {
        return -1;
    }

    listener_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listener_fd < 0) {
        return -1;
    }

    memset(&address, 0, sizeof(address));
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
    address.sun_len = (uint8_t)sizeof(address);
#endif
    address.sun_family = AF_UNIX;
    (void)strncpy(address.sun_path, socket_path, sizeof(address.sun_path) - 1u);

    unlink(socket_path);
    if (bind(listener_fd, (const struct sockaddr *)&address, (socklen_t)sizeof(address)) != 0) {
        fprintf(stderr, "Broker bind error on %s: %s\n", socket_path, strerror(errno));
        close(listener_fd);
        return -1;
    }

    if (listen(listener_fd, (int)CD_BROKER_DEFAULT_MAX_CLIENTS) != 0) {
        close(listener_fd);
        unlink(socket_path);
        return -1;
    }

    flags = fcntl(listener_fd, F_GETFL, 0);
    if (flags < 0 || fcntl(listener_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(listener_fd);
        unlink(socket_path);
        return -1;
    }

    return listener_fd;
}

static uint64_t active_client_mask(const broker_state_t *state)
{
    uint64_t mask;
    size_t i;

    mask = 0u;
    for (i = 0; i < state->client_capacity; ++i) {
        if (state->clients[i].in_use) {
            mask |= (1ull << i);
        }
    }

    return mask;
}

static void remove_endpoint_routes_for_client(broker_state_t *state, size_t client_index)
{
    size_t i;

    for (i = 0; i < state->endpoint_route_capacity; ++i) {
        if (state->endpoint_routes[i].in_use &&
            state->endpoint_routes[i].client_index == client_index) {
            memset(&state->endpoint_routes[i], 0, sizeof(state->endpoint_routes[i]));
        }
    }
}

static void disconnect_client(broker_state_t *state, size_t client_index)
{
    size_t i;
    broker_client_t *client;

    if (state == NULL || client_index >= state->client_capacity) {
        return;
    }

    client = &state->clients[client_index];
    if (!client->in_use) {
        return;
    }

    cd_ipc_socket_transport_close(&client->transport);
    memset(client, 0, sizeof(*client));
    remove_endpoint_routes_for_client(state, client_index);

    for (i = 0; i < state->topic_route_capacity; ++i) {
        if (state->topic_routes[i].in_use) {
            state->topic_routes[i].recipient_mask &= ~(1ull << client_index);
        }
    }
}

static size_t find_free_client_slot(const broker_state_t *state)
{
    size_t i;

    for (i = 0; i < state->client_capacity; ++i) {
        if (!state->clients[i].in_use) {
            return i;
        }
    }

    return SIZE_MAX;
}

static broker_topic_route_t *find_topic_route(
    broker_state_t *state,
    cd_topic_t topic,
    bool create_if_missing
)
{
    size_t i;

    for (i = 0; i < state->topic_route_capacity; ++i) {
        broker_topic_route_t *route;

        route = &state->topic_routes[i];
        if (route->in_use && route->topic == topic) {
            return route;
        }
    }

    if (!create_if_missing) {
        return NULL;
    }

    for (i = 0; i < state->topic_route_capacity; ++i) {
        broker_topic_route_t *route;

        route = &state->topic_routes[i];
        if (route->in_use) {
            continue;
        }

        route->in_use = true;
        route->topic = topic;
        route->recipient_mask = active_client_mask(state);
        return route;
    }

    return NULL;
}

static bool load_routes_file(broker_state_t *state, const char *routes_file)
{
    FILE *file;
    char line[1024];
    unsigned line_number;

    if (state == NULL || routes_file == NULL || routes_file[0] == '\0') {
        return false;
    }

    file = fopen(routes_file, "r");
    if (file == NULL) {
        fprintf(stderr, "Broker could not open routes file: %s\n", routes_file);
        return false;
    }

    line_number = 0u;
    while (fgets(line, sizeof(line), file) != NULL) {
        char *save_ptr;
        char *token;
        char *topic_token;
        broker_topic_route_t *route;
        uint64_t topic_u64;

        line_number += 1u;
        save_ptr = NULL;
        token = strtok_r(line, " \t\r\n", &save_ptr);
        if (token == NULL || token[0] == '#') {
            continue;
        }

        if (strcmp(token, "topic") != 0) {
            fprintf(
                stderr,
                "Broker routes file parse error at line %u: expected 'topic'\n",
                line_number
            );
            fclose(file);
            return false;
        }

        topic_token = strtok_r(NULL, " \t\r\n", &save_ptr);
        if (topic_token == NULL || !parse_u64_token(topic_token, &topic_u64) ||
            topic_u64 > (uint64_t)UINT32_MAX) {
            fprintf(
                stderr,
                "Broker routes file parse error at line %u: invalid topic id\n",
                line_number
            );
            fclose(file);
            return false;
        }

        route = find_topic_route(state, (cd_topic_t)topic_u64, true);
        if (route == NULL) {
            fprintf(
                stderr,
                "Broker routes file parse error at line %u: topic capacity reached\n",
                line_number
            );
            fclose(file);
            return false;
        }
        route->has_configured_endpoints = true;
        route->configured_endpoint_count = 0u;

        while (true) {
            uint64_t endpoint_u64;
            char *endpoint_token;

            endpoint_token = strtok_r(NULL, " \t\r\n", &save_ptr);
            if (endpoint_token == NULL) {
                break;
            }
            if (!parse_u64_token(endpoint_token, &endpoint_u64) ||
                endpoint_u64 == 0u || endpoint_u64 > (uint64_t)UINT32_MAX) {
                fprintf(
                    stderr,
                    "Broker routes file parse error at line %u: invalid endpoint id\n",
                    line_number
                );
                fclose(file);
                return false;
            }
            if (route->configured_endpoint_count >= CD_BROKER_MAX_TOPIC_ENDPOINTS) {
                fprintf(
                    stderr,
                    "Broker routes file parse error at line %u: too many topic endpoints\n",
                    line_number
                );
                fclose(file);
                return false;
            }

            route->configured_endpoints[route->configured_endpoint_count] =
                (cd_endpoint_id_t)endpoint_u64;
            route->configured_endpoint_count += 1u;
        }

        if (route->configured_endpoint_count == 0u) {
            fprintf(
                stderr,
                "Broker routes file parse error at line %u: topic needs at least one endpoint\n",
                line_number
            );
            fclose(file);
            return false;
        }
    }

    fclose(file);
    return true;
}

static void upsert_endpoint_route(
    broker_state_t *state,
    cd_endpoint_id_t endpoint,
    size_t client_index
)
{
    size_t i;

    if (endpoint == CD_ENDPOINT_NONE) {
        return;
    }

    for (i = 0; i < state->endpoint_route_capacity; ++i) {
        broker_endpoint_route_t *route;

        route = &state->endpoint_routes[i];
        if (route->in_use && route->endpoint == endpoint) {
            route->client_index = client_index;
            return;
        }
    }

    for (i = 0; i < state->endpoint_route_capacity; ++i) {
        broker_endpoint_route_t *route;

        route = &state->endpoint_routes[i];
        if (route->in_use) {
            continue;
        }

        route->in_use = true;
        route->endpoint = endpoint;
        route->client_index = client_index;
        return;
    }
}

static bool lookup_endpoint_route(
    const broker_state_t *state,
    cd_endpoint_id_t endpoint,
    size_t *out_client_index
)
{
    size_t i;

    if (endpoint == CD_ENDPOINT_NONE || out_client_index == NULL) {
        return false;
    }

    for (i = 0; i < state->endpoint_route_capacity; ++i) {
        const broker_endpoint_route_t *route;

        route = &state->endpoint_routes[i];
        if (!route->in_use || route->endpoint != endpoint) {
            continue;
        }
        if (route->client_index >= state->client_capacity ||
            !state->clients[route->client_index].in_use) {
            return false;
        }
        *out_client_index = route->client_index;
        return true;
    }

    return false;
}

static cd_message_id_t next_generated_message_id(broker_state_t *state)
{
    cd_message_id_t message_id;

    if (state == NULL) {
        return 0u;
    }

    message_id = state->next_generated_message_id++;
    if (message_id == 0u) {
        message_id = state->next_generated_message_id++;
    }

    return message_id;
}

static size_t write_metrics_snapshot_payload(const broker_state_t *state, char *buffer, size_t capacity)
{
    int written;

    if (state == NULL || buffer == NULL || capacity == 0u) {
        return 0u;
    }

    written = snprintf(
        buffer,
        capacity,
        "clients=%zu published=%" PRIu64
        " delivered=%" PRIu64 " dropped=%" PRIu64
        " timeouts=%" PRIu64 " transport_errors=%" PRIu64,
        active_client_count(state),
        state->metrics.published,
        state->metrics.delivered,
        state->metrics.dropped,
        state->metrics.timeouts,
        state->metrics.transport_errors
    );
    if (written < 0) {
        return 0u;
    }
    if ((size_t)written >= capacity) {
        return capacity - 1u;
    }
    return (size_t)written;
}

static bool try_handle_diagnostics_request(
    broker_state_t *state,
    size_t source_client_index,
    const cd_envelope_t *message
)
{
    cd_envelope_t reply;
    cd_status_t send_status;
    char payload[192];
    size_t payload_size;

    if (state == NULL || message == NULL) {
        return false;
    }
    if (source_client_index >= state->client_capacity ||
        !state->clients[source_client_index].in_use) {
        return false;
    }
    if (message->kind != CD_MESSAGE_REQUEST ||
        message->target_endpoint != CD_BROKER_DIAGNOSTICS_ENDPOINT ||
        message->source_endpoint == CD_ENDPOINT_NONE) {
        return false;
    }

    payload_size = write_metrics_snapshot_payload(state, payload, sizeof(payload));
    if (payload_size == 0u) {
        return false;
    }

    memset(&reply, 0, sizeof(reply));
    reply.message_id = next_generated_message_id(state);
    reply.correlation_id = message->message_id;
    reply.kind = CD_MESSAGE_REPLY;
    reply.topic = CD_BROKER_DIAGNOSTICS_TOPIC_METRICS;
    reply.source_endpoint = CD_BROKER_DIAGNOSTICS_ENDPOINT;
    reply.target_endpoint = message->source_endpoint;
    reply.schema_id = CD_BROKER_DIAGNOSTICS_SCHEMA_METRICS_TEXT;
    reply.schema_version = 1u;
    reply.payload = payload;
    reply.payload_size = payload_size;

    send_status = state->clients[source_client_index].transport.send(
        state->clients[source_client_index].transport.impl,
        &reply
    );
    if (send_status == CD_STATUS_OK) {
        state->metrics.delivered += 1u;
        return true;
    }

    state->metrics.transport_errors += 1u;
    state->metrics.dropped += 1u;
    if (send_status == CD_STATUS_TRANSPORT_UNAVAILABLE ||
        send_status == CD_STATUS_SCHEMA_MISMATCH) {
        disconnect_client(state, source_client_index);
    }
    return true;
}

static void route_incoming_message(
    broker_state_t *state,
    size_t source_client_index,
    const cd_envelope_t *message
)
{
    uint64_t target_mask;
    size_t i;

    state->metrics.published += 1u;
    upsert_endpoint_route(state, message->source_endpoint, source_client_index);
    if (try_handle_diagnostics_request(state, source_client_index, message)) {
        return;
    }

    target_mask = 0u;
    if (message->kind == CD_MESSAGE_EVENT) {
        broker_topic_route_t *topic_route;

        topic_route = find_topic_route(state, message->topic, !state->explicit_topic_routes_only);
        if (topic_route == NULL) {
            state->metrics.dropped += 1u;
            return;
        }
        if (topic_route->has_configured_endpoints) {
            for (i = 0; i < topic_route->configured_endpoint_count; ++i) {
                size_t endpoint_client_index;

                if (lookup_endpoint_route(
                        state,
                        topic_route->configured_endpoints[i],
                        &endpoint_client_index
                    )) {
                    target_mask |= (1ull << endpoint_client_index);
                }
            }
        } else {
            target_mask = topic_route->recipient_mask;
        }
    } else if (message->kind == CD_MESSAGE_COMMAND ||
               message->kind == CD_MESSAGE_REQUEST ||
               message->kind == CD_MESSAGE_REPLY) {
        size_t target_client_index;

        if (!lookup_endpoint_route(state, message->target_endpoint, &target_client_index)) {
            state->metrics.dropped += 1u;
            if (message->kind == CD_MESSAGE_REQUEST) {
                state->metrics.timeouts += 1u;
            }
            return;
        }

        target_mask = (1ull << target_client_index);
    } else {
        state->metrics.dropped += 1u;
        return;
    }

    target_mask &= active_client_mask(state);
    target_mask &= ~(1ull << source_client_index);
    if (target_mask == 0u) {
        state->metrics.dropped += 1u;
        if (message->kind == CD_MESSAGE_REQUEST) {
            state->metrics.timeouts += 1u;
        }
        return;
    }

    for (i = 0; i < state->client_capacity; ++i) {
        cd_status_t send_status;

        if ((target_mask & (1ull << i)) == 0u) {
            continue;
        }

        send_status = state->clients[i].transport.send(state->clients[i].transport.impl, message);
        if (send_status == CD_STATUS_OK) {
            state->metrics.delivered += 1u;
            continue;
        }

        state->metrics.transport_errors += 1u;
        state->metrics.dropped += 1u;
        if (send_status == CD_STATUS_TRANSPORT_UNAVAILABLE ||
            send_status == CD_STATUS_SCHEMA_MISMATCH) {
            disconnect_client(state, i);
        }
    }
}

static cd_status_t on_transport_receive(void *user_data, const cd_envelope_t *message)
{
    broker_receive_context_t *context;

    context = (broker_receive_context_t *)user_data;
    if (context == NULL || context->state == NULL || message == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    if (context->source_client_index >= context->state->client_capacity ||
        !context->state->clients[context->source_client_index].in_use) {
        return CD_STATUS_TRANSPORT_UNAVAILABLE;
    }

    route_incoming_message(context->state, context->source_client_index, message);
    return CD_STATUS_OK;
}

static void accept_new_clients(broker_state_t *state, int listener_fd)
{
    while (true) {
        int client_fd;
        size_t slot;
        cd_status_t status;
        cd_transport_t transport;
        size_t i;

        client_fd = accept(listener_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            state->metrics.transport_errors += 1u;
            break;
        }

        slot = find_free_client_slot(state);
        if (slot == SIZE_MAX) {
            close(client_fd);
            state->metrics.dropped += 1u;
            continue;
        }

        memset(&transport, 0, sizeof(transport));
        status = cd_ipc_socket_transport_init(state->context, client_fd, NULL, &transport);
        if (status != CD_STATUS_OK) {
            close(client_fd);
            state->metrics.transport_errors += 1u;
            continue;
        }

        state->clients[slot].in_use = true;
        state->clients[slot].transport = transport;
        for (i = 0; i < state->topic_route_capacity; ++i) {
            if (state->topic_routes[i].in_use &&
                !state->topic_routes[i].has_configured_endpoints) {
                state->topic_routes[i].recipient_mask |= (1ull << slot);
            }
        }
    }
}

static void poll_clients(broker_state_t *state)
{
    size_t i;

    for (i = 0; i < state->client_capacity; ++i) {
        broker_receive_context_t receive_context;
        cd_status_t poll_status;

        if (!state->clients[i].in_use) {
            continue;
        }

        memset(&receive_context, 0, sizeof(receive_context));
        receive_context.state = state;
        receive_context.source_client_index = i;
        poll_status = state->clients[i].transport.poll(
            state->clients[i].transport.impl,
            on_transport_receive,
            &receive_context,
            64u,
            NULL
        );
        if (poll_status == CD_STATUS_OK) {
            continue;
        }

        state->metrics.transport_errors += 1u;
        if (poll_status == CD_STATUS_TRANSPORT_UNAVAILABLE ||
            poll_status == CD_STATUS_SCHEMA_MISMATCH) {
            disconnect_client(state, i);
        }
    }
}

static size_t active_client_count(const broker_state_t *state)
{
    size_t count;
    size_t i;

    count = 0u;
    for (i = 0; i < state->client_capacity; ++i) {
        if (state->clients[i].in_use) {
            count += 1u;
        }
    }
    return count;
}

static void print_metrics(const broker_state_t *state, bool final_snapshot)
{
    printf(
        "[broker] %s clients=%zu published=%" PRIu64
        " delivered=%" PRIu64 " dropped=%" PRIu64 " timeouts=%" PRIu64
        " transport_errors=%" PRIu64 "\n",
        final_snapshot ? "final" : "metrics",
        active_client_count(state),
        state->metrics.published,
        state->metrics.delivered,
        state->metrics.dropped,
        state->metrics.timeouts,
        state->metrics.transport_errors
    );
}

int main(int argc, char **argv)
{
    broker_config_t config;
    broker_state_t state;
    int listener_fd;
    uint64_t start_ms;
    uint64_t next_metrics_ms;

    if (!parse_args(argc, argv, &config)) {
        return 1;
    }

    memset(&state, 0, sizeof(state));
    state.next_generated_message_id = 1u;
    if (cd_context_init(&state.context, NULL) != CD_STATUS_OK) {
        fprintf(stderr, "Broker failed to initialize conduit context.\n");
        return 1;
    }

    state.client_capacity = config.max_clients;
    state.topic_route_capacity = config.max_topic_routes;
    state.endpoint_route_capacity = config.max_endpoint_routes;
    state.clients = (broker_client_t *)calloc(state.client_capacity, sizeof(*state.clients));
    state.topic_routes =
        (broker_topic_route_t *)calloc(state.topic_route_capacity, sizeof(*state.topic_routes));
    state.endpoint_routes = (broker_endpoint_route_t *)calloc(
        state.endpoint_route_capacity,
        sizeof(*state.endpoint_routes)
    );
    if (state.clients == NULL || state.topic_routes == NULL || state.endpoint_routes == NULL) {
        fprintf(stderr, "Broker allocation failure.\n");
        free(state.endpoint_routes);
        free(state.topic_routes);
        free(state.clients);
        cd_context_shutdown(state.context);
        return 1;
    }

    if (!install_signal_handlers()) {
        fprintf(stderr, "Broker failed to install signal handlers.\n");
        free(state.endpoint_routes);
        free(state.topic_routes);
        free(state.clients);
        cd_context_shutdown(state.context);
        return 1;
    }

    if (config.routes_file != NULL) {
        state.explicit_topic_routes_only = true;
        if (!load_routes_file(&state, config.routes_file)) {
            free(state.endpoint_routes);
            free(state.topic_routes);
            free(state.clients);
            cd_context_shutdown(state.context);
            return 1;
        }
    }

    listener_fd = create_listener_socket(config.socket_path);
    if (listener_fd < 0) {
        fprintf(stderr, "Broker failed to open socket path: %s\n", config.socket_path);
        free(state.endpoint_routes);
        free(state.topic_routes);
        free(state.clients);
        cd_context_shutdown(state.context);
        return 1;
    }

    printf("[broker] listening on %s\n", config.socket_path);
    start_ms = broker_now_ms();
    next_metrics_ms = 0u;
    if (config.metrics_interval_ms > 0u) {
        next_metrics_ms = start_ms + config.metrics_interval_ms;
    }

    while (g_broker_running) {
        uint64_t now_ms;

        now_ms = broker_now_ms();
        if (config.run_ms > 0u && now_ms - start_ms >= config.run_ms) {
            break;
        }

        accept_new_clients(&state, listener_fd);
        poll_clients(&state);

        if (config.metrics_interval_ms > 0u && now_ms >= next_metrics_ms) {
            print_metrics(&state, false);
            next_metrics_ms = now_ms + config.metrics_interval_ms;
        }

        broker_sleep_ms(1u);
    }

    print_metrics(&state, true);
    {
        size_t i;

        for (i = 0; i < state.client_capacity; ++i) {
            disconnect_client(&state, i);
        }
    }

    close(listener_fd);
    unlink(config.socket_path);
    free(state.endpoint_routes);
    free(state.topic_routes);
    free(state.clients);
    cd_context_shutdown(state.context);
    return 0;
}
