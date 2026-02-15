#include "conduit/conduit.h"

#include <errno.h>
#include <inttypes.h>
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
    CD_LOADGEN_TOPIC_EVENT = 0x00F00001u,
    CD_LOADGEN_TOPIC_REQUEST = 0x00F00002u,
    CD_LOADGEN_TOPIC_REGISTER = 0x00F00003u
};

enum {
    CD_LOADGEN_SCHEMA_PAYLOAD = 0x00F10001u,
    CD_LOADGEN_SCHEMA_REPLY = 0x00F10002u
};

enum {
    CD_LOADGEN_ENDPOINT_SENDER = 500u,
    CD_LOADGEN_ENDPOINT_EVENT_CONSUMER = 501u,
    CD_LOADGEN_ENDPOINT_REPLIER = 502u
};

typedef struct loadgen_config {
    const char *socket_path;
    uint64_t events;
    uint64_t requests;
    size_t payload_size;
    uint64_t request_timeout_ns;
    uint64_t max_duration_ms;
    size_t max_queued_messages;
    size_t max_inflight_requests;
    int connect_attempts;
    useconds_t connect_retry_sleep_us;
} loadgen_config_t;

typedef struct loadgen_state {
    cd_bus_t *bus_replier;
    uint64_t events_received;
    uint64_t request_hits;
} loadgen_state_t;

static uint64_t now_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0u;
    }
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)(ts.tv_nsec / 1000000u);
}

static uint64_t now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0u;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
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

static void print_usage(const char *program_name)
{
    printf(
        "Usage: %s --socket <path> [options]\n"
        "Options:\n"
        "  --events <n>              default 5000\n"
        "  --requests <n>            default 1000\n"
        "  --payload-size <bytes>    default 64 (min 8)\n"
        "  --request-timeout-ns <n>  default 1000000000\n"
        "  --max-duration-ms <n>     default 15000\n"
        "  --max-queued-messages <n> default 4096\n"
        "  --max-inflight-requests <n> default 256\n"
        "  --connect-attempts <n>    default 3000\n"
        "  --help\n",
        program_name
    );
}

static bool parse_args(int argc, char **argv, loadgen_config_t *out_config)
{
    loadgen_config_t config;
    int i;

    if (out_config == NULL) {
        return false;
    }

    memset(&config, 0, sizeof(config));
    config.events = 5000u;
    config.requests = 1000u;
    config.payload_size = 64u;
    config.request_timeout_ns = 1000000000ull;
    config.max_duration_ms = 15000u;
    config.max_queued_messages = 4096u;
    config.max_inflight_requests = 256u;
    config.connect_attempts = 3000;
    config.connect_retry_sleep_us = 1000u;

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
        } else if (strcmp(arg, "--events") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value)) {
                return false;
            }
            config.events = value;
        } else if (strcmp(arg, "--requests") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value)) {
                return false;
            }
            config.requests = value;
        } else if (strcmp(arg, "--payload-size") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value) || value > SIZE_MAX) {
                return false;
            }
            config.payload_size = (size_t)value;
        } else if (strcmp(arg, "--request-timeout-ns") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value)) {
                return false;
            }
            config.request_timeout_ns = value;
        } else if (strcmp(arg, "--max-duration-ms") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value)) {
                return false;
            }
            config.max_duration_ms = value;
        } else if (strcmp(arg, "--max-queued-messages") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value) || value == 0u || value > SIZE_MAX) {
                return false;
            }
            config.max_queued_messages = (size_t)value;
        } else if (strcmp(arg, "--max-inflight-requests") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value) || value == 0u || value > SIZE_MAX) {
                return false;
            }
            config.max_inflight_requests = (size_t)value;
        } else if (strcmp(arg, "--connect-attempts") == 0) {
            if (!parse_u64_arg(argv[i + 1], &value) || value == 0u || value > (uint64_t)INT32_MAX) {
                return false;
            }
            config.connect_attempts = (int)value;
        } else {
            fprintf(stderr, "Unknown option: %s\n", arg);
            print_usage(argv[0]);
            return false;
        }

        i += 2;
    }

    if (config.socket_path == NULL || config.socket_path[0] == '\0') {
        fprintf(stderr, "--socket is required.\n");
        return false;
    }
    if (config.payload_size < sizeof(uint64_t)) {
        config.payload_size = sizeof(uint64_t);
    }

    *out_config = config;
    return true;
}

static int connect_unix_socket_retry(
    const char *socket_path,
    int max_attempts,
    useconds_t sleep_us
)
{
    int attempt;
    struct sockaddr_un address;

    if (socket_path == NULL || max_attempts <= 0 || strlen(socket_path) >= sizeof(address.sun_path)) {
        return -1;
    }

    memset(&address, 0, sizeof(address));
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
    address.sun_len = (uint8_t)sizeof(address);
#endif
    address.sun_family = AF_UNIX;
    (void)strncpy(address.sun_path, socket_path, sizeof(address.sun_path) - 1u);

    for (attempt = 0; attempt < max_attempts; ++attempt) {
        int fd;

        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (connect(fd, (const struct sockaddr *)&address, sizeof(address)) == 0) {
            return fd;
        }

        close(fd);
        if (errno != ENOENT && errno != ECONNREFUSED && errno != EINTR) {
            return -1;
        }
        usleep(sleep_us);
    }

    return -1;
}

static cd_status_t on_event_consume(void *user_data, const cd_envelope_t *message)
{
    loadgen_state_t *state;

    state = (loadgen_state_t *)user_data;
    if (state == NULL || message == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    if (message->kind != CD_MESSAGE_EVENT || message->topic != CD_LOADGEN_TOPIC_EVENT ||
        message->payload == NULL || message->payload_size < sizeof(uint64_t)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    state->events_received += 1u;
    return CD_STATUS_OK;
}

static cd_status_t on_request_replier(void *user_data, const cd_envelope_t *message)
{
    loadgen_state_t *state;
    cd_reply_params_t reply_params;
    uint64_t reply_value;

    state = (loadgen_state_t *)user_data;
    if (state == NULL || state->bus_replier == NULL || message == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    if (message->kind != CD_MESSAGE_REQUEST || message->topic != CD_LOADGEN_TOPIC_REQUEST ||
        message->payload == NULL || message->payload_size < sizeof(uint64_t)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    state->request_hits += 1u;
    memcpy(&reply_value, message->payload, sizeof(reply_value));

    memset(&reply_params, 0, sizeof(reply_params));
    reply_params.source_endpoint = CD_LOADGEN_ENDPOINT_REPLIER;
    reply_params.target_endpoint = message->source_endpoint;
    reply_params.correlation_id = message->message_id;
    reply_params.topic = CD_LOADGEN_TOPIC_REQUEST;
    reply_params.schema_id = CD_LOADGEN_SCHEMA_REPLY;
    reply_params.schema_version = 1u;
    reply_params.flags = 0u;
    reply_params.payload = &reply_value;
    reply_params.payload_size = sizeof(reply_value);
    return cd_send_reply(state->bus_replier, &reply_params, NULL);
}

static cd_status_t pump_pair(cd_bus_t *bus_a, cd_bus_t *bus_b, size_t *out_total_processed)
{
    size_t processed_a;
    size_t processed_b;
    cd_status_t status;

    processed_a = 0u;
    processed_b = 0u;
    status = cd_bus_pump(bus_a, 0u, &processed_a);
    if (status != CD_STATUS_OK) {
        return status;
    }
    status = cd_bus_pump(bus_b, 0u, &processed_b);
    if (status != CD_STATUS_OK) {
        return status;
    }

    if (out_total_processed != NULL) {
        *out_total_processed = processed_a + processed_b;
    }
    return CD_STATUS_OK;
}

int main(int argc, char **argv)
{
    loadgen_config_t config;
    loadgen_state_t state;
    cd_context_t *context;
    cd_bus_t *bus_sender;
    cd_bus_t *bus_replier;
    cd_bus_config_t bus_config;
    cd_transport_t transport_sender;
    cd_transport_t transport_replier;
    cd_subscription_desc_t sub;
    uint8_t *payload_buffer;
    uint64_t events_sent;
    uint64_t requests_sent;
    uint64_t replies_received;
    uint64_t start_ms;
    uint64_t deadline_ms;
    uint64_t event_phase_start_ms;
    uint64_t request_phase_start_ms;
    uint64_t event_phase_elapsed_ms;
    uint64_t request_phase_elapsed_ms;
    uint64_t event_queue_full_retries;
    uint64_t request_queue_full_retries;
    uint64_t request_rtt_total_ns;
    uint64_t request_rtt_max_ns;
    int sender_fd;
    int replier_fd;

    if (!parse_args(argc, argv, &config)) {
        return 1;
    }

    memset(&state, 0, sizeof(state));
    context = NULL;
    bus_sender = NULL;
    bus_replier = NULL;
    sender_fd = -1;
    replier_fd = -1;
    payload_buffer = NULL;
    memset(&transport_sender, 0, sizeof(transport_sender));
    memset(&transport_replier, 0, sizeof(transport_replier));

    sender_fd = connect_unix_socket_retry(
        config.socket_path,
        config.connect_attempts,
        config.connect_retry_sleep_us
    );
    replier_fd = connect_unix_socket_retry(
        config.socket_path,
        config.connect_attempts,
        config.connect_retry_sleep_us
    );
    if (sender_fd < 0 || replier_fd < 0) {
        fprintf(stderr, "[loadgen] failed to connect to broker socket: %s\n", config.socket_path);
        close(sender_fd);
        close(replier_fd);
        return 1;
    }

    if (cd_context_init(&context, NULL) != CD_STATUS_OK) {
        close(sender_fd);
        close(replier_fd);
        return 1;
    }

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = config.max_queued_messages;
    bus_config.max_subscriptions = 32u;
    bus_config.max_inflight_requests = config.max_inflight_requests;
    bus_config.max_transports = 1u;
    if (cd_bus_create(context, &bus_config, &bus_sender) != CD_STATUS_OK ||
        cd_bus_create(context, &bus_config, &bus_replier) != CD_STATUS_OK) {
        cd_bus_destroy(bus_sender);
        cd_bus_destroy(bus_replier);
        cd_context_shutdown(context);
        close(sender_fd);
        close(replier_fd);
        return 1;
    }

    if (cd_ipc_socket_transport_init(context, sender_fd, NULL, &transport_sender) != CD_STATUS_OK ||
        cd_ipc_socket_transport_init(context, replier_fd, NULL, &transport_replier) != CD_STATUS_OK) {
        cd_ipc_socket_transport_close(&transport_sender);
        cd_ipc_socket_transport_close(&transport_replier);
        cd_bus_destroy(bus_sender);
        cd_bus_destroy(bus_replier);
        cd_context_shutdown(context);
        close(sender_fd);
        close(replier_fd);
        return 1;
    }
    sender_fd = -1;
    replier_fd = -1;

    if (cd_bus_attach_transport(bus_sender, &transport_sender) != CD_STATUS_OK ||
        cd_bus_attach_transport(bus_replier, &transport_replier) != CD_STATUS_OK) {
        cd_ipc_socket_transport_close(&transport_sender);
        cd_ipc_socket_transport_close(&transport_replier);
        cd_bus_destroy(bus_sender);
        cd_bus_destroy(bus_replier);
        cd_context_shutdown(context);
        return 1;
    }

    state.bus_replier = bus_replier;
    memset(&sub, 0, sizeof(sub));
    sub.endpoint = CD_LOADGEN_ENDPOINT_EVENT_CONSUMER;
    sub.topic = CD_LOADGEN_TOPIC_EVENT;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = on_event_consume;
    sub.user_data = &state;
    if (cd_subscribe(bus_replier, &sub, NULL) != CD_STATUS_OK) {
        cd_ipc_socket_transport_close(&transport_sender);
        cd_ipc_socket_transport_close(&transport_replier);
        cd_bus_destroy(bus_sender);
        cd_bus_destroy(bus_replier);
        cd_context_shutdown(context);
        return 1;
    }

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = CD_LOADGEN_ENDPOINT_REPLIER;
    sub.topic = CD_LOADGEN_TOPIC_REQUEST;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    sub.handler = on_request_replier;
    sub.user_data = &state;
    if (cd_subscribe(bus_replier, &sub, NULL) != CD_STATUS_OK) {
        cd_ipc_socket_transport_close(&transport_sender);
        cd_ipc_socket_transport_close(&transport_replier);
        cd_bus_destroy(bus_sender);
        cd_bus_destroy(bus_replier);
        cd_context_shutdown(context);
        return 1;
    }

    payload_buffer = (uint8_t *)malloc(config.payload_size);
    if (payload_buffer == NULL) {
        cd_ipc_socket_transport_close(&transport_sender);
        cd_ipc_socket_transport_close(&transport_replier);
        cd_bus_destroy(bus_sender);
        cd_bus_destroy(bus_replier);
        cd_context_shutdown(context);
        return 1;
    }
    memset(payload_buffer, 0, config.payload_size);

    {
        cd_publish_params_t registration;

        memset(&registration, 0, sizeof(registration));
        registration.source_endpoint = CD_LOADGEN_ENDPOINT_EVENT_CONSUMER;
        registration.topic = CD_LOADGEN_TOPIC_REGISTER;
        registration.flags = 0u;
        registration.payload = "register-event";
        registration.payload_size = strlen("register-event");
        if (cd_publish(bus_replier, &registration, NULL) != CD_STATUS_OK) {
            free(payload_buffer);
            cd_ipc_socket_transport_close(&transport_sender);
            cd_ipc_socket_transport_close(&transport_replier);
            cd_bus_destroy(bus_sender);
            cd_bus_destroy(bus_replier);
            cd_context_shutdown(context);
            return 1;
        }

        memset(&registration, 0, sizeof(registration));
        registration.source_endpoint = CD_LOADGEN_ENDPOINT_REPLIER;
        registration.topic = CD_LOADGEN_TOPIC_REGISTER;
        registration.flags = 0u;
        registration.payload = "register-request";
        registration.payload_size = strlen("register-request");
        if (cd_publish(bus_replier, &registration, NULL) != CD_STATUS_OK) {
            free(payload_buffer);
            cd_ipc_socket_transport_close(&transport_sender);
            cd_ipc_socket_transport_close(&transport_replier);
            cd_bus_destroy(bus_sender);
            cd_bus_destroy(bus_replier);
            cd_context_shutdown(context);
            return 1;
        }
    }

    start_ms = now_ms();
    deadline_ms = start_ms + config.max_duration_ms;
    event_phase_start_ms = start_ms;
    event_phase_elapsed_ms = 0u;
    request_phase_start_ms = 0u;
    request_phase_elapsed_ms = 0u;
    event_queue_full_retries = 0u;
    request_queue_full_retries = 0u;
    request_rtt_total_ns = 0u;
    request_rtt_max_ns = 0u;
    events_sent = 0u;
    requests_sent = 0u;
    replies_received = 0u;

    while (events_sent < config.events || state.events_received < config.events) {
        cd_publish_params_t publish;
        size_t processed_total;
        bool progressed;
        uint64_t value;
        cd_status_t status;

        if (now_ms() >= deadline_ms) {
            fprintf(stderr, "[loadgen] timeout during event phase.\n");
            free(payload_buffer);
            cd_ipc_socket_transport_close(&transport_sender);
            cd_ipc_socket_transport_close(&transport_replier);
            cd_bus_destroy(bus_sender);
            cd_bus_destroy(bus_replier);
            cd_context_shutdown(context);
            return 1;
        }

        progressed = false;
        if (events_sent < config.events) {
            value = events_sent;
            memcpy(payload_buffer, &value, sizeof(value));

            memset(&publish, 0, sizeof(publish));
            publish.source_endpoint = CD_LOADGEN_ENDPOINT_SENDER;
            publish.topic = CD_LOADGEN_TOPIC_EVENT;
            publish.schema_id = CD_LOADGEN_SCHEMA_PAYLOAD;
            publish.schema_version = 1u;
            publish.flags = 0u;
            publish.payload = payload_buffer;
            publish.payload_size = config.payload_size;
            status = cd_publish(bus_sender, &publish, NULL);
            if (status == CD_STATUS_OK) {
                events_sent += 1u;
                progressed = true;
            } else if (status == CD_STATUS_QUEUE_FULL ||
                       status == CD_STATUS_CAPACITY_REACHED) {
                event_queue_full_retries += 1u;
            } else {
                fprintf(stderr, "[loadgen] event publish failed: %s\n", cd_status_string(status));
                free(payload_buffer);
                cd_ipc_socket_transport_close(&transport_sender);
                cd_ipc_socket_transport_close(&transport_replier);
                cd_bus_destroy(bus_sender);
                cd_bus_destroy(bus_replier);
                cd_context_shutdown(context);
                return 1;
            }
        }

        status = pump_pair(bus_sender, bus_replier, &processed_total);
        if (status != CD_STATUS_OK) {
            fprintf(stderr, "[loadgen] pump failed during event phase: %s\n", cd_status_string(status));
            free(payload_buffer);
            cd_ipc_socket_transport_close(&transport_sender);
            cd_ipc_socket_transport_close(&transport_replier);
            cd_bus_destroy(bus_sender);
            cd_bus_destroy(bus_replier);
            cd_context_shutdown(context);
            return 1;
        }
        if (processed_total > 0u) {
            progressed = true;
        }
        if (!progressed) {
            usleep(200u);
        }
    }
    event_phase_elapsed_ms = now_ms() - event_phase_start_ms;
    request_phase_start_ms = now_ms();

    while (requests_sent < config.requests) {
        cd_request_params_t request;
        cd_request_token_t token;
        uint64_t value;
        uint64_t request_send_ns;
        cd_status_t status;

        if (now_ms() >= deadline_ms) {
            fprintf(stderr, "[loadgen] timeout before request send.\n");
            free(payload_buffer);
            cd_ipc_socket_transport_close(&transport_sender);
            cd_ipc_socket_transport_close(&transport_replier);
            cd_bus_destroy(bus_sender);
            cd_bus_destroy(bus_replier);
            cd_context_shutdown(context);
            return 1;
        }

        value = requests_sent;
        memcpy(payload_buffer, &value, sizeof(value));

        memset(&request, 0, sizeof(request));
        request.source_endpoint = CD_LOADGEN_ENDPOINT_SENDER;
        request.target_endpoint = CD_LOADGEN_ENDPOINT_REPLIER;
        request.topic = CD_LOADGEN_TOPIC_REQUEST;
        request.schema_id = CD_LOADGEN_SCHEMA_PAYLOAD;
        request.schema_version = 1u;
        request.flags = 0u;
        request.timeout_ns = config.request_timeout_ns;
        request.payload = payload_buffer;
        request.payload_size = config.payload_size;

        token = 0u;
        request_send_ns = now_ns();
        status = cd_request_async(bus_sender, &request, &token);
        if (status == CD_STATUS_QUEUE_FULL || status == CD_STATUS_CAPACITY_REACHED) {
            size_t processed_total;
            request_queue_full_retries += 1u;
            status = pump_pair(bus_sender, bus_replier, &processed_total);
            if (status != CD_STATUS_OK) {
                fprintf(stderr, "[loadgen] pump failed before request send: %s\n", cd_status_string(status));
                free(payload_buffer);
                cd_ipc_socket_transport_close(&transport_sender);
                cd_ipc_socket_transport_close(&transport_replier);
                cd_bus_destroy(bus_sender);
                cd_bus_destroy(bus_replier);
                cd_context_shutdown(context);
                return 1;
            }
            usleep(200u);
            continue;
        }
        if (status != CD_STATUS_OK || token == 0u) {
            fprintf(stderr, "[loadgen] request send failed: %s\n", cd_status_string(status));
            free(payload_buffer);
            cd_ipc_socket_transport_close(&transport_sender);
            cd_ipc_socket_transport_close(&transport_replier);
            cd_bus_destroy(bus_sender);
            cd_bus_destroy(bus_replier);
            cd_context_shutdown(context);
            return 1;
        }
        requests_sent += 1u;

        while (true) {
            cd_reply_t reply;
            int ready;
            size_t processed_total;
            uint64_t reply_value;

            if (now_ms() >= deadline_ms) {
                fprintf(stderr, "[loadgen] timeout waiting for request reply.\n");
                free(payload_buffer);
                cd_ipc_socket_transport_close(&transport_sender);
                cd_ipc_socket_transport_close(&transport_replier);
                cd_bus_destroy(bus_sender);
                cd_bus_destroy(bus_replier);
                cd_context_shutdown(context);
                return 1;
            }

            status = pump_pair(bus_sender, bus_replier, &processed_total);
            if (status != CD_STATUS_OK) {
                fprintf(stderr, "[loadgen] pump failed during request phase: %s\n", cd_status_string(status));
                free(payload_buffer);
                cd_ipc_socket_transport_close(&transport_sender);
                cd_ipc_socket_transport_close(&transport_replier);
                cd_bus_destroy(bus_sender);
                cd_bus_destroy(bus_replier);
                cd_context_shutdown(context);
                return 1;
            }

            memset(&reply, 0, sizeof(reply));
            ready = 0;
            status = cd_poll_reply(bus_sender, token, &reply, &ready);
            if (status == CD_STATUS_OK && ready == 1) {
                if (reply.payload == NULL || reply.payload_size < sizeof(uint64_t)) {
                    cd_reply_dispose(bus_sender, &reply);
                    free(payload_buffer);
                    cd_ipc_socket_transport_close(&transport_sender);
                    cd_ipc_socket_transport_close(&transport_replier);
                    cd_bus_destroy(bus_sender);
                    cd_bus_destroy(bus_replier);
                    cd_context_shutdown(context);
                    return 1;
                }

                memcpy(&reply_value, reply.payload, sizeof(reply_value));
                cd_reply_dispose(bus_sender, &reply);
                if (reply_value != value) {
                    fprintf(stderr, "[loadgen] reply mismatch got=%" PRIu64 " expected=%" PRIu64 "\n", reply_value, value);
                    free(payload_buffer);
                    cd_ipc_socket_transport_close(&transport_sender);
                    cd_ipc_socket_transport_close(&transport_replier);
                    cd_bus_destroy(bus_sender);
                    cd_bus_destroy(bus_replier);
                    cd_context_shutdown(context);
                    return 1;
                }
                {
                    uint64_t request_rtt_ns;
                    request_rtt_ns = now_ns() - request_send_ns;
                    request_rtt_total_ns += request_rtt_ns;
                    if (request_rtt_ns > request_rtt_max_ns) {
                        request_rtt_max_ns = request_rtt_ns;
                    }
                }
                replies_received += 1u;
                break;
            }
            if (status == CD_STATUS_OK && ready == 0) {
                if (processed_total == 0u) {
                    usleep(200u);
                }
                continue;
            }

            fprintf(stderr, "[loadgen] reply poll failed: %s\n", cd_status_string(status));
            free(payload_buffer);
            cd_ipc_socket_transport_close(&transport_sender);
            cd_ipc_socket_transport_close(&transport_replier);
            cd_bus_destroy(bus_sender);
            cd_bus_destroy(bus_replier);
            cd_context_shutdown(context);
            return 1;
        }
    }
    request_phase_elapsed_ms = now_ms() - request_phase_start_ms;

    {
        uint64_t elapsed_ms;
        uint64_t total_messages;
        double throughput;
        double request_rtt_avg_us;
        double request_rtt_max_us;

        elapsed_ms = now_ms() - start_ms;
        if (elapsed_ms == 0u) {
            elapsed_ms = 1u;
        }
        total_messages = events_sent + requests_sent + replies_received;
        throughput = ((double)total_messages * 1000.0) / (double)elapsed_ms;
        request_rtt_avg_us =
            requests_sent > 0u ? ((double)request_rtt_total_ns / (double)requests_sent) / 1000.0 : 0.0;
        request_rtt_max_us = ((double)request_rtt_max_ns) / 1000.0;

        printf(
            "[loadgen] done events_sent=%" PRIu64 " events_received=%" PRIu64
            " requests_sent=%" PRIu64 " replies_received=%" PRIu64
            " request_hits=%" PRIu64 " elapsed_ms=%" PRIu64
            " event_phase_ms=%" PRIu64 " request_phase_ms=%" PRIu64
            " req_rtt_avg_us=%.2f req_rtt_max_us=%.2f"
            " event_queue_full_retries=%" PRIu64
            " request_queue_full_retries=%" PRIu64
            " throughput_msg_per_s=%.2f\n",
            events_sent,
            state.events_received,
            requests_sent,
            replies_received,
            state.request_hits,
            elapsed_ms,
            event_phase_elapsed_ms,
            request_phase_elapsed_ms,
            request_rtt_avg_us,
            request_rtt_max_us,
            event_queue_full_retries,
            request_queue_full_retries,
            throughput
        );
    }

    if (events_sent != config.events ||
        state.events_received != config.events ||
        requests_sent != config.requests ||
        replies_received != config.requests ||
        state.request_hits != config.requests) {
        free(payload_buffer);
        cd_ipc_socket_transport_close(&transport_sender);
        cd_ipc_socket_transport_close(&transport_replier);
        cd_bus_destroy(bus_sender);
        cd_bus_destroy(bus_replier);
        cd_context_shutdown(context);
        return 1;
    }

    free(payload_buffer);
    cd_ipc_socket_transport_close(&transport_sender);
    cd_ipc_socket_transport_close(&transport_replier);
    cd_bus_destroy(bus_sender);
    cd_bus_destroy(bus_replier);
    cd_context_shutdown(context);
    return 0;
}
