#include "conduit/conduit.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef CONDUIT_BROKER_BIN
#define CONDUIT_BROKER_BIN "conduit_broker"
#endif

#ifndef CONDUIT_LOADGEN_BIN
#define CONDUIT_LOADGEN_BIN "conduit_loadgen"
#endif

enum {
    TEST_BROKER_DIAGNOSTICS_ENDPOINT = 0xFFFFFFFEu,
    TEST_BROKER_DIAGNOSTICS_TOPIC_METRICS = 0x00B00001u,
    TEST_BROKER_DIAGNOSTICS_SCHEMA_METRICS_TEXT = 0x00B00001u
};

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

typedef struct fake_clock {
    uint64_t now_ns;
} fake_clock_t;

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

typedef struct request_replier_state {
    cd_bus_t *bus;
    int request_hits;
    cd_message_id_t last_request_message_id;
    cd_message_id_t last_reply_message_id;
    char last_payload[32];
    const char *reply_payload;
    size_t reply_payload_size;
    uint16_t reply_flags;
} request_replier_state_t;

typedef struct broker_metrics_snapshot {
    uint64_t published;
    uint64_t delivered;
    uint64_t dropped;
    uint64_t timeouts;
    uint64_t transport_errors;
} broker_metrics_snapshot_t;

typedef struct loadgen_summary {
    uint64_t events_sent;
    uint64_t events_received;
    uint64_t requests_sent;
    uint64_t replies_received;
    uint64_t request_hits;
    uint64_t elapsed_ms;
    uint64_t event_phase_ms;
    uint64_t request_phase_ms;
    uint64_t event_queue_full_retries;
    uint64_t request_queue_full_retries;
    double req_rtt_avg_us;
    double req_rtt_max_us;
    double throughput_msg_per_s;
} loadgen_summary_t;

typedef struct trace_capture_state {
    int total;
    int enqueue_count;
    int dispatch_count;
    int reply_capture_count;
    int transport_send_count;
    int transport_poll_count;
} trace_capture_state_t;

typedef struct publish_thread_args {
    cd_bus_t *bus;
    cd_topic_t topic;
    cd_endpoint_id_t source_endpoint;
    int messages;
    int failures;
} publish_thread_args_t;

typedef struct request_echo_replier_state {
    cd_bus_t *bus;
    int request_hits;
} request_echo_replier_state_t;

typedef struct request_thread_args {
    cd_bus_t *request_bus;
    int thread_index;
    int request_count;
    int failures;
} request_thread_args_t;

typedef struct pump_thread_args {
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    atomic_int stop;
    int failures;
} pump_thread_args_t;

static void close_fd_if_open(int *fd)
{
    if (fd != NULL && *fd >= 0) {
        close(*fd);
        *fd = -1;
    }
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

static int wait_for_broker_ready(
    const char *socket_path,
    int max_attempts,
    useconds_t sleep_us
)
{
    int fd;

    fd = connect_unix_socket_retry(socket_path, max_attempts, sleep_us);
    if (fd < 0) {
        return 1;
    }
    close(fd);
    return 0;
}

static int unix_path_socket_bind_supported(void)
{
    int fd;
    int bind_result;
    struct sockaddr_un address;
    char socket_path[104];

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }

    memset(&address, 0, sizeof(address));
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
    address.sun_len = (uint8_t)sizeof(address);
#endif
    address.sun_family = AF_UNIX;
    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-bind-check-%ld.sock",
        (long)getpid()
    );
    (void)strncpy(address.sun_path, socket_path, sizeof(address.sun_path) - 1u);
    unlink(socket_path);

    bind_result = bind(fd, (const struct sockaddr *)&address, sizeof(address));
    close(fd);
    unlink(socket_path);

    return bind_result == 0 ? 1 : 0;
}

static int write_text_file(const char *path, const char *contents)
{
    FILE *file;
    size_t expected_size;
    size_t written_size;

    if (path == NULL || contents == NULL) {
        return 1;
    }

    file = fopen(path, "w");
    if (file == NULL) {
        return 1;
    }

    expected_size = strlen(contents);
    written_size = fwrite(contents, 1u, expected_size, file);
    if (written_size != expected_size) {
        fclose(file);
        return 1;
    }
    if (fclose(file) != 0) {
        return 1;
    }

    return 0;
}

static void dump_log_file_to_stderr(const char *label, const char *path)
{
    FILE *file;
    char line[512];
    int line_count;

    if (label == NULL || path == NULL) {
        return;
    }

    fprintf(stderr, "---- %s (%s) ----\n", label, path);
    file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "(unavailable)\n");
        return;
    }

    line_count = 0;
    while (fgets(line, sizeof(line), file) != NULL) {
        fputs(line, stderr);
        line_count += 1;
        if (line_count >= 200) {
            fprintf(stderr, "... (truncated)\n");
            break;
        }
    }
    fclose(file);
}

static pid_t spawn_broker_process(
    const char *socket_path,
    const char *routes_path,
    const char *run_ms,
    const char *metrics_interval_ms,
    const char *log_path
)
{
    pid_t broker_pid;

    if (socket_path == NULL || run_ms == NULL || metrics_interval_ms == NULL) {
        return -1;
    }

    broker_pid = fork();
    if (broker_pid < 0) {
        return -1;
    }
    if (broker_pid == 0) {
        if (log_path != NULL && log_path[0] != '\0') {
            int log_fd;

            log_fd = open(log_path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
            if (log_fd < 0) {
                _exit(126);
            }
            if (dup2(log_fd, STDOUT_FILENO) < 0 || dup2(log_fd, STDERR_FILENO) < 0) {
                close(log_fd);
                _exit(126);
            }
            close(log_fd);
        }

        if (routes_path != NULL && routes_path[0] != '\0') {
            execl(
                CONDUIT_BROKER_BIN,
                CONDUIT_BROKER_BIN,
                "--socket",
                socket_path,
                "--routes-file",
                routes_path,
                "--run-ms",
                run_ms,
                "--metrics-interval-ms",
                metrics_interval_ms,
                (char *)NULL
            );
        } else {
            execl(
                CONDUIT_BROKER_BIN,
                CONDUIT_BROKER_BIN,
                "--socket",
                socket_path,
                "--run-ms",
                run_ms,
                "--metrics-interval-ms",
                metrics_interval_ms,
                (char *)NULL
            );
        }
        _exit(127);
    }

    return broker_pid;
}

static pid_t spawn_loadgen_process(
    const char *socket_path,
    const char *events,
    const char *requests,
    const char *payload_size,
    const char *max_duration_ms,
    const char *request_timeout_ns,
    const char *max_queued_messages,
    const char *max_inflight_requests,
    const char *log_path
)
{
    pid_t loadgen_pid;

    if (socket_path == NULL || events == NULL || requests == NULL ||
        payload_size == NULL || max_duration_ms == NULL ||
        request_timeout_ns == NULL) {
        return -1;
    }

    loadgen_pid = fork();
    if (loadgen_pid < 0) {
        return -1;
    }
    if (loadgen_pid == 0) {
        if (log_path != NULL && log_path[0] != '\0') {
            int log_fd;

            log_fd = open(log_path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
            if (log_fd < 0) {
                _exit(126);
            }
            if (dup2(log_fd, STDOUT_FILENO) < 0 || dup2(log_fd, STDERR_FILENO) < 0) {
                close(log_fd);
                _exit(126);
            }
            close(log_fd);
        }

        {
            const char *argv_buffer[24];
            int argc;

            argc = 0;
            argv_buffer[argc++] = CONDUIT_LOADGEN_BIN;
            argv_buffer[argc++] = "--socket";
            argv_buffer[argc++] = socket_path;
            argv_buffer[argc++] = "--events";
            argv_buffer[argc++] = events;
            argv_buffer[argc++] = "--requests";
            argv_buffer[argc++] = requests;
            argv_buffer[argc++] = "--payload-size";
            argv_buffer[argc++] = payload_size;
            argv_buffer[argc++] = "--max-duration-ms";
            argv_buffer[argc++] = max_duration_ms;
            argv_buffer[argc++] = "--request-timeout-ns";
            argv_buffer[argc++] = request_timeout_ns;
            if (max_queued_messages != NULL && max_queued_messages[0] != '\0') {
                argv_buffer[argc++] = "--max-queued-messages";
                argv_buffer[argc++] = max_queued_messages;
            }
            if (max_inflight_requests != NULL && max_inflight_requests[0] != '\0') {
                argv_buffer[argc++] = "--max-inflight-requests";
                argv_buffer[argc++] = max_inflight_requests;
            }
            argv_buffer[argc++] = "--connect-attempts";
            argv_buffer[argc++] = "12000";
            argv_buffer[argc++] = NULL;
            execv(CONDUIT_LOADGEN_BIN, (char *const *)argv_buffer);
        }
        _exit(127);
    }

    return loadgen_pid;
}

static int stop_broker_process(pid_t broker_pid, int *out_wait_status)
{
    int status;

    if (broker_pid <= 0) {
        return 1;
    }

    if (kill(broker_pid, SIGTERM) != 0 && errno != ESRCH) {
        return 1;
    }
    if (waitpid(broker_pid, &status, 0) != broker_pid) {
        return 1;
    }

    if (out_wait_status != NULL) {
        *out_wait_status = status;
    }
    return 0;
}

static int parse_broker_final_metrics(
    const char *log_path,
    broker_metrics_snapshot_t *out_metrics
)
{
    FILE *file;
    char line[512];
    int found;

    if (log_path == NULL || out_metrics == NULL) {
        return 1;
    }

    memset(out_metrics, 0, sizeof(*out_metrics));
    file = fopen(log_path, "r");
    if (file == NULL) {
        return 1;
    }

    found = 0;
    while (fgets(line, sizeof(line), file) != NULL) {
        unsigned long long published;
        unsigned long long delivered;
        unsigned long long dropped;
        unsigned long long timeouts;
        unsigned long long transport_errors;
        size_t clients;

        if (strncmp(line, "[broker] final ", strlen("[broker] final ")) != 0) {
            continue;
        }

        if (sscanf(
                line,
                "[broker] final clients=%zu published=%llu delivered=%llu dropped=%llu "
                "timeouts=%llu transport_errors=%llu",
                &clients,
                &published,
                &delivered,
                &dropped,
                &timeouts,
                &transport_errors
            ) == 6) {
            (void)clients;
            out_metrics->published = (uint64_t)published;
            out_metrics->delivered = (uint64_t)delivered;
            out_metrics->dropped = (uint64_t)dropped;
            out_metrics->timeouts = (uint64_t)timeouts;
            out_metrics->transport_errors = (uint64_t)transport_errors;
            found = 1;
        }
    }

    fclose(file);
    return found ? 0 : 1;
}

static int parse_line_u64_field(const char *line, const char *field_prefix, uint64_t *out_value)
{
    const char *start;
    unsigned long long value;
    char *end_ptr;

    if (line == NULL || field_prefix == NULL || out_value == NULL) {
        return 1;
    }

    start = strstr(line, field_prefix);
    if (start == NULL) {
        return 1;
    }
    start += strlen(field_prefix);

    errno = 0;
    end_ptr = NULL;
    value = strtoull(start, &end_ptr, 10);
    if (errno != 0 || end_ptr == start) {
        return 1;
    }

    *out_value = (uint64_t)value;
    return 0;
}

static int parse_line_double_field(const char *line, const char *field_prefix, double *out_value)
{
    const char *start;
    char *end_ptr;
    double value;

    if (line == NULL || field_prefix == NULL || out_value == NULL) {
        return 1;
    }

    start = strstr(line, field_prefix);
    if (start == NULL) {
        return 1;
    }
    start += strlen(field_prefix);

    errno = 0;
    end_ptr = NULL;
    value = strtod(start, &end_ptr);
    if (errno != 0 || end_ptr == start) {
        return 1;
    }

    *out_value = value;
    return 0;
}

static int parse_loadgen_done_summary(const char *log_path, loadgen_summary_t *out_summary)
{
    FILE *file;
    char line[1024];
    int found;

    if (log_path == NULL || out_summary == NULL) {
        return 1;
    }

    memset(out_summary, 0, sizeof(*out_summary));
    file = fopen(log_path, "r");
    if (file == NULL) {
        return 1;
    }

    found = 0;
    while (fgets(line, sizeof(line), file) != NULL) {
        if (strncmp(line, "[loadgen] done ", strlen("[loadgen] done ")) != 0) {
            continue;
        }

        if (parse_line_u64_field(line, "events_sent=", &out_summary->events_sent) != 0 ||
            parse_line_u64_field(line, "events_received=", &out_summary->events_received) != 0 ||
            parse_line_u64_field(line, "requests_sent=", &out_summary->requests_sent) != 0 ||
            parse_line_u64_field(line, "replies_received=", &out_summary->replies_received) != 0 ||
            parse_line_u64_field(line, "request_hits=", &out_summary->request_hits) != 0 ||
            parse_line_u64_field(line, "elapsed_ms=", &out_summary->elapsed_ms) != 0 ||
            parse_line_u64_field(line, "event_phase_ms=", &out_summary->event_phase_ms) != 0 ||
            parse_line_u64_field(line, "request_phase_ms=", &out_summary->request_phase_ms) != 0 ||
            parse_line_u64_field(
                line,
                "event_queue_full_retries=",
                &out_summary->event_queue_full_retries
            ) != 0 ||
            parse_line_u64_field(
                line,
                "request_queue_full_retries=",
                &out_summary->request_queue_full_retries
            ) != 0 ||
            parse_line_double_field(line, "req_rtt_avg_us=", &out_summary->req_rtt_avg_us) != 0 ||
            parse_line_double_field(line, "req_rtt_max_us=", &out_summary->req_rtt_max_us) != 0 ||
            parse_line_double_field(
                line,
                "throughput_msg_per_s=",
                &out_summary->throughput_msg_per_s
            ) != 0) {
            fclose(file);
            return 1;
        }

        found = 1;
    }

    fclose(file);
    return found ? 0 : 1;
}

static int parse_metrics_snapshot_payload(
    const void *payload,
    size_t payload_size,
    size_t *out_clients,
    broker_metrics_snapshot_t *out_metrics
)
{
    char text[256];
    unsigned long long published;
    unsigned long long delivered;
    unsigned long long dropped;
    unsigned long long timeouts;
    unsigned long long transport_errors;
    size_t clients;

    if (payload == NULL || out_clients == NULL || out_metrics == NULL ||
        payload_size == 0u || payload_size >= sizeof(text)) {
        return 1;
    }

    memset(text, 0, sizeof(text));
    memcpy(text, payload, payload_size);
    if (sscanf(
            text,
            "clients=%zu published=%llu delivered=%llu dropped=%llu "
            "timeouts=%llu transport_errors=%llu",
            &clients,
            &published,
            &delivered,
            &dropped,
            &timeouts,
            &transport_errors
        ) != 6) {
        return 1;
    }

    *out_clients = clients;
    out_metrics->published = (uint64_t)published;
    out_metrics->delivered = (uint64_t)delivered;
    out_metrics->dropped = (uint64_t)dropped;
    out_metrics->timeouts = (uint64_t)timeouts;
    out_metrics->transport_errors = (uint64_t)transport_errors;
    return 0;
}

static void test_free_adapter(void *user_data, void *ptr)
{
    (void)user_data;
    free(ptr);
}

static uint64_t test_fake_now_ns(void *user_data)
{
    fake_clock_t *clock;

    clock = (fake_clock_t *)user_data;
    return clock->now_ns;
}

static int setup_runtime_full(
    size_t max_queued_messages,
    size_t max_subscriptions,
    size_t max_inflight_requests,
    const cd_context_config_t *context_config,
    test_runtime_t *runtime
)
{
    cd_bus_config_t bus_config;

    if (runtime == NULL) {
        return 1;
    }

    runtime->context = NULL;
    runtime->bus = NULL;
    ASSERT_STATUS(cd_context_init(&runtime->context, context_config), CD_STATUS_OK);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = max_queued_messages;
    bus_config.max_subscriptions = max_subscriptions;
    bus_config.max_inflight_requests = max_inflight_requests;
    ASSERT_STATUS(cd_bus_create(runtime->context, &bus_config, &runtime->bus), CD_STATUS_OK);

    return 0;
}

static int setup_runtime(size_t max_queued_messages, size_t max_subscriptions, test_runtime_t *runtime)
{
    return setup_runtime_full(max_queued_messages, max_subscriptions, 0u, NULL, runtime);
}

static int setup_runtime_with_clock(
    size_t max_queued_messages,
    size_t max_subscriptions,
    size_t max_inflight_requests,
    fake_clock_t *clock,
    test_runtime_t *runtime
)
{
    cd_context_config_t context_config;

    memset(&context_config, 0, sizeof(context_config));
    context_config.clock.now_ns = test_fake_now_ns;
    context_config.clock.user_data = clock;
    return setup_runtime_full(
        max_queued_messages,
        max_subscriptions,
        max_inflight_requests,
        &context_config,
        runtime
    );
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

static void trace_capture_handler(void *user_data, const cd_trace_event_t *event)
{
    trace_capture_state_t *state;

    state = (trace_capture_state_t *)user_data;
    if (state == NULL || event == NULL) {
        return;
    }

    state->total += 1;
    switch (event->kind) {
    case CD_TRACE_EVENT_ENQUEUE:
        state->enqueue_count += 1;
        break;
    case CD_TRACE_EVENT_DISPATCH:
        state->dispatch_count += 1;
        break;
    case CD_TRACE_EVENT_REPLY_CAPTURE:
        state->reply_capture_count += 1;
        break;
    case CD_TRACE_EVENT_TRANSPORT_SEND:
        state->transport_send_count += 1;
        break;
    case CD_TRACE_EVENT_TRANSPORT_POLL:
        state->transport_poll_count += 1;
        break;
    default:
        break;
    }
}

static void *publish_thread_main(void *arg)
{
    publish_thread_args_t *thread_args;
    int i;

    thread_args = (publish_thread_args_t *)arg;
    if (thread_args == NULL || thread_args->bus == NULL || thread_args->messages <= 0) {
        return NULL;
    }

    for (i = 0; i < thread_args->messages; ++i) {
        cd_publish_params_t publish;
        uint64_t payload_value;
        cd_status_t status;

        memset(&publish, 0, sizeof(publish));
        payload_value = (uint64_t)i;
        publish.source_endpoint = thread_args->source_endpoint;
        publish.topic = thread_args->topic;
        publish.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
        publish.payload = &payload_value;
        publish.payload_size = sizeof(payload_value);

        status = cd_publish(thread_args->bus, &publish, NULL);
        if (status != CD_STATUS_OK) {
            thread_args->failures += 1;
            return NULL;
        }
    }

    return NULL;
}

static cd_status_t request_echo_replier_handler(void *user_data, const cd_envelope_t *message)
{
    request_echo_replier_state_t *state;
    cd_reply_params_t reply_params;
    uint64_t reply_value;

    state = (request_echo_replier_state_t *)user_data;
    if (state == NULL || state->bus == NULL || message == NULL ||
        message->kind != CD_MESSAGE_REQUEST ||
        message->payload == NULL || message->payload_size < sizeof(reply_value)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    memcpy(&reply_value, message->payload, sizeof(reply_value));
    state->request_hits += 1;

    memset(&reply_params, 0, sizeof(reply_params));
    reply_params.source_endpoint = message->target_endpoint;
    reply_params.target_endpoint = message->source_endpoint;
    reply_params.correlation_id = message->message_id;
    reply_params.topic = message->topic;
    reply_params.schema_id = 0x00C00001u;
    reply_params.schema_version = 1u;
    reply_params.flags = 0u;
    reply_params.payload = &reply_value;
    reply_params.payload_size = sizeof(reply_value);
    return cd_send_reply(state->bus, &reply_params, NULL);
}

static void *pump_pair_thread_main(void *arg)
{
    pump_thread_args_t *thread_args;
    int drain_i;

    thread_args = (pump_thread_args_t *)arg;
    if (thread_args == NULL || thread_args->bus_a == NULL || thread_args->bus_b == NULL) {
        return NULL;
    }

    while (atomic_load_explicit(&thread_args->stop, memory_order_acquire) == 0) {
        if (cd_bus_pump(thread_args->bus_b, 0u, NULL) != CD_STATUS_OK ||
            cd_bus_pump(thread_args->bus_a, 0u, NULL) != CD_STATUS_OK) {
            thread_args->failures += 1;
            return NULL;
        }
        usleep(100u);
    }

    for (drain_i = 0; drain_i < 200; ++drain_i) {
        if (cd_bus_pump(thread_args->bus_b, 0u, NULL) != CD_STATUS_OK ||
            cd_bus_pump(thread_args->bus_a, 0u, NULL) != CD_STATUS_OK) {
            thread_args->failures += 1;
            return NULL;
        }
        usleep(50u);
    }

    return NULL;
}

static void *request_thread_main(void *arg)
{
    request_thread_args_t *thread_args;
    int i;

    thread_args = (request_thread_args_t *)arg;
    if (thread_args == NULL || thread_args->request_bus == NULL || thread_args->request_count <= 0) {
        return NULL;
    }

    for (i = 0; i < thread_args->request_count; ++i) {
        cd_request_params_t request;
        cd_request_token_t token;
        uint64_t payload_value;
        int send_attempts;

        payload_value = ((uint64_t)(unsigned)thread_args->thread_index << 32) | (uint64_t)(unsigned)i;

        memset(&request, 0, sizeof(request));
        request.source_endpoint = (cd_endpoint_id_t)(700u + (unsigned)thread_args->thread_index);
        request.target_endpoint = 901u;
        request.topic = 0x00C10001u;
        request.schema_id = 0x00C10001u;
        request.schema_version = 1u;
        request.flags = 0u;
        request.timeout_ns = 30000000000ull;
        request.payload = &payload_value;
        request.payload_size = sizeof(payload_value);

        token = 0u;
        send_attempts = 0;
        while (true) {
            cd_status_t send_status;

            send_status = cd_request_async(thread_args->request_bus, &request, &token);
            if (send_status == CD_STATUS_OK && token != 0u) {
                break;
            }
            if (send_status == CD_STATUS_QUEUE_FULL || send_status == CD_STATUS_CAPACITY_REACHED) {
                send_attempts += 1;
                if (send_attempts > 20000) {
                    thread_args->failures += 1;
                    return NULL;
                }
                usleep(200u);
                continue;
            }
            thread_args->failures += 1;
            return NULL;
        }

        while (true) {
            cd_reply_t reply;
            int ready;
            cd_status_t poll_status;
            uint64_t reply_value;

            memset(&reply, 0, sizeof(reply));
            ready = 0;
            poll_status = cd_poll_reply(thread_args->request_bus, token, &reply, &ready);
            if (poll_status == CD_STATUS_OK && ready == 0) {
                usleep(5000u);
                continue;
            }
            if (poll_status == CD_STATUS_OK && ready == 1) {
                if (reply.payload == NULL || reply.payload_size < sizeof(reply_value)) {
                    thread_args->failures += 1;
                    return NULL;
                }
                memcpy(&reply_value, reply.payload, sizeof(reply_value));
                cd_reply_dispose(thread_args->request_bus, &reply);
                if (reply_value != payload_value) {
                    thread_args->failures += 1;
                    return NULL;
                }
                poll_status = cd_poll_reply(thread_args->request_bus, token, NULL, NULL);
                if (poll_status != CD_STATUS_NOT_FOUND) {
                    thread_args->failures += 1;
                    return NULL;
                }
                break;
            }

            thread_args->failures += 1;
            return NULL;
        }
    }

    return NULL;
}

static cd_status_t request_replier_handler(void *user_data, const cd_envelope_t *message)
{
    request_replier_state_t *state;
    cd_reply_params_t reply_params;
    cd_message_id_t reply_message_id;
    cd_status_t send_status;
    size_t payload_len;

    state = (request_replier_state_t *)user_data;
    if (state == NULL || message == NULL || message->kind != CD_MESSAGE_REQUEST || state->bus == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    state->request_hits += 1;
    state->last_request_message_id = message->message_id;
    memset(state->last_payload, 0, sizeof(state->last_payload));
    payload_len = message->payload_size;
    if (payload_len > sizeof(state->last_payload) - 1u) {
        payload_len = sizeof(state->last_payload) - 1u;
    }
    if (payload_len > 0u && message->payload != NULL) {
        memcpy(state->last_payload, message->payload, payload_len);
    }

    memset(&reply_params, 0, sizeof(reply_params));
    reply_params.source_endpoint = message->target_endpoint;
    reply_params.target_endpoint = message->source_endpoint;
    reply_params.correlation_id = message->message_id;
    reply_params.topic = message->topic;
    reply_params.schema_id = 9001u;
    reply_params.schema_version = 1u;
    reply_params.flags = state->reply_flags;
    reply_params.payload = state->reply_payload;
    reply_params.payload_size = state->reply_payload_size;

    reply_message_id = 0u;
    send_status = cd_send_reply(state->bus, &reply_params, &reply_message_id);
    if (send_status != CD_STATUS_OK) {
        return send_status;
    }
    state->last_reply_message_id = reply_message_id;

    return CD_STATUS_OK;
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

static int test_trace_hook_reports_core_events(void)
{
    test_runtime_t runtime;
    trace_capture_state_t trace_state;
    cd_subscription_desc_t event_sub;
    cd_subscription_desc_t request_sub;
    handler_state_t event_handler;
    capture_state_t capture;
    request_replier_state_t replier_state;
    cd_publish_params_t publish;
    cd_request_params_t request;
    cd_request_token_t token;
    cd_reply_t reply;
    int ready;
    size_t processed;
    int total_before_disable;

    ASSERT_TRUE(setup_runtime_full(16u, 16u, 8u, NULL, &runtime) == 0);
    memset(&trace_state, 0, sizeof(trace_state));
    cd_bus_set_trace_hook(runtime.bus, trace_capture_handler, &trace_state);

    memset(&capture, 0, sizeof(capture));
    memset(&event_handler, 0, sizeof(event_handler));
    event_handler.capture = &capture;
    event_handler.marker = 1;
    event_handler.return_status = CD_STATUS_OK;

    memset(&event_sub, 0, sizeof(event_sub));
    event_sub.endpoint = 77u;
    event_sub.topic = 7001u;
    event_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    event_sub.handler = generic_handler;
    event_sub.user_data = &event_handler;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &event_sub, NULL), CD_STATUS_OK);

    memset(&replier_state, 0, sizeof(replier_state));
    replier_state.bus = runtime.bus;
    replier_state.reply_payload = "trace-reply";
    replier_state.reply_payload_size = strlen("trace-reply");

    memset(&request_sub, 0, sizeof(request_sub));
    request_sub.endpoint = 88u;
    request_sub.topic = 7002u;
    request_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    request_sub.handler = request_replier_handler;
    request_sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &request_sub, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 7u;
    publish.topic = 7001u;
    publish.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    publish.payload = "trace-event";
    publish.payload_size = strlen("trace-event");
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, NULL), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 9u;
    request.target_endpoint = 88u;
    request.topic = 7002u;
    request.schema_id = 1u;
    request.schema_version = 1u;
    request.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    request.timeout_ns = 2000000000ull;
    request.payload = "trace-request";
    request.payload_size = strlen("trace-request");
    token = 0u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token), CD_STATUS_OK);
    ASSERT_TRUE(token != 0u);

    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed >= 1u);
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);

    ready = 0;
    memset(&reply, 0, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, &reply, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    cd_reply_dispose(runtime.bus, &reply);

    ASSERT_TRUE(trace_state.total > 0);
    ASSERT_TRUE(trace_state.enqueue_count >= 3);
    ASSERT_TRUE(trace_state.dispatch_count >= 3);
    ASSERT_TRUE(trace_state.reply_capture_count >= 1);
    ASSERT_TRUE(trace_state.transport_send_count == 0);
    ASSERT_TRUE(trace_state.transport_poll_count == 0);

    total_before_disable = trace_state.total;
    cd_bus_set_trace_hook(runtime.bus, NULL, NULL);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 7u;
    publish.topic = 7001u;
    publish.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    publish.payload = "trace-off";
    publish.payload_size = strlen("trace-off");
    ASSERT_STATUS(cd_publish(runtime.bus, &publish, NULL), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(trace_state.total == total_before_disable);

    teardown_runtime(&runtime);
    return 0;
}

static int test_threadsafe_multi_producer_publish(void)
{
    enum {
        THREAD_COUNT = 4,
        MESSAGES_PER_THREAD = 1500
    };

    test_runtime_t runtime;
    capture_state_t capture;
    handler_state_t handler;
    cd_subscription_desc_t sub;
    pthread_t threads[THREAD_COUNT];
    publish_thread_args_t thread_args[THREAD_COUNT];
    int i;
    size_t processed;

    ASSERT_TRUE(setup_runtime(32768u, 8u, &runtime) == 0);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 1;
    handler.return_status = CD_STATUS_OK;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 222u;
    sub.topic = 9100u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = generic_handler;
    sub.user_data = &handler;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_OK);

    memset(threads, 0, sizeof(threads));
    memset(thread_args, 0, sizeof(thread_args));
    for (i = 0; i < THREAD_COUNT; ++i) {
        thread_args[i].bus = runtime.bus;
        thread_args[i].topic = 9100u;
        thread_args[i].source_endpoint = (cd_endpoint_id_t)(300u + (unsigned)i);
        thread_args[i].messages = MESSAGES_PER_THREAD;
        thread_args[i].failures = 0;
        ASSERT_TRUE(pthread_create(&threads[i], NULL, publish_thread_main, &thread_args[i]) == 0);
    }

    for (i = 0; i < THREAD_COUNT; ++i) {
        ASSERT_TRUE(pthread_join(threads[i], NULL) == 0);
        ASSERT_TRUE(thread_args[i].failures == 0);
    }

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == (size_t)(THREAD_COUNT * MESSAGES_PER_THREAD));
    ASSERT_TRUE(capture.count == (THREAD_COUNT * MESSAGES_PER_THREAD));

    teardown_runtime(&runtime);
    return 0;
}

static int test_threadsafe_concurrent_request_reply_ownership(void)
{
    enum {
        REQUEST_THREAD_COUNT = 4,
        REQUESTS_PER_THREAD = 200
    };

    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_inproc_hub_t *hub;
    cd_inproc_hub_config_t hub_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t request_sub;
    request_echo_replier_state_t replier_state;
    pump_thread_args_t pump_args;
    pthread_t pump_thread;
    request_thread_args_t request_args[REQUEST_THREAD_COUNT];
    pthread_t request_threads[REQUEST_THREAD_COUNT];
    int i;

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    hub = NULL;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));
    memset(&replier_state, 0, sizeof(replier_state));
    memset(&pump_args, 0, sizeof(pump_args));
    memset(request_args, 0, sizeof(request_args));
    memset(request_threads, 0, sizeof(request_threads));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 8192u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 4096u;
    bus_config.max_transports = 2u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    memset(&hub_config, 0, sizeof(hub_config));
    hub_config.max_peers = 2u;
    hub_config.max_queued_messages_per_peer = 8192u;
    ASSERT_STATUS(cd_inproc_hub_init(context, &hub_config, &hub), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_b), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    replier_state.bus = bus_b;
    memset(&request_sub, 0, sizeof(request_sub));
    request_sub.endpoint = 901u;
    request_sub.topic = 0x00C10001u;
    request_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    request_sub.handler = request_echo_replier_handler;
    request_sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus_b, &request_sub, NULL), CD_STATUS_OK);

    pump_args.bus_a = bus_a;
    pump_args.bus_b = bus_b;
    atomic_init(&pump_args.stop, 0);
    pump_args.failures = 0;
    ASSERT_TRUE(pthread_create(&pump_thread, NULL, pump_pair_thread_main, &pump_args) == 0);

    for (i = 0; i < REQUEST_THREAD_COUNT; ++i) {
        request_args[i].request_bus = bus_a;
        request_args[i].thread_index = i;
        request_args[i].request_count = REQUESTS_PER_THREAD;
        request_args[i].failures = 0;
        ASSERT_TRUE(pthread_create(&request_threads[i], NULL, request_thread_main, &request_args[i]) == 0);
    }

    for (i = 0; i < REQUEST_THREAD_COUNT; ++i) {
        ASSERT_TRUE(pthread_join(request_threads[i], NULL) == 0);
        ASSERT_TRUE(request_args[i].failures == 0);
    }

    atomic_store_explicit(&pump_args.stop, 1, memory_order_release);
    ASSERT_TRUE(pthread_join(pump_thread, NULL) == 0);
    ASSERT_TRUE(pump_args.failures == 0);

    ASSERT_TRUE(replier_state.request_hits == (REQUEST_THREAD_COUNT * REQUESTS_PER_THREAD));

    cd_inproc_transport_close(&transport_a);
    cd_inproc_transport_close(&transport_b);
    cd_inproc_hub_shutdown(hub);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);
    return 0;
}

static int test_request_reply_roundtrip_and_dispose(void)
{
    test_runtime_t runtime;
    cd_subscription_desc_t sub;
    request_replier_state_t replier_state;
    cd_request_params_t request;
    cd_request_token_t token;
    size_t processed;
    int ready;
    cd_reply_t reply;

    ASSERT_TRUE(setup_runtime_full(8u, 8u, 8u, NULL, &runtime) == 0);

    memset(&replier_state, 0, sizeof(replier_state));
    replier_state.bus = runtime.bus;
    replier_state.reply_payload = "reply-ok";
    replier_state.reply_payload_size = strlen("reply-ok");
    replier_state.reply_flags = CD_MESSAGE_FLAG_LOCAL_ONLY;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 50u;
    sub.topic = 120u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    sub.handler = request_replier_handler;
    sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_OK);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 1u;
    request.target_endpoint = 50u;
    request.topic = 120u;
    request.schema_id = 100u;
    request.schema_version = 1u;
    request.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    request.timeout_ns = 1000000000ull;
    request.payload = "ask";
    request.payload_size = 3u;

    token = 0u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token), CD_STATUS_OK);
    ASSERT_TRUE(token != 0u);

    ready = -1;
    memset(&reply, 0, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, &reply, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 0);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(replier_state.request_hits == 1);
    ASSERT_TRUE(strcmp(replier_state.last_payload, "ask") == 0);

    ready = -1;
    memset(&reply, 0, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, &reply, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 0);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);

    ready = -1;
    memset(&reply, 0, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, &reply, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.message_id == replier_state.last_reply_message_id);
    ASSERT_TRUE(reply.payload != NULL);
    ASSERT_TRUE(reply.payload_size == replier_state.reply_payload_size);
    ASSERT_TRUE(strncmp((const char *)reply.payload, "reply-ok", reply.payload_size) == 0);

    cd_reply_dispose(runtime.bus, &reply);
    ASSERT_TRUE(reply.payload == NULL);
    ASSERT_TRUE(reply.payload_size == 0u);

    ready = -1;
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, NULL, &ready), CD_STATUS_NOT_FOUND);

    teardown_runtime(&runtime);
    return 0;
}

static int test_request_timeout_with_fake_clock(void)
{
    test_runtime_t runtime;
    fake_clock_t clock;
    cd_request_params_t request;
    cd_subscription_desc_t sub;
    capture_state_t capture;
    handler_state_t handler;
    cd_request_token_t token;
    size_t processed;
    int ready;
    cd_reply_t reply;

    memset(&clock, 0, sizeof(clock));
    clock.now_ns = 1000u;
    ASSERT_TRUE(setup_runtime_with_clock(8u, 8u, 8u, &clock, &runtime) == 0);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 1u;
    request.target_endpoint = 77u;
    request.topic = 333u;
    request.timeout_ns = 50u;
    request.payload = "timeout";
    request.payload_size = 7u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token), CD_STATUS_OK);

    ready = -1;
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, NULL, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 0);

    clock.now_ns = 1049u;
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, NULL, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 0);

    clock.now_ns = 1050u;
    memset(&reply, 0xAB, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, &reply, &ready), CD_STATUS_TIMEOUT);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.message_id == 0u);
    ASSERT_TRUE(reply.payload == NULL);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 9;
    handler.return_status = CD_STATUS_OK;
    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 77u;
    sub.topic = 333u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    sub.handler = generic_handler;
    sub.user_data = &handler;
    ASSERT_STATUS(cd_subscribe(runtime.bus, &sub, NULL), CD_STATUS_OK);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 0);

    ASSERT_STATUS(cd_poll_reply(runtime.bus, token, NULL, &ready), CD_STATUS_NOT_FOUND);

    teardown_runtime(&runtime);
    return 0;
}

static int test_request_inflight_capacity_and_validation(void)
{
    test_runtime_t runtime;
    cd_request_params_t request;
    cd_request_token_t token_1;
    cd_request_token_t token_2;

    ASSERT_TRUE(setup_runtime_full(8u, 8u, 1u, NULL, &runtime) == 0);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 1u;
    request.target_endpoint = 2u;
    request.topic = 7u;
    request.timeout_ns = 1000u;

    token_1 = 0u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token_1), CD_STATUS_OK);
    ASSERT_TRUE(token_1 != 0u);

    token_2 = 0u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token_2), CD_STATUS_CAPACITY_REACHED);
    ASSERT_TRUE(token_2 == 0u);

    request.target_endpoint = CD_ENDPOINT_NONE;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token_2), CD_STATUS_INVALID_ARGUMENT);

    request.target_endpoint = 2u;
    request.timeout_ns = 0u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token_2), CD_STATUS_INVALID_ARGUMENT);

    request.timeout_ns = 1000u;
    request.payload = NULL;
    request.payload_size = 1u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token_2), CD_STATUS_INVALID_ARGUMENT);

    ASSERT_STATUS(cd_request_async(runtime.bus, &request, NULL), CD_STATUS_INVALID_ARGUMENT);

    teardown_runtime(&runtime);
    return 0;
}

static int test_queue_and_argument_edges(void)
{
    test_runtime_t runtime;
    cd_publish_params_t publish;
    cd_command_params_t command;
    cd_request_params_t request;
    cd_reply_params_t reply_params;
    cd_reply_t reply;
    cd_request_token_t token;
    cd_message_id_t invalid_message_id;
    size_t processed;

    ASSERT_TRUE(setup_runtime_full(1u, 4u, 2u, NULL, &runtime) == 0);

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

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 1u;
    request.target_endpoint = 2u;
    request.topic = 8u;
    request.timeout_ns = 1000u;
    token = 88u;
    ASSERT_STATUS(cd_request_async(runtime.bus, &request, &token), CD_STATUS_QUEUE_FULL);
    ASSERT_TRUE(token == 0u);

    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, NULL), CD_STATUS_OK);

    memset(&command, 0, sizeof(command));
    command.source_endpoint = 1u;
    command.target_endpoint = CD_ENDPOINT_NONE;
    command.topic = 4u;
    invalid_message_id = 234u;
    ASSERT_STATUS(cd_send_command(runtime.bus, &command, &invalid_message_id), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(invalid_message_id == 0u);

    memset(&reply_params, 0, sizeof(reply_params));
    reply_params.source_endpoint = 2u;
    reply_params.target_endpoint = CD_ENDPOINT_NONE;
    reply_params.correlation_id = 1u;
    ASSERT_STATUS(cd_send_reply(runtime.bus, &reply_params, NULL), CD_STATUS_INVALID_ARGUMENT);

    reply_params.target_endpoint = 1u;
    reply_params.correlation_id = 0u;
    ASSERT_STATUS(cd_send_reply(runtime.bus, &reply_params, NULL), CD_STATUS_INVALID_ARGUMENT);

    ASSERT_STATUS(cd_bus_pump(NULL, 0u, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_publish(NULL, &publish, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_send_command(NULL, &command, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_send_reply(NULL, &reply_params, NULL), CD_STATUS_INVALID_ARGUMENT);

    token = 99u;
    ASSERT_STATUS(cd_request_async(NULL, &request, &token), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(token == 0u);
    ASSERT_STATUS(cd_request_async(runtime.bus, NULL, &token), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(token == 0u);

    memset(&reply, 0xAB, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(NULL, 1u, &reply, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(reply.message_id == 0u);
    ASSERT_STATUS(cd_poll_reply(runtime.bus, 0u, &reply, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_TRUE(reply.message_id == 0u);
    ASSERT_STATUS(cd_poll_reply(runtime.bus, 123456u, &reply, NULL), CD_STATUS_NOT_FOUND);
    ASSERT_TRUE(reply.message_id == 0u);

    processed = 123u;
    ASSERT_STATUS(cd_bus_pump(runtime.bus, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 0u);

    cd_reply_dispose(NULL, &reply);
    cd_reply_dispose(runtime.bus, NULL);

    teardown_runtime(&runtime);
    return 0;
}

static int test_ipc_codec_roundtrip(void)
{
    cd_ipc_codec_config_t codec_config;
    cd_envelope_t message;
    cd_envelope_t decoded;
    uint8_t frame[256];
    size_t frame_size;
    size_t expected_size;
    const char payload[] = "frame-hello";

    memset(&codec_config, 0, sizeof(codec_config));
    codec_config.max_payload_size = 128u;

    expected_size = 0u;
    ASSERT_STATUS(
        cd_ipc_frame_size_for_payload(strlen(payload), &codec_config, &expected_size),
        CD_STATUS_OK
    );
    ASSERT_TRUE(expected_size == CD_IPC_FRAME_HEADER_SIZE + strlen(payload));

    memset(&message, 0, sizeof(message));
    message.message_id = 88u;
    message.correlation_id = 77u;
    message.kind = CD_MESSAGE_REQUEST;
    message.topic = 301u;
    message.source_endpoint = 12u;
    message.target_endpoint = 99u;
    message.schema_id = 111u;
    message.schema_version = 2u;
    message.flags = CD_MESSAGE_FLAG_HIGH_PRIORITY;
    message.timestamp_ns = 123456u;
    message.payload = payload;
    message.payload_size = strlen(payload);

    frame_size = 0u;
    ASSERT_STATUS(
        cd_ipc_encode_envelope(&message, &codec_config, frame, sizeof(frame), &frame_size),
        CD_STATUS_OK
    );
    ASSERT_TRUE(frame_size == expected_size);

    memset(&decoded, 0, sizeof(decoded));
    ASSERT_STATUS(cd_ipc_decode_envelope(frame, frame_size, &codec_config, &decoded), CD_STATUS_OK);
    ASSERT_TRUE(decoded.message_id == message.message_id);
    ASSERT_TRUE(decoded.correlation_id == message.correlation_id);
    ASSERT_TRUE(decoded.kind == message.kind);
    ASSERT_TRUE(decoded.topic == message.topic);
    ASSERT_TRUE(decoded.source_endpoint == message.source_endpoint);
    ASSERT_TRUE(decoded.target_endpoint == message.target_endpoint);
    ASSERT_TRUE(decoded.schema_id == message.schema_id);
    ASSERT_TRUE(decoded.schema_version == message.schema_version);
    ASSERT_TRUE(decoded.flags == message.flags);
    ASSERT_TRUE(decoded.timestamp_ns == message.timestamp_ns);
    ASSERT_TRUE(decoded.payload_size == message.payload_size);
    ASSERT_TRUE(decoded.payload != NULL);
    ASSERT_TRUE(memcmp(decoded.payload, payload, message.payload_size) == 0);

    return 0;
}

static int test_ipc_codec_validation_guards(void)
{
    cd_ipc_codec_config_t codec_config;
    cd_ipc_codec_config_t tiny_codec_config;
    cd_envelope_t message;
    cd_envelope_t decoded;
    uint8_t frame[128];
    size_t frame_size;
    size_t computed_size;
    const char payload[] = "abcd";

    enum {
        FRAME_OFFSET_VERSION = 4u,
        FRAME_OFFSET_FLAGS = 46u,
        FRAME_OFFSET_PAYLOAD_SIZE = 56u
    };

    memset(&codec_config, 0, sizeof(codec_config));
    codec_config.max_payload_size = 64u;

    computed_size = 5u;
    ASSERT_STATUS(cd_ipc_frame_size_for_payload(65u, &codec_config, &computed_size), CD_STATUS_CAPACITY_REACHED);
    ASSERT_TRUE(computed_size == 0u);

    memset(&message, 0, sizeof(message));
    message.message_id = 1u;
    message.kind = CD_MESSAGE_EVENT;
    message.topic = 11u;
    message.flags = CD_MESSAGE_FLAG_LOCAL_ONLY;
    message.payload = payload;
    message.payload_size = strlen(payload);

    frame_size = 9u;
    ASSERT_STATUS(
        cd_ipc_encode_envelope(
            &message,
            &codec_config,
            frame,
            CD_IPC_FRAME_HEADER_SIZE + message.payload_size - 1u,
            &frame_size
        ),
        CD_STATUS_CAPACITY_REACHED
    );
    ASSERT_TRUE(frame_size == 0u);

    frame_size = 0u;
    ASSERT_STATUS(
        cd_ipc_encode_envelope(&message, &codec_config, frame, sizeof(frame), &frame_size),
        CD_STATUS_OK
    );

    frame[FRAME_OFFSET_VERSION] ^= 0x01u;
    ASSERT_STATUS(
        cd_ipc_decode_envelope(frame, frame_size, &codec_config, &decoded),
        CD_STATUS_SCHEMA_MISMATCH
    );
    frame[FRAME_OFFSET_VERSION] ^= 0x01u;

    frame[FRAME_OFFSET_FLAGS] = 0x04u;
    frame[FRAME_OFFSET_FLAGS + 1u] = 0x00u;
    ASSERT_STATUS(
        cd_ipc_decode_envelope(frame, frame_size, &codec_config, &decoded),
        CD_STATUS_SCHEMA_MISMATCH
    );
    ASSERT_STATUS(
        cd_ipc_encode_envelope(&message, &codec_config, frame, sizeof(frame), &frame_size),
        CD_STATUS_OK
    );

    frame[FRAME_OFFSET_PAYLOAD_SIZE] = 0x05u;
    frame[FRAME_OFFSET_PAYLOAD_SIZE + 1u] = 0x00u;
    frame[FRAME_OFFSET_PAYLOAD_SIZE + 2u] = 0x00u;
    frame[FRAME_OFFSET_PAYLOAD_SIZE + 3u] = 0x00u;
    frame[FRAME_OFFSET_PAYLOAD_SIZE + 4u] = 0x00u;
    frame[FRAME_OFFSET_PAYLOAD_SIZE + 5u] = 0x00u;
    frame[FRAME_OFFSET_PAYLOAD_SIZE + 6u] = 0x00u;
    frame[FRAME_OFFSET_PAYLOAD_SIZE + 7u] = 0x00u;
    ASSERT_STATUS(
        cd_ipc_decode_envelope(frame, frame_size, &codec_config, &decoded),
        CD_STATUS_SCHEMA_MISMATCH
    );

    ASSERT_STATUS(
        cd_ipc_encode_envelope(&message, &codec_config, frame, sizeof(frame), &frame_size),
        CD_STATUS_OK
    );
    memset(&tiny_codec_config, 0, sizeof(tiny_codec_config));
    tiny_codec_config.max_payload_size = 2u;
    ASSERT_STATUS(
        cd_ipc_decode_envelope(frame, frame_size, &tiny_codec_config, &decoded),
        CD_STATUS_CAPACITY_REACHED
    );

    ASSERT_STATUS(
        cd_ipc_decode_envelope(frame, CD_IPC_FRAME_HEADER_SIZE - 1u, &codec_config, &decoded),
        CD_STATUS_SCHEMA_MISMATCH
    );

    return 0;
}

static int test_transport_attach_detach_validation(void)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_inproc_hub_t *hub;
    cd_inproc_hub_config_t hub_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_transport_t invalid_transport;

    context = NULL;
    bus = NULL;
    hub = NULL;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));
    memset(&invalid_transport, 0, sizeof(invalid_transport));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);

    ASSERT_STATUS(cd_bus_attach_transport(NULL, &invalid_transport), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_bus_attach_transport(bus, NULL), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_bus_attach_transport(bus, &invalid_transport), CD_STATUS_INVALID_ARGUMENT);

    memset(&hub_config, 0, sizeof(hub_config));
    hub_config.max_peers = 2u;
    hub_config.max_queued_messages_per_peer = 8u;
    ASSERT_STATUS(cd_inproc_hub_init(context, &hub_config, &hub), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_b), CD_STATUS_OK);

    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport_a), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport_b), CD_STATUS_CAPACITY_REACHED);

    ASSERT_STATUS(cd_bus_detach_transport(bus, &transport_b), CD_STATUS_NOT_FOUND);
    ASSERT_STATUS(cd_bus_detach_transport(bus, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_detach_transport(bus, &transport_a), CD_STATUS_NOT_FOUND);
    ASSERT_STATUS(cd_bus_detach_transport(NULL, &transport_a), CD_STATUS_INVALID_ARGUMENT);
    ASSERT_STATUS(cd_bus_detach_transport(bus, NULL), CD_STATUS_INVALID_ARGUMENT);

    cd_inproc_transport_close(&transport_a);
    cd_inproc_transport_close(&transport_b);
    cd_inproc_hub_shutdown(hub);
    cd_bus_destroy(bus);
    cd_context_shutdown(context);
    return 0;
}

static int test_inproc_transport_event_routing_between_buses(void)
{
    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_inproc_hub_t *hub;
    cd_inproc_hub_config_t hub_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t sub;
    cd_publish_params_t publish;
    capture_state_t capture;
    handler_state_t handler;
    size_t processed;

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    hub = NULL;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 2u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    memset(&hub_config, 0, sizeof(hub_config));
    hub_config.max_peers = 2u;
    hub_config.max_queued_messages_per_peer = 32u;
    ASSERT_STATUS(cd_inproc_hub_init(context, &hub_config, &hub), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_b), CD_STATUS_OK);

    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 44;
    handler.return_status = CD_STATUS_OK;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 9u;
    sub.topic = 700u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = generic_handler;
    sub.user_data = &handler;
    ASSERT_STATUS(cd_subscribe(bus_b, &sub, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 3u;
    publish.topic = 700u;
    publish.schema_id = 11u;
    publish.schema_version = 1u;
    publish.flags = 0u;
    publish.payload = "xy";
    publish.payload_size = 2u;
    ASSERT_STATUS(cd_publish(bus_a, &publish, NULL), CD_STATUS_OK);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 1);
    ASSERT_TRUE(capture.markers[0] == 44);
    ASSERT_TRUE(capture.kinds[0] == CD_MESSAGE_EVENT);
    ASSERT_TRUE(strcmp(capture.payloads[0], "xy") == 0);

    ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);

    cd_inproc_transport_close(&transport_a);
    cd_inproc_transport_close(&transport_b);
    cd_inproc_hub_shutdown(hub);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);
    return 0;
}

static int test_inproc_transport_request_reply_between_buses(void)
{
    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_inproc_hub_t *hub;
    cd_inproc_hub_config_t hub_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t sub;
    request_replier_state_t replier_state;
    cd_request_params_t request;
    cd_request_token_t token;
    size_t processed;
    int ready;
    cd_reply_t reply;

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    hub = NULL;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 2u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    memset(&hub_config, 0, sizeof(hub_config));
    hub_config.max_peers = 2u;
    hub_config.max_queued_messages_per_peer = 32u;
    ASSERT_STATUS(cd_inproc_hub_init(context, &hub_config, &hub), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_inproc_hub_create_transport(hub, &transport_b), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    memset(&replier_state, 0, sizeof(replier_state));
    replier_state.bus = bus_b;
    replier_state.reply_payload = "remote-ok";
    replier_state.reply_payload_size = strlen("remote-ok");
    replier_state.reply_flags = 0u;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 55u;
    sub.topic = 808u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    sub.handler = request_replier_handler;
    sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus_b, &sub, NULL), CD_STATUS_OK);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 6u;
    request.target_endpoint = 55u;
    request.topic = 808u;
    request.schema_id = 77u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 1000000000ull;
    request.payload = "remote-ask";
    request.payload_size = strlen("remote-ask");

    token = 0u;
    ASSERT_STATUS(cd_request_async(bus_a, &request, &token), CD_STATUS_OK);
    ASSERT_TRUE(token != 0u);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(replier_state.request_hits == 1);
    ASSERT_TRUE(strcmp(replier_state.last_payload, "remote-ask") == 0);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 2u);

    ready = 0;
    memset(&reply, 0, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(bus_a, token, &reply, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload != NULL);
    ASSERT_TRUE(reply.payload_size == replier_state.reply_payload_size);
    ASSERT_TRUE(strncmp((const char *)reply.payload, "remote-ok", reply.payload_size) == 0);
    cd_reply_dispose(bus_a, &reply);

    cd_inproc_transport_close(&transport_a);
    cd_inproc_transport_close(&transport_b);
    cd_inproc_hub_shutdown(hub);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);
    return 0;
}

static int test_ipc_socket_transport_event_routing_between_buses(void)
{
    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t sub;
    cd_publish_params_t publish;
    capture_state_t capture;
    handler_state_t handler;
    size_t processed;
    int sockets[2];

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    sockets[0] = -1;
    sockets[1] = -1;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[0], NULL, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[1], NULL, &transport_b), CD_STATUS_OK);
    sockets[0] = -1;
    sockets[1] = -1;

    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    memset(&capture, 0, sizeof(capture));
    memset(&handler, 0, sizeof(handler));
    handler.capture = &capture;
    handler.marker = 77;
    handler.return_status = CD_STATUS_OK;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 19u;
    sub.topic = 1700u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    sub.handler = generic_handler;
    sub.user_data = &handler;
    ASSERT_STATUS(cd_subscribe(bus_b, &sub, NULL), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 2u;
    publish.topic = 1700u;
    publish.schema_id = 42u;
    publish.schema_version = 1u;
    publish.flags = 0u;
    publish.payload = "sock-xy";
    publish.payload_size = strlen("sock-xy");
    ASSERT_STATUS(cd_publish(bus_a, &publish, NULL), CD_STATUS_OK);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(capture.count == 1);
    ASSERT_TRUE(capture.markers[0] == 77);
    ASSERT_TRUE(capture.kinds[0] == CD_MESSAGE_EVENT);
    ASSERT_TRUE(strcmp(capture.payloads[0], "sock-xy") == 0);

    ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);

    cd_ipc_socket_transport_close(&transport_a);
    cd_ipc_socket_transport_close(&transport_b);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);
    return 0;
}

static int test_ipc_socket_transport_request_reply_between_buses(void)
{
    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t sub;
    request_replier_state_t replier_state;
    cd_request_params_t request;
    cd_request_token_t token;
    size_t processed;
    int ready;
    cd_reply_t reply;
    int sockets[2];

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    sockets[0] = -1;
    sockets[1] = -1;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[0], NULL, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[1], NULL, &transport_b), CD_STATUS_OK);
    sockets[0] = -1;
    sockets[1] = -1;

    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    memset(&replier_state, 0, sizeof(replier_state));
    replier_state.bus = bus_b;
    replier_state.reply_payload = "socket-ok";
    replier_state.reply_payload_size = strlen("socket-ok");
    replier_state.reply_flags = 0u;

    memset(&sub, 0, sizeof(sub));
    sub.endpoint = 55u;
    sub.topic = 1908u;
    sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    sub.handler = request_replier_handler;
    sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus_b, &sub, NULL), CD_STATUS_OK);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 6u;
    request.target_endpoint = 55u;
    request.topic = 1908u;
    request.schema_id = 77u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 1000000000ull;
    request.payload = "socket-ask";
    request.payload_size = strlen("socket-ask");

    token = 0u;
    ASSERT_STATUS(cd_request_async(bus_a, &request, &token), CD_STATUS_OK);
    ASSERT_TRUE(token != 0u);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 1u);
    ASSERT_TRUE(replier_state.request_hits == 1);
    ASSERT_TRUE(strcmp(replier_state.last_payload, "socket-ask") == 0);

    processed = 0u;
    ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
    ASSERT_TRUE(processed == 2u);

    ready = 0;
    memset(&reply, 0, sizeof(reply));
    ASSERT_STATUS(cd_poll_reply(bus_a, token, &reply, &ready), CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload != NULL);
    ASSERT_TRUE(reply.payload_size == replier_state.reply_payload_size);
    ASSERT_TRUE(strncmp((const char *)reply.payload, "socket-ok", reply.payload_size) == 0);
    cd_reply_dispose(bus_a, &reply);

    cd_ipc_socket_transport_close(&transport_a);
    cd_ipc_socket_transport_close(&transport_b);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);
    return 0;
}

static int test_ipc_socket_transport_protocol_mismatch_path(void)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_transport_t transport;
    size_t processed;
    int sockets[2];
    uint8_t malformed_frame[CD_IPC_FRAME_HEADER_SIZE];

    context = NULL;
    bus = NULL;
    sockets[0] = -1;
    sockets[1] = -1;
    memset(&transport, 0, sizeof(transport));
    memset(malformed_frame, 0, sizeof(malformed_frame));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[0], NULL, &transport), CD_STATUS_OK);
    sockets[0] = -1;

    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport), CD_STATUS_OK);
    ASSERT_TRUE(send(sockets[1], malformed_frame, sizeof(malformed_frame), 0) == (ssize_t)sizeof(malformed_frame));

    processed = 999u;
    ASSERT_STATUS(cd_bus_pump(bus, 0u, &processed), CD_STATUS_SCHEMA_MISMATCH);
    ASSERT_TRUE(processed == 0u);

    close_fd_if_open(&sockets[1]);
    cd_ipc_socket_transport_close(&transport);
    cd_bus_destroy(bus);
    cd_context_shutdown(context);
    return 0;
}

static int test_ipc_socket_transport_disconnect_paths(void)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_transport_t transport;
    size_t processed;
    int sockets[2];
    uint8_t truncated[13];
    cd_envelope_t message;

    context = NULL;
    bus = NULL;
    sockets[0] = -1;
    sockets[1] = -1;
    memset(&transport, 0, sizeof(transport));
    memset(truncated, 0xA5, sizeof(truncated));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[0], NULL, &transport), CD_STATUS_OK);
    sockets[0] = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport), CD_STATUS_OK);

    ASSERT_TRUE(send(sockets[1], truncated, sizeof(truncated), 0) == (ssize_t)sizeof(truncated));
    close_fd_if_open(&sockets[1]);
    processed = 123u;
    ASSERT_STATUS(cd_bus_pump(bus, 0u, &processed), CD_STATUS_TRANSPORT_UNAVAILABLE);
    ASSERT_TRUE(processed == 0u);

    cd_ipc_socket_transport_close(&transport);
    cd_bus_destroy(bus);
    cd_context_shutdown(context);

    context = NULL;
    sockets[0] = -1;
    sockets[1] = -1;
    memset(&transport, 0, sizeof(transport));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[0], NULL, &transport), CD_STATUS_OK);
    sockets[0] = -1;
    close_fd_if_open(&sockets[1]);

    memset(&message, 0, sizeof(message));
    message.message_id = 1u;
    message.kind = CD_MESSAGE_EVENT;
    message.topic = 500u;
    message.source_endpoint = 7u;
    message.flags = CD_MESSAGE_FLAG_NONE;
    message.payload = "x";
    message.payload_size = 1u;
    ASSERT_STATUS(transport.send(transport.impl, &message), CD_STATUS_TRANSPORT_UNAVAILABLE);

    cd_ipc_socket_transport_close(&transport);
    cd_context_shutdown(context);
    return 0;
}

static int run_ipc_socket_two_process_child(int socket_fd)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_transport_t transport;
    cd_subscription_desc_t event_sub;
    cd_subscription_desc_t request_sub;
    capture_state_t capture;
    handler_state_t event_handler;
    request_replier_state_t replier_state;
    size_t processed;
    int i;

    context = NULL;
    bus = NULL;
    memset(&transport, 0, sizeof(transport));
    memset(&capture, 0, sizeof(capture));
    memset(&event_handler, 0, sizeof(event_handler));
    memset(&replier_state, 0, sizeof(replier_state));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, socket_fd, NULL, &transport), CD_STATUS_OK);
    socket_fd = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport), CD_STATUS_OK);

    event_handler.capture = &capture;
    event_handler.marker = 1;
    event_handler.return_status = CD_STATUS_OK;

    memset(&event_sub, 0, sizeof(event_sub));
    event_sub.endpoint = 300u;
    event_sub.topic = 2401u;
    event_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    event_sub.handler = generic_handler;
    event_sub.user_data = &event_handler;
    ASSERT_STATUS(cd_subscribe(bus, &event_sub, NULL), CD_STATUS_OK);

    replier_state.bus = bus;
    replier_state.reply_payload = "proc-ok";
    replier_state.reply_payload_size = strlen("proc-ok");
    replier_state.reply_flags = 0u;

    memset(&request_sub, 0, sizeof(request_sub));
    request_sub.endpoint = 401u;
    request_sub.topic = 2402u;
    request_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    request_sub.handler = request_replier_handler;
    request_sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus, &request_sub, NULL), CD_STATUS_OK);

    for (i = 0; i < 4000; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus, 0u, &processed), CD_STATUS_OK);
        if (capture.count >= 1 && replier_state.request_hits >= 1) {
            break;
        }
        usleep(1000u);
    }

    ASSERT_TRUE(capture.count == 1);
    ASSERT_TRUE(capture.kinds[0] == CD_MESSAGE_EVENT);
    ASSERT_TRUE(strcmp(capture.payloads[0], "proc-event") == 0);
    ASSERT_TRUE(replier_state.request_hits == 1);
    ASSERT_TRUE(strcmp(replier_state.last_payload, "proc-ask") == 0);

    cd_ipc_socket_transport_close(&transport);
    cd_bus_destroy(bus);
    cd_context_shutdown(context);
    return 0;
}

static int test_ipc_socket_transport_forked_two_process_roundtrip(void)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_transport_t transport;
    cd_publish_params_t publish;
    cd_request_params_t request;
    cd_request_token_t token;
    cd_reply_t reply;
    cd_status_t pump_status;
    cd_status_t poll_status;
    size_t processed;
    int ready;
    int sockets[2];
    pid_t child_pid;
    int child_status;
    int i;

    context = NULL;
    bus = NULL;
    sockets[0] = -1;
    sockets[1] = -1;
    child_pid = -1;
    child_status = 0;
    token = 0u;
    ready = 0;
    pump_status = CD_STATUS_OK;
    poll_status = CD_STATUS_OK;
    memset(&transport, 0, sizeof(transport));
    memset(&reply, 0, sizeof(reply));

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    child_pid = fork();
    ASSERT_TRUE(child_pid >= 0);
    if (child_pid == 0) {
        int child_result;

        close_fd_if_open(&sockets[0]);
        child_result = run_ipc_socket_two_process_child(sockets[1]);
        close_fd_if_open(&sockets[1]);
        _exit(child_result == 0 ? 0 : 1);
    }

    close_fd_if_open(&sockets[1]);

    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, sockets[0], NULL, &transport), CD_STATUS_OK);
    sockets[0] = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport), CD_STATUS_OK);

    memset(&publish, 0, sizeof(publish));
    publish.source_endpoint = 10u;
    publish.topic = 2401u;
    publish.schema_id = 1u;
    publish.schema_version = 1u;
    publish.flags = 0u;
    publish.payload = "proc-event";
    publish.payload_size = strlen("proc-event");
    ASSERT_STATUS(cd_publish(bus, &publish, NULL), CD_STATUS_OK);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 10u;
    request.target_endpoint = 401u;
    request.topic = 2402u;
    request.schema_id = 2u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 2000000000ull;
    request.payload = "proc-ask";
    request.payload_size = strlen("proc-ask");
    ASSERT_STATUS(cd_request_async(bus, &request, &token), CD_STATUS_OK);
    ASSERT_TRUE(token != 0u);

    for (i = 0; i < 4000; ++i) {
        processed = 0u;
        pump_status = cd_bus_pump(bus, 0u, &processed);
        ASSERT_TRUE(
            pump_status == CD_STATUS_OK || pump_status == CD_STATUS_TRANSPORT_UNAVAILABLE
        );
        poll_status = cd_poll_reply(bus, token, &reply, &ready);
        if (poll_status == CD_STATUS_OK && ready == 1) {
            break;
        }
        ASSERT_TRUE(poll_status == CD_STATUS_OK);
        ASSERT_TRUE(ready == 0);
        if (pump_status == CD_STATUS_TRANSPORT_UNAVAILABLE) {
            break;
        }
        usleep(1000u);
    }

    ASSERT_TRUE(poll_status == CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload != NULL);
    ASSERT_TRUE(reply.payload_size == strlen("proc-ok"));
    ASSERT_TRUE(strncmp((const char *)reply.payload, "proc-ok", reply.payload_size) == 0);
    cd_reply_dispose(bus, &reply);

    ASSERT_TRUE(waitpid(child_pid, &child_status, 0) == child_pid);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_TRUE(WEXITSTATUS(child_status) == 0);

    cd_ipc_socket_transport_close(&transport);
    cd_bus_destroy(bus);
    cd_context_shutdown(context);
    return 0;
}

static int test_broker_process_routes_between_clients(void)
{
    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t event_sub;
    cd_subscription_desc_t request_sub;
    request_replier_state_t replier_state;
    capture_state_t capture;
    handler_state_t event_handler;
    cd_publish_params_t registration_publish;
    cd_publish_params_t event_publish;
    cd_request_params_t request;
    cd_request_token_t token;
    cd_reply_t reply;
    int ready;
    size_t processed;
    cd_status_t poll_status;
    int fd_a;
    int fd_b;
    pid_t broker_pid;
    int broker_status;
    int i;
    char socket_path[104];
    char routes_path[128];
    char broker_log_path[128];
    broker_metrics_snapshot_t broker_metrics;

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));
    memset(&capture, 0, sizeof(capture));
    memset(&event_handler, 0, sizeof(event_handler));
    memset(&replier_state, 0, sizeof(replier_state));
    memset(&reply, 0, sizeof(reply));
    fd_a = -1;
    fd_b = -1;
    broker_pid = -1;
    broker_status = 0;
    token = 0u;
    ready = 0;
    poll_status = CD_STATUS_OK;

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-broker-test-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        routes_path,
        sizeof(routes_path),
        "/tmp/conduit-broker-routes-%ld-%u.txt",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path,
        sizeof(broker_log_path),
        "/tmp/conduit-broker-log-%ld-%u.txt",
        (long)getpid(),
        (unsigned)rand()
    );
    unlink(socket_path);
    unlink(routes_path);
    unlink(broker_log_path);
    ASSERT_TRUE(write_text_file(routes_path, "topic 3501 901\n") == 0);

    broker_pid = spawn_broker_process(socket_path, routes_path, "8000", "0", broker_log_path);
    ASSERT_TRUE(broker_pid > 0);

    fd_a = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_a >= 0);
    fd_b = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_b >= 0);

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 64u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_a, NULL, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_b, NULL, &transport_b), CD_STATUS_OK);
    fd_a = -1;
    fd_b = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    event_handler.capture = &capture;
    event_handler.marker = 901;
    event_handler.return_status = CD_STATUS_OK;

    memset(&event_sub, 0, sizeof(event_sub));
    event_sub.endpoint = 901u;
    event_sub.topic = 3501u;
    event_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT);
    event_sub.handler = generic_handler;
    event_sub.user_data = &event_handler;
    ASSERT_STATUS(cd_subscribe(bus_b, &event_sub, NULL), CD_STATUS_OK);

    replier_state.bus = bus_b;
    replier_state.reply_payload = "broker-ok";
    replier_state.reply_payload_size = strlen("broker-ok");
    replier_state.reply_flags = 0u;

    memset(&request_sub, 0, sizeof(request_sub));
    request_sub.endpoint = 955u;
    request_sub.topic = 3502u;
    request_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    request_sub.handler = request_replier_handler;
    request_sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus_b, &request_sub, NULL), CD_STATUS_OK);

    memset(&registration_publish, 0, sizeof(registration_publish));
    registration_publish.source_endpoint = 901u;
    registration_publish.topic = 3599u;
    registration_publish.flags = 0u;
    registration_publish.payload = "register-event";
    registration_publish.payload_size = strlen("register-event");
    ASSERT_STATUS(cd_publish(bus_b, &registration_publish, NULL), CD_STATUS_OK);

    memset(&registration_publish, 0, sizeof(registration_publish));
    registration_publish.source_endpoint = 955u;
    registration_publish.topic = 3599u;
    registration_publish.flags = 0u;
    registration_publish.payload = "register";
    registration_publish.payload_size = strlen("register");
    ASSERT_STATUS(cd_publish(bus_b, &registration_publish, NULL), CD_STATUS_OK);

    for (i = 0; i < 200; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        usleep(1000u);
    }

    memset(&event_publish, 0, sizeof(event_publish));
    event_publish.source_endpoint = 10u;
    event_publish.topic = 3501u;
    event_publish.schema_id = 1u;
    event_publish.schema_version = 1u;
    event_publish.flags = 0u;
    event_publish.payload = "broker-ev";
    event_publish.payload_size = strlen("broker-ev");
    ASSERT_STATUS(cd_publish(bus_a, &event_publish, NULL), CD_STATUS_OK);

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 10u;
    request.target_endpoint = 955u;
    request.topic = 3502u;
    request.schema_id = 2u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 2000000000ull;
    request.payload = "broker-ask";
    request.payload_size = strlen("broker-ask");
    ASSERT_STATUS(cd_request_async(bus_a, &request, &token), CD_STATUS_OK);

    for (i = 0; i < 3000; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        poll_status = cd_poll_reply(bus_a, token, &reply, &ready);
        if (poll_status == CD_STATUS_OK && ready == 1) {
            break;
        }
        ASSERT_TRUE(poll_status == CD_STATUS_OK);
        ASSERT_TRUE(ready == 0);
        usleep(1000u);
    }

    ASSERT_TRUE(capture.count == 1);
    ASSERT_TRUE(strcmp(capture.payloads[0], "broker-ev") == 0);
    ASSERT_TRUE(replier_state.request_hits == 1);
    ASSERT_TRUE(strcmp(replier_state.last_payload, "broker-ask") == 0);
    ASSERT_TRUE(poll_status == CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload_size == strlen("broker-ok"));
    ASSERT_TRUE(strncmp((const char *)reply.payload, "broker-ok", reply.payload_size) == 0);
    cd_reply_dispose(bus_a, &reply);

    cd_ipc_socket_transport_close(&transport_a);
    cd_ipc_socket_transport_close(&transport_b);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path, &broker_metrics) == 0);
    ASSERT_TRUE(broker_metrics.published >= 5u);
    ASSERT_TRUE(broker_metrics.delivered >= 3u);
    ASSERT_TRUE(broker_metrics.dropped >= 1u);
    unlink(socket_path);
    unlink(routes_path);
    unlink(broker_log_path);
    return 0;
}

static int test_broker_reconnect_without_restart(void)
{
    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t request_sub;
    request_replier_state_t replier_state;
    cd_publish_params_t registration_publish;
    cd_request_params_t request;
    cd_request_token_t token;
    cd_reply_t reply;
    int ready;
    size_t processed;
    cd_status_t poll_status;
    int fd_a;
    int fd_b;
    pid_t broker_pid;
    int broker_status;
    int i;
    char socket_path[104];
    char broker_log_path[128];
    broker_metrics_snapshot_t broker_metrics;

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));
    memset(&replier_state, 0, sizeof(replier_state));
    memset(&reply, 0, sizeof(reply));
    fd_a = -1;
    fd_b = -1;
    broker_pid = -1;
    broker_status = 0;
    token = 0u;
    ready = 0;
    poll_status = CD_STATUS_OK;

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-broker-reconnect-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path,
        sizeof(broker_log_path),
        "/tmp/conduit-broker-reconnect-log-%ld-%u.txt",
        (long)getpid(),
        (unsigned)rand()
    );
    unlink(socket_path);
    unlink(broker_log_path);

    broker_pid = spawn_broker_process(socket_path, NULL, "15000", "0", broker_log_path);
    ASSERT_TRUE(broker_pid > 0);

    fd_a = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_a >= 0);
    fd_b = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_b >= 0);

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 64u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_a, NULL, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_b, NULL, &transport_b), CD_STATUS_OK);
    fd_a = -1;
    fd_b = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    replier_state.bus = bus_b;
    replier_state.reply_payload = "reconnect-1";
    replier_state.reply_payload_size = strlen("reconnect-1");
    replier_state.reply_flags = 0u;

    memset(&request_sub, 0, sizeof(request_sub));
    request_sub.endpoint = 955u;
    request_sub.topic = 3602u;
    request_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    request_sub.handler = request_replier_handler;
    request_sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus_b, &request_sub, NULL), CD_STATUS_OK);

    memset(&registration_publish, 0, sizeof(registration_publish));
    registration_publish.source_endpoint = 955u;
    registration_publish.topic = 3699u;
    registration_publish.flags = 0u;
    registration_publish.payload = "register-1";
    registration_publish.payload_size = strlen("register-1");
    ASSERT_STATUS(cd_publish(bus_b, &registration_publish, NULL), CD_STATUS_OK);

    for (i = 0; i < 250; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        usleep(1000u);
    }

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 10u;
    request.target_endpoint = 955u;
    request.topic = 3602u;
    request.schema_id = 2u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 2000000000ull;
    request.payload = "ask-1";
    request.payload_size = strlen("ask-1");
    ASSERT_STATUS(cd_request_async(bus_a, &request, &token), CD_STATUS_OK);

    for (i = 0; i < 3000; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        poll_status = cd_poll_reply(bus_a, token, &reply, &ready);
        if (poll_status == CD_STATUS_OK && ready == 1) {
            break;
        }
        ASSERT_TRUE(poll_status == CD_STATUS_OK);
        ASSERT_TRUE(ready == 0);
        usleep(1000u);
    }
    ASSERT_TRUE(poll_status == CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload_size == strlen("reconnect-1"));
    ASSERT_TRUE(strncmp((const char *)reply.payload, "reconnect-1", reply.payload_size) == 0);
    cd_reply_dispose(bus_a, &reply);

    cd_ipc_socket_transport_close(&transport_b);
    cd_bus_destroy(bus_b);
    bus_b = NULL;
    memset(&transport_b, 0, sizeof(transport_b));

    fd_b = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_b >= 0);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_b, NULL, &transport_b), CD_STATUS_OK);
    fd_b = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    memset(&replier_state, 0, sizeof(replier_state));
    replier_state.bus = bus_b;
    replier_state.reply_payload = "reconnect-2";
    replier_state.reply_payload_size = strlen("reconnect-2");
    replier_state.reply_flags = 0u;

    memset(&request_sub, 0, sizeof(request_sub));
    request_sub.endpoint = 955u;
    request_sub.topic = 3602u;
    request_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    request_sub.handler = request_replier_handler;
    request_sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus_b, &request_sub, NULL), CD_STATUS_OK);

    memset(&registration_publish, 0, sizeof(registration_publish));
    registration_publish.source_endpoint = 955u;
    registration_publish.topic = 3699u;
    registration_publish.flags = 0u;
    registration_publish.payload = "register-2";
    registration_publish.payload_size = strlen("register-2");
    ASSERT_STATUS(cd_publish(bus_b, &registration_publish, NULL), CD_STATUS_OK);

    for (i = 0; i < 250; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        usleep(1000u);
    }

    token = 0u;
    ready = 0;
    poll_status = CD_STATUS_OK;
    memset(&reply, 0, sizeof(reply));
    memset(&request, 0, sizeof(request));
    request.source_endpoint = 10u;
    request.target_endpoint = 955u;
    request.topic = 3602u;
    request.schema_id = 2u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 2000000000ull;
    request.payload = "ask-2";
    request.payload_size = strlen("ask-2");
    ASSERT_STATUS(cd_request_async(bus_a, &request, &token), CD_STATUS_OK);

    for (i = 0; i < 3000; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        poll_status = cd_poll_reply(bus_a, token, &reply, &ready);
        if (poll_status == CD_STATUS_OK && ready == 1) {
            break;
        }
        ASSERT_TRUE(poll_status == CD_STATUS_OK);
        ASSERT_TRUE(ready == 0);
        usleep(1000u);
    }
    ASSERT_TRUE(poll_status == CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload_size == strlen("reconnect-2"));
    ASSERT_TRUE(strncmp((const char *)reply.payload, "reconnect-2", reply.payload_size) == 0);
    cd_reply_dispose(bus_a, &reply);

    cd_ipc_socket_transport_close(&transport_a);
    cd_ipc_socket_transport_close(&transport_b);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path, &broker_metrics) == 0);
    ASSERT_TRUE(broker_metrics.published >= 6u);
    ASSERT_TRUE(broker_metrics.delivered >= 4u);
    unlink(socket_path);
    unlink(broker_log_path);
    return 0;
}

static int test_broker_diagnostics_request_endpoint(void)
{
    cd_context_t *context;
    cd_bus_t *bus;
    cd_bus_config_t bus_config;
    cd_transport_t transport;
    cd_request_params_t request;
    cd_request_token_t token;
    cd_reply_t reply;
    broker_metrics_snapshot_t payload_metrics;
    broker_metrics_snapshot_t final_metrics;
    size_t payload_clients;
    size_t processed;
    int fd;
    pid_t broker_pid;
    int broker_status;
    int ready;
    int i;
    cd_status_t poll_status;
    char socket_path[104];
    char broker_log_path[128];

    context = NULL;
    bus = NULL;
    memset(&transport, 0, sizeof(transport));
    memset(&request, 0, sizeof(request));
    memset(&reply, 0, sizeof(reply));
    memset(&payload_metrics, 0, sizeof(payload_metrics));
    memset(&final_metrics, 0, sizeof(final_metrics));
    payload_clients = 0u;
    token = 0u;
    processed = 0u;
    fd = -1;
    broker_pid = -1;
    broker_status = 0;
    ready = 0;
    i = 0;
    poll_status = CD_STATUS_OK;

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-broker-diag-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path,
        sizeof(broker_log_path),
        "/tmp/conduit-broker-diag-log-%ld-%u.txt",
        (long)getpid(),
        (unsigned)rand()
    );
    unlink(socket_path);
    unlink(broker_log_path);

    broker_pid = spawn_broker_process(socket_path, NULL, "10000", "0", broker_log_path);
    ASSERT_TRUE(broker_pid > 0);

    fd = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd >= 0);

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 32u;
    bus_config.max_subscriptions = 8u;
    bus_config.max_inflight_requests = 8u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd, NULL, &transport), CD_STATUS_OK);
    fd = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus, &transport), CD_STATUS_OK);

    request.source_endpoint = 777u;
    request.target_endpoint = TEST_BROKER_DIAGNOSTICS_ENDPOINT;
    request.topic = TEST_BROKER_DIAGNOSTICS_TOPIC_METRICS;
    request.schema_id = TEST_BROKER_DIAGNOSTICS_SCHEMA_METRICS_TEXT;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 2000000000ull;
    request.payload = "metrics";
    request.payload_size = strlen("metrics");
    ASSERT_STATUS(cd_request_async(bus, &request, &token), CD_STATUS_OK);

    for (i = 0; i < 3000; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus, 0u, &processed), CD_STATUS_OK);
        poll_status = cd_poll_reply(bus, token, &reply, &ready);
        if (poll_status == CD_STATUS_OK && ready == 1) {
            break;
        }
        ASSERT_TRUE(poll_status == CD_STATUS_OK);
        ASSERT_TRUE(ready == 0);
        usleep(1000u);
    }

    ASSERT_TRUE(poll_status == CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.schema_id == TEST_BROKER_DIAGNOSTICS_SCHEMA_METRICS_TEXT);
    ASSERT_TRUE(reply.schema_version == 1u);
    ASSERT_TRUE(reply.payload_size > 0u);
    ASSERT_TRUE(
        parse_metrics_snapshot_payload(
            reply.payload,
            reply.payload_size,
            &payload_clients,
            &payload_metrics
        ) == 0
    );
    ASSERT_TRUE(payload_clients >= 1u);
    ASSERT_TRUE(payload_metrics.published >= 1u);
    cd_reply_dispose(bus, &reply);

    cd_ipc_socket_transport_close(&transport);
    cd_bus_destroy(bus);
    cd_context_shutdown(context);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path, &final_metrics) == 0);
    ASSERT_TRUE(final_metrics.published >= 1u);
    ASSERT_TRUE(final_metrics.delivered >= 1u);
    ASSERT_TRUE(payload_metrics.published <= final_metrics.published);
    ASSERT_TRUE(payload_metrics.delivered <= final_metrics.delivered);

    unlink(socket_path);
    unlink(broker_log_path);
    return 0;
}

static int test_broker_restart_with_client_reconnect(void)
{
    cd_context_t *context;
    cd_bus_t *bus_a;
    cd_bus_t *bus_b;
    cd_bus_config_t bus_config;
    cd_transport_t transport_a;
    cd_transport_t transport_b;
    cd_subscription_desc_t request_sub;
    request_replier_state_t replier_state;
    cd_publish_params_t registration_publish;
    cd_request_params_t request;
    cd_request_token_t token;
    cd_reply_t reply;
    int ready;
    size_t processed;
    cd_status_t poll_status;
    int fd_a;
    int fd_b;
    pid_t broker_pid;
    int broker_status;
    int i;
    char socket_path[104];
    char broker_log_path_a[128];
    char broker_log_path_b[128];
    broker_metrics_snapshot_t broker_metrics_a;
    broker_metrics_snapshot_t broker_metrics_b;

    context = NULL;
    bus_a = NULL;
    bus_b = NULL;
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));
    memset(&replier_state, 0, sizeof(replier_state));
    memset(&reply, 0, sizeof(reply));
    fd_a = -1;
    fd_b = -1;
    broker_pid = -1;
    broker_status = 0;
    token = 0u;
    ready = 0;
    poll_status = CD_STATUS_OK;

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-broker-restart-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path_a,
        sizeof(broker_log_path_a),
        "/tmp/conduit-broker-restart-log-a-%ld-%u.txt",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path_b,
        sizeof(broker_log_path_b),
        "/tmp/conduit-broker-restart-log-b-%ld-%u.txt",
        (long)getpid(),
        (unsigned)rand()
    );
    unlink(socket_path);
    unlink(broker_log_path_a);
    unlink(broker_log_path_b);

    broker_pid = spawn_broker_process(socket_path, NULL, "20000", "0", broker_log_path_a);
    ASSERT_TRUE(broker_pid > 0);

    fd_a = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_a >= 0);
    fd_b = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_b >= 0);

    ASSERT_STATUS(cd_context_init(&context, NULL), CD_STATUS_OK);
    memset(&bus_config, 0, sizeof(bus_config));
    bus_config.max_queued_messages = 64u;
    bus_config.max_subscriptions = 16u;
    bus_config.max_inflight_requests = 16u;
    bus_config.max_transports = 1u;
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_create(context, &bus_config, &bus_b), CD_STATUS_OK);

    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_a, NULL, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_b, NULL, &transport_b), CD_STATUS_OK);
    fd_a = -1;
    fd_b = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    replier_state.bus = bus_b;
    replier_state.reply_payload = "restart-1";
    replier_state.reply_payload_size = strlen("restart-1");
    replier_state.reply_flags = 0u;

    memset(&request_sub, 0, sizeof(request_sub));
    request_sub.endpoint = 997u;
    request_sub.topic = 3702u;
    request_sub.kind_mask = CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST);
    request_sub.handler = request_replier_handler;
    request_sub.user_data = &replier_state;
    ASSERT_STATUS(cd_subscribe(bus_b, &request_sub, NULL), CD_STATUS_OK);

    memset(&registration_publish, 0, sizeof(registration_publish));
    registration_publish.source_endpoint = 997u;
    registration_publish.topic = 3799u;
    registration_publish.flags = 0u;
    registration_publish.payload = "register-r1";
    registration_publish.payload_size = strlen("register-r1");
    ASSERT_STATUS(cd_publish(bus_b, &registration_publish, NULL), CD_STATUS_OK);

    for (i = 0; i < 250; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        usleep(1000u);
    }

    memset(&request, 0, sizeof(request));
    request.source_endpoint = 44u;
    request.target_endpoint = 997u;
    request.topic = 3702u;
    request.schema_id = 2u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 2000000000ull;
    request.payload = "ask-r1";
    request.payload_size = strlen("ask-r1");
    ASSERT_STATUS(cd_request_async(bus_a, &request, &token), CD_STATUS_OK);

    for (i = 0; i < 3000; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        poll_status = cd_poll_reply(bus_a, token, &reply, &ready);
        if (poll_status == CD_STATUS_OK && ready == 1) {
            break;
        }
        ASSERT_TRUE(poll_status == CD_STATUS_OK);
        ASSERT_TRUE(ready == 0);
        usleep(1000u);
    }
    ASSERT_TRUE(poll_status == CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload_size == strlen("restart-1"));
    ASSERT_TRUE(strncmp((const char *)reply.payload, "restart-1", reply.payload_size) == 0);
    cd_reply_dispose(bus_a, &reply);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path_a, &broker_metrics_a) == 0);
    ASSERT_TRUE(broker_metrics_a.published >= 3u);
    ASSERT_TRUE(broker_metrics_a.delivered >= 2u);

    ASSERT_STATUS(cd_bus_detach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_detach_transport(bus_b, &transport_b), CD_STATUS_OK);
    cd_ipc_socket_transport_close(&transport_a);
    cd_ipc_socket_transport_close(&transport_b);
    memset(&transport_a, 0, sizeof(transport_a));
    memset(&transport_b, 0, sizeof(transport_b));

    broker_pid = spawn_broker_process(socket_path, NULL, "20000", "0", broker_log_path_b);
    ASSERT_TRUE(broker_pid > 0);

    fd_a = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_a >= 0);
    fd_b = connect_unix_socket_retry(socket_path, 1200, 1000u);
    ASSERT_TRUE(fd_b >= 0);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_a, NULL, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_ipc_socket_transport_init(context, fd_b, NULL, &transport_b), CD_STATUS_OK);
    fd_a = -1;
    fd_b = -1;
    ASSERT_STATUS(cd_bus_attach_transport(bus_a, &transport_a), CD_STATUS_OK);
    ASSERT_STATUS(cd_bus_attach_transport(bus_b, &transport_b), CD_STATUS_OK);

    replier_state.reply_payload = "restart-2";
    replier_state.reply_payload_size = strlen("restart-2");

    memset(&registration_publish, 0, sizeof(registration_publish));
    registration_publish.source_endpoint = 997u;
    registration_publish.topic = 3799u;
    registration_publish.flags = 0u;
    registration_publish.payload = "register-r2";
    registration_publish.payload_size = strlen("register-r2");
    ASSERT_STATUS(cd_publish(bus_b, &registration_publish, NULL), CD_STATUS_OK);

    for (i = 0; i < 250; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        usleep(1000u);
    }

    token = 0u;
    ready = 0;
    poll_status = CD_STATUS_OK;
    memset(&reply, 0, sizeof(reply));
    memset(&request, 0, sizeof(request));
    request.source_endpoint = 44u;
    request.target_endpoint = 997u;
    request.topic = 3702u;
    request.schema_id = 2u;
    request.schema_version = 1u;
    request.flags = 0u;
    request.timeout_ns = 2000000000ull;
    request.payload = "ask-r2";
    request.payload_size = strlen("ask-r2");
    ASSERT_STATUS(cd_request_async(bus_a, &request, &token), CD_STATUS_OK);

    for (i = 0; i < 3000; ++i) {
        processed = 0u;
        ASSERT_STATUS(cd_bus_pump(bus_b, 0u, &processed), CD_STATUS_OK);
        ASSERT_STATUS(cd_bus_pump(bus_a, 0u, &processed), CD_STATUS_OK);
        poll_status = cd_poll_reply(bus_a, token, &reply, &ready);
        if (poll_status == CD_STATUS_OK && ready == 1) {
            break;
        }
        ASSERT_TRUE(poll_status == CD_STATUS_OK);
        ASSERT_TRUE(ready == 0);
        usleep(1000u);
    }
    ASSERT_TRUE(poll_status == CD_STATUS_OK);
    ASSERT_TRUE(ready == 1);
    ASSERT_TRUE(reply.payload_size == strlen("restart-2"));
    ASSERT_TRUE(strncmp((const char *)reply.payload, "restart-2", reply.payload_size) == 0);
    cd_reply_dispose(bus_a, &reply);

    cd_ipc_socket_transport_close(&transport_a);
    cd_ipc_socket_transport_close(&transport_b);
    cd_bus_destroy(bus_a);
    cd_bus_destroy(bus_b);
    cd_context_shutdown(context);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path_b, &broker_metrics_b) == 0);
    ASSERT_TRUE(broker_metrics_b.published >= 3u);
    ASSERT_TRUE(broker_metrics_b.delivered >= 2u);
    unlink(socket_path);
    unlink(broker_log_path_a);
    unlink(broker_log_path_b);
    return 0;
}

static int test_loadgen_soak_against_broker(void)
{
    pid_t broker_pid;
    pid_t loadgen_pid;
    int broker_status;
    int loadgen_status;
    char socket_path[104];
    char broker_log_path[128];
    char loadgen_log_path[128];
    broker_metrics_snapshot_t broker_metrics;

    broker_pid = -1;
    loadgen_pid = -1;
    broker_status = 0;
    loadgen_status = 0;

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-loadgen-soak-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path,
        sizeof(broker_log_path),
        "/tmp/conduit-loadgen-broker-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        loadgen_log_path,
        sizeof(loadgen_log_path),
        "/tmp/conduit-loadgen-client-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);

    broker_pid = spawn_broker_process(socket_path, NULL, "45000", "0", broker_log_path);
    ASSERT_TRUE(broker_pid > 0);
    ASSERT_TRUE(wait_for_broker_ready(socket_path, 5000, 1000u) == 0);

    loadgen_pid = spawn_loadgen_process(
        socket_path,
        "3000",
        "600",
        "64",
        "25000",
        "2000000000",
        "4096",
        "256",
        loadgen_log_path
    );
    ASSERT_TRUE(loadgen_pid > 0);
    ASSERT_TRUE(waitpid(loadgen_pid, &loadgen_status, 0) == loadgen_pid);
    ASSERT_TRUE(WIFEXITED(loadgen_status));
    if (!WIFEXITED(loadgen_status) || WEXITSTATUS(loadgen_status) != 0) {
        dump_log_file_to_stderr("loadgen soak log", loadgen_log_path);
        dump_log_file_to_stderr("broker soak log", broker_log_path);
    }
    ASSERT_TRUE(WEXITSTATUS(loadgen_status) == 0);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path, &broker_metrics) == 0);
    ASSERT_TRUE(broker_metrics.published >= 4200u);
    ASSERT_TRUE(broker_metrics.delivered >= 3600u);
    ASSERT_TRUE(broker_metrics.timeouts == 0u);

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);
    return 0;
}

static int test_loadgen_profile_baseline_against_broker(void)
{
    pid_t broker_pid;
    pid_t loadgen_pid;
    int broker_status;
    int loadgen_status;
    char socket_path[104];
    char broker_log_path[128];
    char loadgen_log_path[128];
    broker_metrics_snapshot_t broker_metrics;
    loadgen_summary_t summary;

    broker_pid = -1;
    loadgen_pid = -1;
    broker_status = 0;
    loadgen_status = 0;
    memset(&broker_metrics, 0, sizeof(broker_metrics));
    memset(&summary, 0, sizeof(summary));

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-loadgen-profile-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path,
        sizeof(broker_log_path),
        "/tmp/conduit-loadgen-profile-broker-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        loadgen_log_path,
        sizeof(loadgen_log_path),
        "/tmp/conduit-loadgen-profile-client-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);

    broker_pid = spawn_broker_process(socket_path, NULL, "50000", "0", broker_log_path);
    ASSERT_TRUE(broker_pid > 0);
    ASSERT_TRUE(wait_for_broker_ready(socket_path, 5000, 1000u) == 0);

    loadgen_pid = spawn_loadgen_process(
        socket_path,
        "4000",
        "800",
        "64",
        "30000",
        "2000000000",
        "4096",
        "512",
        loadgen_log_path
    );
    ASSERT_TRUE(loadgen_pid > 0);
    ASSERT_TRUE(waitpid(loadgen_pid, &loadgen_status, 0) == loadgen_pid);
    ASSERT_TRUE(WIFEXITED(loadgen_status));
    if (!WIFEXITED(loadgen_status) || WEXITSTATUS(loadgen_status) != 0) {
        dump_log_file_to_stderr("loadgen profile log", loadgen_log_path);
        dump_log_file_to_stderr("broker profile log", broker_log_path);
    }
    ASSERT_TRUE(WEXITSTATUS(loadgen_status) == 0);
    ASSERT_TRUE(parse_loadgen_done_summary(loadgen_log_path, &summary) == 0);

    ASSERT_TRUE(summary.events_sent == 4000u);
    ASSERT_TRUE(summary.events_received == 4000u);
    ASSERT_TRUE(summary.requests_sent == 800u);
    ASSERT_TRUE(summary.replies_received == 800u);
    ASSERT_TRUE(summary.request_hits == 800u);
    ASSERT_TRUE(summary.elapsed_ms > 0u);
    ASSERT_TRUE(summary.event_phase_ms > 0u);
    ASSERT_TRUE(summary.request_phase_ms > 0u);
    ASSERT_TRUE(summary.req_rtt_avg_us > 0.0);
    ASSERT_TRUE(summary.req_rtt_max_us > 0.0);

    ASSERT_TRUE(summary.throughput_msg_per_s >= 200.0);
    ASSERT_TRUE(summary.req_rtt_avg_us <= 50000.0);
    ASSERT_TRUE(summary.req_rtt_max_us <= 500000.0);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path, &broker_metrics) == 0);
    ASSERT_TRUE(broker_metrics.published >= 5600u);
    ASSERT_TRUE(broker_metrics.delivered >= 4800u);
    ASSERT_TRUE(broker_metrics.timeouts == 0u);

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);
    return 0;
}

static int test_broker_malformed_frame_burst_under_load(void)
{
    pid_t broker_pid;
    pid_t loadgen_pid;
    int broker_status;
    int loadgen_status;
    char socket_path[104];
    char broker_log_path[128];
    char loadgen_log_path[128];
    broker_metrics_snapshot_t broker_metrics;
    int i;

    broker_pid = -1;
    loadgen_pid = -1;
    broker_status = 0;
    loadgen_status = 0;

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-broker-malformed-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path,
        sizeof(broker_log_path),
        "/tmp/conduit-broker-malformed-log-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        loadgen_log_path,
        sizeof(loadgen_log_path),
        "/tmp/conduit-loadgen-malformed-log-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);

    broker_pid = spawn_broker_process(socket_path, NULL, "45000", "0", broker_log_path);
    ASSERT_TRUE(broker_pid > 0);
    ASSERT_TRUE(wait_for_broker_ready(socket_path, 5000, 1000u) == 0);

    loadgen_pid = spawn_loadgen_process(
        socket_path,
        "1800",
        "350",
        "64",
        "25000",
        "2000000000",
        "4096",
        "256",
        loadgen_log_path
    );
    ASSERT_TRUE(loadgen_pid > 0);

    for (i = 0; i < 32; ++i) {
        int attacker_fd;
        uint8_t malformed_frame[CD_IPC_FRAME_HEADER_SIZE];

        attacker_fd = connect_unix_socket_retry(socket_path, 1200, 1000u);
        ASSERT_TRUE(attacker_fd >= 0);
        memset(malformed_frame, 0, sizeof(malformed_frame));
        ASSERT_TRUE(
            send(attacker_fd, malformed_frame, sizeof(malformed_frame), 0) ==
            (ssize_t)sizeof(malformed_frame)
        );
        close_fd_if_open(&attacker_fd);
        usleep(400u);
    }

    ASSERT_TRUE(waitpid(loadgen_pid, &loadgen_status, 0) == loadgen_pid);
    ASSERT_TRUE(WIFEXITED(loadgen_status));
    if (!WIFEXITED(loadgen_status) || WEXITSTATUS(loadgen_status) != 0) {
        dump_log_file_to_stderr("loadgen malformed log", loadgen_log_path);
        dump_log_file_to_stderr("broker malformed log", broker_log_path);
    }
    ASSERT_TRUE(WEXITSTATUS(loadgen_status) == 0);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path, &broker_metrics) == 0);
    ASSERT_TRUE(broker_metrics.published >= 2500u);
    ASSERT_TRUE(broker_metrics.delivered >= 2100u);
    /* Transport error counters vary across OS socket stacks; survival + throughput is the gate. */

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);
    return 0;
}

static int test_broker_disconnect_storm_under_load(void)
{
    pid_t broker_pid;
    pid_t loadgen_pid;
    int broker_status;
    int loadgen_status;
    char socket_path[104];
    char broker_log_path[128];
    char loadgen_log_path[128];
    broker_metrics_snapshot_t broker_metrics;
    int i;

    broker_pid = -1;
    loadgen_pid = -1;
    broker_status = 0;
    loadgen_status = 0;

    if (!unix_path_socket_bind_supported()) {
        return 0;
    }

    snprintf(
        socket_path,
        sizeof(socket_path),
        "/tmp/conduit-broker-storm-%ld-%u.sock",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        broker_log_path,
        sizeof(broker_log_path),
        "/tmp/conduit-broker-storm-log-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );
    snprintf(
        loadgen_log_path,
        sizeof(loadgen_log_path),
        "/tmp/conduit-loadgen-storm-log-%ld-%u.log",
        (long)getpid(),
        (unsigned)rand()
    );

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);

    broker_pid = spawn_broker_process(socket_path, NULL, "45000", "0", broker_log_path);
    ASSERT_TRUE(broker_pid > 0);
    ASSERT_TRUE(wait_for_broker_ready(socket_path, 5000, 1000u) == 0);

    loadgen_pid = spawn_loadgen_process(
        socket_path,
        "1800",
        "350",
        "64",
        "25000",
        "2000000000",
        "4096",
        "256",
        loadgen_log_path
    );
    ASSERT_TRUE(loadgen_pid > 0);

    for (i = 0; i < 80; ++i) {
        int transient_fd;

        transient_fd = connect_unix_socket_retry(socket_path, 1200, 1000u);
        ASSERT_TRUE(transient_fd >= 0);
        close_fd_if_open(&transient_fd);
        usleep(300u);
    }

    ASSERT_TRUE(waitpid(loadgen_pid, &loadgen_status, 0) == loadgen_pid);
    ASSERT_TRUE(WIFEXITED(loadgen_status));
    if (!WIFEXITED(loadgen_status) || WEXITSTATUS(loadgen_status) != 0) {
        dump_log_file_to_stderr("loadgen storm log", loadgen_log_path);
        dump_log_file_to_stderr("broker storm log", broker_log_path);
    }
    ASSERT_TRUE(WEXITSTATUS(loadgen_status) == 0);

    ASSERT_TRUE(stop_broker_process(broker_pid, &broker_status) == 0);
    ASSERT_TRUE(WIFEXITED(broker_status));
    ASSERT_TRUE(WEXITSTATUS(broker_status) == 0);
    ASSERT_TRUE(parse_broker_final_metrics(broker_log_path, &broker_metrics) == 0);
    ASSERT_TRUE(broker_metrics.published >= 2500u);
    ASSERT_TRUE(broker_metrics.delivered >= 2100u);
    /* Disconnect storm still must not break active load routing; absolute error counts are non-portable. */

    unlink(socket_path);
    unlink(broker_log_path);
    unlink(loadgen_log_path);
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
    RUN_TEST(test_trace_hook_reports_core_events);
    RUN_TEST(test_threadsafe_multi_producer_publish);
    RUN_TEST(test_threadsafe_concurrent_request_reply_ownership);
    RUN_TEST(test_request_reply_roundtrip_and_dispose);
    RUN_TEST(test_request_timeout_with_fake_clock);
    RUN_TEST(test_request_inflight_capacity_and_validation);
    RUN_TEST(test_transport_attach_detach_validation);
    RUN_TEST(test_inproc_transport_event_routing_between_buses);
    RUN_TEST(test_inproc_transport_request_reply_between_buses);
    RUN_TEST(test_ipc_socket_transport_event_routing_between_buses);
    RUN_TEST(test_ipc_socket_transport_request_reply_between_buses);
    RUN_TEST(test_ipc_socket_transport_protocol_mismatch_path);
    RUN_TEST(test_ipc_socket_transport_disconnect_paths);
    RUN_TEST(test_ipc_socket_transport_forked_two_process_roundtrip);
    RUN_TEST(test_broker_process_routes_between_clients);
    RUN_TEST(test_broker_reconnect_without_restart);
    RUN_TEST(test_broker_diagnostics_request_endpoint);
    RUN_TEST(test_broker_restart_with_client_reconnect);
    RUN_TEST(test_loadgen_soak_against_broker);
    RUN_TEST(test_loadgen_profile_baseline_against_broker);
    RUN_TEST(test_broker_malformed_frame_burst_under_load);
    RUN_TEST(test_broker_disconnect_storm_under_load);
    RUN_TEST(test_ipc_codec_roundtrip);
    RUN_TEST(test_ipc_codec_validation_guards);
    RUN_TEST(test_queue_and_argument_edges);

    return 0;
}
