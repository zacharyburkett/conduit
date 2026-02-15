#include "internal.h"

#include "conduit/transport_ipc_socket.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

enum {
    CD_IPC_PROTOCOL_MAGIC = 0x43444950u /* "CDIP" */
};

enum {
    CD_IPC_OFFSET_MAGIC = 0u,
    CD_IPC_OFFSET_VERSION = 4u,
    CD_IPC_OFFSET_HEADER_SIZE = 6u,
    CD_IPC_OFFSET_KIND = 8u,
    CD_IPC_OFFSET_FLAGS = 46u,
    CD_IPC_OFFSET_PAYLOAD_SIZE = 56u
};

typedef struct cd_ipc_socket_transport_impl {
    cd_context_t *context;
    cd_mutex_t *lock;
    int socket_fd;
    bool close_socket_on_shutdown;
    cd_ipc_codec_config_t codec_config;
    uint8_t *rx_buffer;
    size_t rx_capacity;
    size_t rx_count;
    uint8_t *tx_buffer;
    size_t tx_capacity;
    size_t tx_size;
    size_t tx_offset;
} cd_ipc_socket_transport_impl_t;

typedef enum cd_ipc_io_result {
    CD_IPC_IO_COMPLETE = 0,
    CD_IPC_IO_WOULD_BLOCK = 1,
    CD_IPC_IO_FAILED = 2
} cd_ipc_io_result_t;

static void ipc_socket_lock(cd_ipc_socket_transport_impl_t *state)
{
    if (state != NULL && state->lock != NULL) {
        cd_mutex_lock(state->lock);
    }
}

static void ipc_socket_unlock(cd_ipc_socket_transport_impl_t *state)
{
    if (state != NULL && state->lock != NULL) {
        cd_mutex_unlock(state->lock);
    }
}

static bool size_add_overflow(size_t a, size_t b, size_t *out_result)
{
    if (a > SIZE_MAX - b) {
        return true;
    }

    *out_result = a + b;
    return false;
}

static size_t normalize_max_payload_size(const cd_ipc_codec_config_t *config)
{
    if (config == NULL || config->max_payload_size == 0u) {
        return CD_IPC_DEFAULT_MAX_PAYLOAD_SIZE;
    }

    return config->max_payload_size;
}

static bool frame_flags_valid(uint16_t flags)
{
    return (flags & (uint16_t)~(CD_MESSAGE_FLAG_LOCAL_ONLY | CD_MESSAGE_FLAG_HIGH_PRIORITY)) == 0u;
}

static uint16_t read_u16_le(const uint8_t *buffer, size_t offset)
{
    return (uint16_t)(((uint16_t)buffer[offset + 0u]) |
                      ((uint16_t)buffer[offset + 1u] << 8u));
}

static uint32_t read_u32_le(const uint8_t *buffer, size_t offset)
{
    return ((uint32_t)buffer[offset + 0u]) |
           ((uint32_t)buffer[offset + 1u] << 8u) |
           ((uint32_t)buffer[offset + 2u] << 16u) |
           ((uint32_t)buffer[offset + 3u] << 24u);
}

static uint64_t read_u64_le(const uint8_t *buffer, size_t offset)
{
    return ((uint64_t)buffer[offset + 0u]) |
           ((uint64_t)buffer[offset + 1u] << 8u) |
           ((uint64_t)buffer[offset + 2u] << 16u) |
           ((uint64_t)buffer[offset + 3u] << 24u) |
           ((uint64_t)buffer[offset + 4u] << 32u) |
           ((uint64_t)buffer[offset + 5u] << 40u) |
           ((uint64_t)buffer[offset + 6u] << 48u) |
           ((uint64_t)buffer[offset + 7u] << 56u);
}

static ssize_t socket_send_bytes(int socket_fd, const uint8_t *buffer, size_t size)
{
#if defined(MSG_NOSIGNAL)
    return send(socket_fd, buffer, size, MSG_NOSIGNAL);
#else
    return send(socket_fd, buffer, size, 0);
#endif
}

static cd_ipc_io_result_t flush_pending_bytes(cd_ipc_socket_transport_impl_t *state)
{
    while (state->tx_offset < state->tx_size) {
        size_t remaining;
        ssize_t written;

        remaining = state->tx_size - state->tx_offset;
        written = socket_send_bytes(state->socket_fd, state->tx_buffer + state->tx_offset, remaining);
        if (written > 0) {
            state->tx_offset += (size_t)written;
            continue;
        }
        if (written == 0) {
            return CD_IPC_IO_FAILED;
        }

        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return CD_IPC_IO_WOULD_BLOCK;
        }
        return CD_IPC_IO_FAILED;
    }

    state->tx_size = 0u;
    state->tx_offset = 0u;
    return CD_IPC_IO_COMPLETE;
}

static void consume_rx_bytes(cd_ipc_socket_transport_impl_t *state, size_t consumed_size)
{
    size_t remaining;

    if (consumed_size == 0u || state->rx_count == 0u) {
        return;
    }
    if (consumed_size >= state->rx_count) {
        state->rx_count = 0u;
        return;
    }

    remaining = state->rx_count - consumed_size;
    memmove(state->rx_buffer, state->rx_buffer + consumed_size, remaining);
    state->rx_count = remaining;
}

static cd_status_t peek_frame_size(
    cd_ipc_socket_transport_impl_t *state,
    size_t *out_frame_size
)
{
    uint32_t kind_raw;
    uint64_t payload_size_u64;
    size_t payload_size;

    if (out_frame_size == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    *out_frame_size = 0u;

    if (state->rx_count < CD_IPC_FRAME_HEADER_SIZE) {
        return CD_STATUS_OK;
    }

    if (read_u32_le(state->rx_buffer, CD_IPC_OFFSET_MAGIC) != CD_IPC_PROTOCOL_MAGIC ||
        read_u16_le(state->rx_buffer, CD_IPC_OFFSET_VERSION) != CD_IPC_PROTOCOL_VERSION ||
        read_u16_le(state->rx_buffer, CD_IPC_OFFSET_HEADER_SIZE) != CD_IPC_FRAME_HEADER_SIZE) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }

    kind_raw = read_u32_le(state->rx_buffer, CD_IPC_OFFSET_KIND);
    if (kind_raw > (uint32_t)CD_MESSAGE_REPLY) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }

    if (!frame_flags_valid(read_u16_le(state->rx_buffer, CD_IPC_OFFSET_FLAGS))) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }

    payload_size_u64 = read_u64_le(state->rx_buffer, CD_IPC_OFFSET_PAYLOAD_SIZE);
    if (payload_size_u64 > (uint64_t)SIZE_MAX) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }
    payload_size = (size_t)payload_size_u64;

    return cd_ipc_frame_size_for_payload(payload_size, &state->codec_config, out_frame_size);
}

static cd_status_t deliver_one_buffered_frame(
    cd_ipc_socket_transport_impl_t *state,
    cd_transport_receive_fn receive_fn,
    void *receive_user_data,
    bool *out_delivered
)
{
    size_t frame_size;
    cd_envelope_t message;
    cd_status_t status;
    cd_status_t receive_status;

    if (out_delivered == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    *out_delivered = false;

    status = peek_frame_size(state, &frame_size);
    if (status != CD_STATUS_OK) {
        return status;
    }

    if (frame_size == 0u || state->rx_count < frame_size) {
        return CD_STATUS_OK;
    }

    memset(&message, 0, sizeof(message));
    status = cd_ipc_decode_envelope(state->rx_buffer, frame_size, &state->codec_config, &message);
    if (status != CD_STATUS_OK) {
        return status;
    }

    receive_status = receive_fn(receive_user_data, &message);
    consume_rx_bytes(state, frame_size);
    *out_delivered = true;

    if (receive_status != CD_STATUS_OK) {
        return receive_status;
    }

    return CD_STATUS_OK;
}

static cd_status_t ipc_socket_send(void *impl, const cd_envelope_t *message)
{
    cd_ipc_socket_transport_impl_t *state;
    cd_ipc_io_result_t io_result;
    cd_status_t status;

    state = (cd_ipc_socket_transport_impl_t *)impl;
    if (state == NULL || message == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    ipc_socket_lock(state);

    if (state->tx_size > 0u) {
        io_result = flush_pending_bytes(state);
        if (io_result == CD_IPC_IO_FAILED) {
            ipc_socket_unlock(state);
            return CD_STATUS_TRANSPORT_UNAVAILABLE;
        }
        if (io_result == CD_IPC_IO_WOULD_BLOCK) {
            ipc_socket_unlock(state);
            return CD_STATUS_CAPACITY_REACHED;
        }
    }

    status = cd_ipc_encode_envelope(
        message,
        &state->codec_config,
        state->tx_buffer,
        state->tx_capacity,
        &state->tx_size
    );
    if (status != CD_STATUS_OK) {
        state->tx_size = 0u;
        state->tx_offset = 0u;
        ipc_socket_unlock(state);
        return status;
    }
    state->tx_offset = 0u;

    io_result = flush_pending_bytes(state);
    if (io_result == CD_IPC_IO_FAILED) {
        state->tx_size = 0u;
        state->tx_offset = 0u;
        ipc_socket_unlock(state);
        return CD_STATUS_TRANSPORT_UNAVAILABLE;
    }
    if (io_result == CD_IPC_IO_WOULD_BLOCK) {
        ipc_socket_unlock(state);
        return CD_STATUS_OK;
    }

    ipc_socket_unlock(state);
    return CD_STATUS_OK;
}

static cd_status_t ipc_socket_poll(
    void *impl,
    cd_transport_receive_fn receive_fn,
    void *receive_user_data,
    size_t max_messages,
    size_t *out_polled_count
)
{
    cd_ipc_socket_transport_impl_t *state;
    size_t limit;
    size_t processed;
    cd_ipc_io_result_t io_result;

    state = (cd_ipc_socket_transport_impl_t *)impl;
    if (out_polled_count != NULL) {
        *out_polled_count = 0u;
    }

    if (state == NULL || receive_fn == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    ipc_socket_lock(state);

    limit = max_messages;
    if (limit == 0u) {
        limit = SIZE_MAX;
    }
    processed = 0u;

    io_result = flush_pending_bytes(state);
    if (io_result == CD_IPC_IO_FAILED) {
        ipc_socket_unlock(state);
        return CD_STATUS_TRANSPORT_UNAVAILABLE;
    }

    while (processed < limit) {
        bool delivered;
        ssize_t received;
        cd_status_t status;

        while (processed < limit) {
            delivered = false;
            status = deliver_one_buffered_frame(state, receive_fn, receive_user_data, &delivered);
            if (status != CD_STATUS_OK) {
                if (out_polled_count != NULL) {
                    *out_polled_count = processed;
                }
                ipc_socket_unlock(state);
                return status;
            }
            if (!delivered) {
                break;
            }
            processed += 1u;
        }

        if (processed >= limit) {
            break;
        }

        if (state->rx_count == state->rx_capacity) {
            if (out_polled_count != NULL) {
                *out_polled_count = processed;
            }
            ipc_socket_unlock(state);
            return CD_STATUS_SCHEMA_MISMATCH;
        }

        received = recv(
            state->socket_fd,
            state->rx_buffer + state->rx_count,
            state->rx_capacity - state->rx_count,
            0
        );
        if (received > 0) {
            state->rx_count += (size_t)received;
            continue;
        }
        if (received == 0) {
            if (out_polled_count != NULL) {
                *out_polled_count = processed;
            }
            ipc_socket_unlock(state);
            return CD_STATUS_TRANSPORT_UNAVAILABLE;
        }

        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }

        if (out_polled_count != NULL) {
            *out_polled_count = processed;
        }
        ipc_socket_unlock(state);
        return CD_STATUS_TRANSPORT_UNAVAILABLE;
    }

    if (out_polled_count != NULL) {
        *out_polled_count = processed;
    }
    ipc_socket_unlock(state);
    return CD_STATUS_OK;
}

static cd_status_t ipc_socket_flush(void *impl)
{
    cd_ipc_socket_transport_impl_t *state;
    cd_ipc_io_result_t io_result;

    state = (cd_ipc_socket_transport_impl_t *)impl;
    if (state == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    ipc_socket_lock(state);
    io_result = flush_pending_bytes(state);
    if (io_result == CD_IPC_IO_FAILED) {
        ipc_socket_unlock(state);
        return CD_STATUS_TRANSPORT_UNAVAILABLE;
    }
    if (io_result == CD_IPC_IO_WOULD_BLOCK) {
        ipc_socket_unlock(state);
        return CD_STATUS_CAPACITY_REACHED;
    }

    ipc_socket_unlock(state);
    return CD_STATUS_OK;
}

static void ipc_socket_shutdown(void *impl)
{
    cd_ipc_socket_transport_impl_t *state;
    cd_context_t *context;

    state = (cd_ipc_socket_transport_impl_t *)impl;
    if (state == NULL) {
        return;
    }

    if (state->close_socket_on_shutdown && state->socket_fd >= 0) {
        close(state->socket_fd);
    }

    context = state->context;
    if (state->tx_buffer != NULL) {
        cd_context_free(context, state->tx_buffer);
    }
    if (state->rx_buffer != NULL) {
        cd_context_free(context, state->rx_buffer);
    }
    if (state->lock != NULL) {
        cd_mutex_destroy(context, state->lock);
    }
    cd_context_free(context, state);
}

cd_status_t cd_ipc_socket_transport_init(
    cd_context_t *context,
    int socket_fd,
    const cd_ipc_socket_transport_config_t *config,
    cd_transport_t *out_transport
)
{
    cd_ipc_socket_transport_impl_t *state;
    size_t max_payload_size;
    size_t frame_capacity;
    int flags;

    if (context == NULL || socket_fd < 0 || out_transport == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    memset(out_transport, 0, sizeof(*out_transport));

    max_payload_size = normalize_max_payload_size(config == NULL ? NULL : &config->codec);
    if (size_add_overflow(CD_IPC_FRAME_HEADER_SIZE, max_payload_size, &frame_capacity)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    state = (cd_ipc_socket_transport_impl_t *)cd_context_alloc(context, sizeof(*state));
    if (state == NULL) {
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(state, 0, sizeof(*state));

    state->context = context;
    state->socket_fd = socket_fd;
    state->close_socket_on_shutdown = (config == NULL || config->keep_socket_open == 0);
    state->codec_config.max_payload_size = max_payload_size;
    state->rx_capacity = frame_capacity;
    state->tx_capacity = frame_capacity;
    if (cd_mutex_create(context, &state->lock) != CD_STATUS_OK) {
        cd_context_free(context, state);
        return CD_STATUS_ALLOCATION_FAILED;
    }

    state->rx_buffer = (uint8_t *)cd_context_alloc(context, state->rx_capacity);
    if (state->rx_buffer == NULL) {
        cd_mutex_destroy(context, state->lock);
        cd_context_free(context, state);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(state->rx_buffer, 0, state->rx_capacity);

    state->tx_buffer = (uint8_t *)cd_context_alloc(context, state->tx_capacity);
    if (state->tx_buffer == NULL) {
        cd_context_free(context, state->rx_buffer);
        cd_mutex_destroy(context, state->lock);
        cd_context_free(context, state);
        return CD_STATUS_ALLOCATION_FAILED;
    }
    memset(state->tx_buffer, 0, state->tx_capacity);

    if (config == NULL || config->disable_nonblocking == 0) {
        flags = fcntl(socket_fd, F_GETFL, 0);
        if (flags < 0 || fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            cd_context_free(context, state->tx_buffer);
            cd_context_free(context, state->rx_buffer);
            cd_mutex_destroy(context, state->lock);
            cd_context_free(context, state);
            return CD_STATUS_TRANSPORT_UNAVAILABLE;
        }
    }

#ifdef SO_NOSIGPIPE
    {
        int one;

        one = 1;
        (void)setsockopt(socket_fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
    }
#endif

    out_transport->impl = state;
    out_transport->send = ipc_socket_send;
    out_transport->poll = ipc_socket_poll;
    out_transport->flush = ipc_socket_flush;
    out_transport->shutdown = ipc_socket_shutdown;
    return CD_STATUS_OK;
}

void cd_ipc_socket_transport_close(cd_transport_t *transport)
{
    if (transport == NULL) {
        return;
    }

    if (transport->shutdown != NULL) {
        transport->shutdown(transport->impl);
    }
    memset(transport, 0, sizeof(*transport));
}
