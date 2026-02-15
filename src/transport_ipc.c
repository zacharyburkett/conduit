#include "conduit/transport_ipc.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

enum {
    CD_IPC_PROTOCOL_MAGIC = 0x43444950u /* "CDIP" */
};

enum {
    CD_IPC_OFFSET_MAGIC = 0u,
    CD_IPC_OFFSET_VERSION = 4u,
    CD_IPC_OFFSET_HEADER_SIZE = 6u,
    CD_IPC_OFFSET_KIND = 8u,
    CD_IPC_OFFSET_TOPIC = 12u,
    CD_IPC_OFFSET_SOURCE_ENDPOINT = 16u,
    CD_IPC_OFFSET_TARGET_ENDPOINT = 20u,
    CD_IPC_OFFSET_MESSAGE_ID = 24u,
    CD_IPC_OFFSET_CORRELATION_ID = 32u,
    CD_IPC_OFFSET_SCHEMA_ID = 40u,
    CD_IPC_OFFSET_SCHEMA_VERSION = 44u,
    CD_IPC_OFFSET_FLAGS = 46u,
    CD_IPC_OFFSET_TIMESTAMP_NS = 48u,
    CD_IPC_OFFSET_PAYLOAD_SIZE = 56u
};

static bool size_add_overflow(size_t a, size_t b, size_t *out_result)
{
    if (a > SIZE_MAX - b) {
        return true;
    }

    *out_result = a + b;
    return false;
}

static bool kind_valid(cd_message_kind_t kind)
{
    switch (kind) {
    case CD_MESSAGE_EVENT:
    case CD_MESSAGE_COMMAND:
    case CD_MESSAGE_REQUEST:
    case CD_MESSAGE_REPLY:
        return true;
    default:
        return false;
    }
}

static bool flags_valid(uint16_t flags)
{
    return (flags & (uint16_t)~(CD_MESSAGE_FLAG_LOCAL_ONLY | CD_MESSAGE_FLAG_HIGH_PRIORITY)) == 0u;
}

static size_t max_payload_size_from_config(const cd_ipc_codec_config_t *config)
{
    if (config == NULL || config->max_payload_size == 0u) {
        return CD_IPC_DEFAULT_MAX_PAYLOAD_SIZE;
    }

    return config->max_payload_size;
}

static void write_u16_le(uint8_t *buffer, size_t offset, uint16_t value)
{
    buffer[offset + 0u] = (uint8_t)(value & 0xFFu);
    buffer[offset + 1u] = (uint8_t)((value >> 8u) & 0xFFu);
}

static void write_u32_le(uint8_t *buffer, size_t offset, uint32_t value)
{
    buffer[offset + 0u] = (uint8_t)(value & 0xFFu);
    buffer[offset + 1u] = (uint8_t)((value >> 8u) & 0xFFu);
    buffer[offset + 2u] = (uint8_t)((value >> 16u) & 0xFFu);
    buffer[offset + 3u] = (uint8_t)((value >> 24u) & 0xFFu);
}

static void write_u64_le(uint8_t *buffer, size_t offset, uint64_t value)
{
    buffer[offset + 0u] = (uint8_t)(value & 0xFFu);
    buffer[offset + 1u] = (uint8_t)((value >> 8u) & 0xFFu);
    buffer[offset + 2u] = (uint8_t)((value >> 16u) & 0xFFu);
    buffer[offset + 3u] = (uint8_t)((value >> 24u) & 0xFFu);
    buffer[offset + 4u] = (uint8_t)((value >> 32u) & 0xFFu);
    buffer[offset + 5u] = (uint8_t)((value >> 40u) & 0xFFu);
    buffer[offset + 6u] = (uint8_t)((value >> 48u) & 0xFFu);
    buffer[offset + 7u] = (uint8_t)((value >> 56u) & 0xFFu);
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

cd_status_t cd_ipc_frame_size_for_payload(
    size_t payload_size,
    const cd_ipc_codec_config_t *config,
    size_t *out_frame_size
)
{
    size_t max_payload_size;
    size_t frame_size;

    if (out_frame_size == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    *out_frame_size = 0u;

    max_payload_size = max_payload_size_from_config(config);
    if (payload_size > max_payload_size) {
        return CD_STATUS_CAPACITY_REACHED;
    }

    if (size_add_overflow(CD_IPC_FRAME_HEADER_SIZE, payload_size, &frame_size)) {
        return CD_STATUS_CAPACITY_REACHED;
    }

    *out_frame_size = frame_size;
    return CD_STATUS_OK;
}

cd_status_t cd_ipc_encode_envelope(
    const cd_envelope_t *message,
    const cd_ipc_codec_config_t *config,
    void *out_frame,
    size_t out_frame_capacity,
    size_t *out_frame_size
)
{
    size_t frame_size;
    cd_status_t status;
    uint8_t *buffer;

    if (out_frame_size != NULL) {
        *out_frame_size = 0u;
    }

    if (message == NULL || out_frame == NULL ||
        (message->payload_size > 0u && message->payload == NULL) ||
        !kind_valid(message->kind) ||
        !flags_valid(message->flags)) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    status = cd_ipc_frame_size_for_payload(message->payload_size, config, &frame_size);
    if (status != CD_STATUS_OK) {
        return status;
    }
    if (out_frame_capacity < frame_size) {
        return CD_STATUS_CAPACITY_REACHED;
    }

    buffer = (uint8_t *)out_frame;
    write_u32_le(buffer, CD_IPC_OFFSET_MAGIC, CD_IPC_PROTOCOL_MAGIC);
    write_u16_le(buffer, CD_IPC_OFFSET_VERSION, CD_IPC_PROTOCOL_VERSION);
    write_u16_le(buffer, CD_IPC_OFFSET_HEADER_SIZE, CD_IPC_FRAME_HEADER_SIZE);
    write_u32_le(buffer, CD_IPC_OFFSET_KIND, (uint32_t)message->kind);
    write_u32_le(buffer, CD_IPC_OFFSET_TOPIC, message->topic);
    write_u32_le(buffer, CD_IPC_OFFSET_SOURCE_ENDPOINT, message->source_endpoint);
    write_u32_le(buffer, CD_IPC_OFFSET_TARGET_ENDPOINT, message->target_endpoint);
    write_u64_le(buffer, CD_IPC_OFFSET_MESSAGE_ID, message->message_id);
    write_u64_le(buffer, CD_IPC_OFFSET_CORRELATION_ID, message->correlation_id);
    write_u32_le(buffer, CD_IPC_OFFSET_SCHEMA_ID, message->schema_id);
    write_u16_le(buffer, CD_IPC_OFFSET_SCHEMA_VERSION, message->schema_version);
    write_u16_le(buffer, CD_IPC_OFFSET_FLAGS, message->flags);
    write_u64_le(buffer, CD_IPC_OFFSET_TIMESTAMP_NS, message->timestamp_ns);
    write_u64_le(buffer, CD_IPC_OFFSET_PAYLOAD_SIZE, (uint64_t)message->payload_size);

    if (message->payload_size > 0u) {
        memcpy(buffer + CD_IPC_FRAME_HEADER_SIZE, message->payload, message->payload_size);
    }

    if (out_frame_size != NULL) {
        *out_frame_size = frame_size;
    }
    return CD_STATUS_OK;
}

cd_status_t cd_ipc_decode_envelope(
    const void *frame,
    size_t frame_size,
    const cd_ipc_codec_config_t *config,
    cd_envelope_t *out_message
)
{
    const uint8_t *buffer;
    uint32_t kind_raw;
    uint64_t payload_size_u64;
    size_t payload_size;
    size_t expected_frame_size;
    cd_status_t status;

    if (frame == NULL || out_message == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }
    memset(out_message, 0, sizeof(*out_message));

    if (frame_size < CD_IPC_FRAME_HEADER_SIZE) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }

    buffer = (const uint8_t *)frame;
    if (read_u32_le(buffer, CD_IPC_OFFSET_MAGIC) != CD_IPC_PROTOCOL_MAGIC) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }
    if (read_u16_le(buffer, CD_IPC_OFFSET_VERSION) != CD_IPC_PROTOCOL_VERSION) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }
    if (read_u16_le(buffer, CD_IPC_OFFSET_HEADER_SIZE) != CD_IPC_FRAME_HEADER_SIZE) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }

    kind_raw = read_u32_le(buffer, CD_IPC_OFFSET_KIND);
    if (kind_raw > (uint32_t)CD_MESSAGE_REPLY) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }
    if (!flags_valid(read_u16_le(buffer, CD_IPC_OFFSET_FLAGS))) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }

    payload_size_u64 = read_u64_le(buffer, CD_IPC_OFFSET_PAYLOAD_SIZE);
    if (payload_size_u64 > (uint64_t)SIZE_MAX) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }
    payload_size = (size_t)payload_size_u64;

    status = cd_ipc_frame_size_for_payload(payload_size, config, &expected_frame_size);
    if (status != CD_STATUS_OK) {
        return status;
    }
    if (frame_size != expected_frame_size) {
        return CD_STATUS_SCHEMA_MISMATCH;
    }

    out_message->message_id = read_u64_le(buffer, CD_IPC_OFFSET_MESSAGE_ID);
    out_message->correlation_id = read_u64_le(buffer, CD_IPC_OFFSET_CORRELATION_ID);
    out_message->kind = (cd_message_kind_t)kind_raw;
    out_message->topic = read_u32_le(buffer, CD_IPC_OFFSET_TOPIC);
    out_message->source_endpoint = read_u32_le(buffer, CD_IPC_OFFSET_SOURCE_ENDPOINT);
    out_message->target_endpoint = read_u32_le(buffer, CD_IPC_OFFSET_TARGET_ENDPOINT);
    out_message->schema_id = read_u32_le(buffer, CD_IPC_OFFSET_SCHEMA_ID);
    out_message->schema_version = read_u16_le(buffer, CD_IPC_OFFSET_SCHEMA_VERSION);
    out_message->flags = read_u16_le(buffer, CD_IPC_OFFSET_FLAGS);
    out_message->timestamp_ns = read_u64_le(buffer, CD_IPC_OFFSET_TIMESTAMP_NS);
    out_message->payload_size = payload_size;
    out_message->payload = NULL;
    if (payload_size > 0u) {
        out_message->payload = buffer + CD_IPC_FRAME_HEADER_SIZE;
    }

    return CD_STATUS_OK;
}
