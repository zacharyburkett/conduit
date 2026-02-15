#ifndef CONDUIT_TYPES_H
#define CONDUIT_TYPES_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t cd_topic_t;
typedef uint32_t cd_endpoint_id_t;
typedef uint64_t cd_message_id_t;
typedef uint64_t cd_request_token_t;
typedef uint32_t cd_subscription_id_t;

enum {
    CD_ENDPOINT_NONE = 0u
};

typedef enum cd_status {
    CD_STATUS_OK = 0,
    CD_STATUS_INVALID_ARGUMENT = 1,
    CD_STATUS_ALLOCATION_FAILED = 2,
    CD_STATUS_QUEUE_FULL = 3,
    CD_STATUS_CAPACITY_REACHED = 4,
    CD_STATUS_NOT_FOUND = 5,
    CD_STATUS_TIMEOUT = 6,
    CD_STATUS_TRANSPORT_UNAVAILABLE = 7,
    CD_STATUS_SCHEMA_MISMATCH = 8,
    CD_STATUS_NOT_IMPLEMENTED = 9
} cd_status_t;

typedef enum cd_message_kind {
    CD_MESSAGE_EVENT = 0,
    CD_MESSAGE_COMMAND = 1,
    CD_MESSAGE_REQUEST = 2,
    CD_MESSAGE_REPLY = 3
} cd_message_kind_t;

enum {
    CD_MESSAGE_FLAG_NONE = 0u,
    CD_MESSAGE_FLAG_LOCAL_ONLY = 1u << 0,
    CD_MESSAGE_FLAG_HIGH_PRIORITY = 1u << 1
};

#define CD_MESSAGE_KIND_MASK(kind) (1u << (unsigned)(kind))
#define CD_MESSAGE_KIND_ALL_MASK   (CD_MESSAGE_KIND_MASK(CD_MESSAGE_EVENT) | \
                                    CD_MESSAGE_KIND_MASK(CD_MESSAGE_COMMAND) | \
                                    CD_MESSAGE_KIND_MASK(CD_MESSAGE_REQUEST) | \
                                    CD_MESSAGE_KIND_MASK(CD_MESSAGE_REPLY))

typedef struct cd_envelope {
    cd_message_id_t message_id;
    cd_message_id_t correlation_id;
    cd_message_kind_t kind;
    cd_topic_t topic;
    cd_endpoint_id_t source_endpoint;
    cd_endpoint_id_t target_endpoint;
    uint32_t schema_id;
    uint16_t schema_version;
    uint16_t flags;
    uint64_t timestamp_ns;
    const void *payload;
    size_t payload_size;
} cd_envelope_t;

const char *cd_status_string(cd_status_t status);

#ifdef __cplusplus
}
#endif

#endif
