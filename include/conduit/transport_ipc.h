#ifndef CONDUIT_TRANSPORT_IPC_H
#define CONDUIT_TRANSPORT_IPC_H

#include "conduit/types.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
    CD_IPC_PROTOCOL_VERSION = 1u,
    CD_IPC_FRAME_HEADER_SIZE = 64u,
    CD_IPC_DEFAULT_MAX_PAYLOAD_SIZE = 1024u * 1024u
};

typedef struct cd_ipc_codec_config {
    /* 0 uses CD_IPC_DEFAULT_MAX_PAYLOAD_SIZE. */
    size_t max_payload_size;
} cd_ipc_codec_config_t;

/*
 * Computes the exact frame size required for a payload under active limits.
 */
cd_status_t cd_ipc_frame_size_for_payload(
    size_t payload_size,
    const cd_ipc_codec_config_t *config,
    size_t *out_frame_size
);

/*
 * Encodes a message envelope into a fixed-header + payload frame.
 *
 * Returns CD_STATUS_CAPACITY_REACHED when out_frame_capacity is too small.
 */
cd_status_t cd_ipc_encode_envelope(
    const cd_envelope_t *message,
    const cd_ipc_codec_config_t *config,
    void *out_frame,
    size_t out_frame_capacity,
    size_t *out_frame_size
);

/*
 * Decodes a single frame into envelope fields.
 *
 * On success, out_message->payload points into frame memory and remains valid
 * only while that frame buffer remains alive.
 */
cd_status_t cd_ipc_decode_envelope(
    const void *frame,
    size_t frame_size,
    const cd_ipc_codec_config_t *config,
    cd_envelope_t *out_message
);

#ifdef __cplusplus
}
#endif

#endif
