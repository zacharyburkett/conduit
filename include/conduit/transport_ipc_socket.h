#ifndef CONDUIT_TRANSPORT_IPC_SOCKET_H
#define CONDUIT_TRANSPORT_IPC_SOCKET_H

#include "conduit/context.h"
#include "conduit/transport.h"
#include "conduit/transport_ipc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cd_ipc_socket_transport_config {
    cd_ipc_codec_config_t codec;
    /*
     * When non-zero, leaves socket in blocking mode.
     * Default behavior sets O_NONBLOCK during init.
     */
    int disable_nonblocking;
    /*
     * When non-zero, transport shutdown does not close socket_fd.
     * Default behavior closes socket_fd.
     */
    int keep_socket_open;
} cd_ipc_socket_transport_config_t;

/*
 * Initializes an IPC socket transport over socket_fd.
 * out_transport receives function pointers and impl state by value.
 */
cd_status_t cd_ipc_socket_transport_init(
    cd_context_t *context,
    int socket_fd,
    const cd_ipc_socket_transport_config_t *config,
    cd_transport_t *out_transport
);

/*
 * Releases transport state created by cd_ipc_socket_transport_init.
 * Safe to call with a zero-initialized transport.
 */
void cd_ipc_socket_transport_close(cd_transport_t *transport);

#ifdef __cplusplus
}
#endif

#endif
