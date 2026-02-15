#ifndef CONDUIT_TRANSPORT_H
#define CONDUIT_TRANSPORT_H

#include "conduit/types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cd_transport cd_transport_t;

typedef cd_status_t (*cd_transport_receive_fn)(void *user_data, const cd_envelope_t *message);

typedef cd_status_t (*cd_transport_send_fn)(void *impl, const cd_envelope_t *message);
typedef cd_status_t (*cd_transport_poll_fn)(
    void *impl,
    cd_transport_receive_fn receive_fn,
    void *receive_user_data,
    size_t max_messages,
    size_t *out_polled_count
);
typedef cd_status_t (*cd_transport_flush_fn)(void *impl);
typedef void (*cd_transport_shutdown_fn)(void *impl);

struct cd_transport {
    void *impl;
    cd_transport_send_fn send;
    cd_transport_poll_fn poll;
    cd_transport_flush_fn flush;
    cd_transport_shutdown_fn shutdown;
};

#ifdef __cplusplus
}
#endif

#endif
