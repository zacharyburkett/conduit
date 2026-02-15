#ifndef CONDUIT_TRANSPORT_INPROC_H
#define CONDUIT_TRANSPORT_INPROC_H

#include "conduit/context.h"
#include "conduit/transport.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cd_inproc_hub cd_inproc_hub_t;

typedef struct cd_inproc_hub_config {
    /* 0 uses implementation default. */
    size_t max_peers;
    /* 0 uses implementation default. */
    size_t max_queued_messages_per_peer;
} cd_inproc_hub_config_t;

cd_status_t cd_inproc_hub_init(
    cd_context_t *context,
    const cd_inproc_hub_config_t *config,
    cd_inproc_hub_t **out_hub
);

void cd_inproc_hub_shutdown(cd_inproc_hub_t *hub);

/*
 * Creates a transport endpoint bound to this hub.
 * out_transport receives function pointers and impl state by value.
 */
cd_status_t cd_inproc_hub_create_transport(cd_inproc_hub_t *hub, cd_transport_t *out_transport);

/*
 * Releases a transport endpoint created by cd_inproc_hub_create_transport.
 * Safe to call with a zero-initialized transport.
 */
void cd_inproc_transport_close(cd_transport_t *transport);

#ifdef __cplusplus
}
#endif

#endif
