#ifndef CONDUIT_CONTEXT_H
#define CONDUIT_CONTEXT_H

#include "conduit/types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cd_context cd_context_t;

typedef void *(*cd_alloc_fn)(void *user_data, size_t size);
typedef void (*cd_free_fn)(void *user_data, void *ptr);
typedef uint64_t (*cd_now_ns_fn)(void *user_data);

typedef struct cd_allocator {
    cd_alloc_fn alloc;
    cd_free_fn free;
    void *user_data;
} cd_allocator_t;

typedef struct cd_clock {
    cd_now_ns_fn now_ns;
    void *user_data;
} cd_clock_t;

typedef struct cd_context_config {
    cd_allocator_t allocator;
    cd_clock_t clock;
} cd_context_config_t;

/*
 * Initialize a conduit context.
 *
 * If config is NULL, default allocator (malloc/free) and clock are used.
 * When custom allocator callbacks are provided, both alloc and free must be set.
 */
cd_status_t cd_context_init(cd_context_t **out_context, const cd_context_config_t *config);

/*
 * Shutdown and free a context initialized by cd_context_init.
 */
void cd_context_shutdown(cd_context_t *context);

#ifdef __cplusplus
}
#endif

#endif
