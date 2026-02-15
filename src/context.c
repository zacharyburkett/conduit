#include "internal.h"

#include <stdlib.h>
#include <time.h>

static void *default_alloc(void *user_data, size_t size)
{
    (void)user_data;
    return malloc(size);
}

static void default_free(void *user_data, void *ptr)
{
    (void)user_data;
    free(ptr);
}

static uint64_t default_now_ns(void *user_data)
{
    struct timespec now;
    (void)user_data;

    timespec_get(&now, TIME_UTC);
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

cd_status_t cd_context_init(cd_context_t **out_context, const cd_context_config_t *config)
{
    cd_alloc_fn alloc_fn;
    cd_free_fn free_fn;
    cd_now_ns_fn now_ns_fn;
    void *alloc_user_data;
    void *clock_user_data;
    cd_context_t *context;

    if (out_context == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    alloc_fn = default_alloc;
    free_fn = default_free;
    now_ns_fn = default_now_ns;
    alloc_user_data = NULL;
    clock_user_data = NULL;

    if (config != NULL) {
        if ((config->allocator.alloc == NULL) != (config->allocator.free == NULL)) {
            return CD_STATUS_INVALID_ARGUMENT;
        }

        if (config->allocator.alloc != NULL) {
            alloc_fn = config->allocator.alloc;
            free_fn = config->allocator.free;
            alloc_user_data = config->allocator.user_data;
        }

        if (config->clock.now_ns != NULL) {
            now_ns_fn = config->clock.now_ns;
            clock_user_data = config->clock.user_data;
        }
    }

    context = (cd_context_t *)alloc_fn(alloc_user_data, sizeof(*context));
    if (context == NULL) {
        return CD_STATUS_ALLOCATION_FAILED;
    }

    context->alloc_fn = alloc_fn;
    context->free_fn = free_fn;
    context->alloc_user_data = alloc_user_data;
    context->now_ns_fn = now_ns_fn;
    context->clock_user_data = clock_user_data;

    *out_context = context;
    return CD_STATUS_OK;
}

void cd_context_shutdown(cd_context_t *context)
{
    if (context == NULL) {
        return;
    }

    context->free_fn(context->alloc_user_data, context);
}

void *cd_context_alloc(cd_context_t *context, size_t size)
{
    return context->alloc_fn(context->alloc_user_data, size);
}

void cd_context_free(cd_context_t *context, void *ptr)
{
    if (ptr == NULL) {
        return;
    }

    context->free_fn(context->alloc_user_data, ptr);
}

uint64_t cd_context_now_ns(const cd_context_t *context)
{
    return context->now_ns_fn(context->clock_user_data);
}
