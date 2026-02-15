#include "internal.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <pthread.h>
#endif

struct cd_mutex {
#if defined(_WIN32)
    CRITICAL_SECTION cs;
#else
    pthread_mutex_t mutex;
#endif
};

cd_status_t cd_mutex_create(cd_context_t *context, cd_mutex_t **out_mutex)
{
    cd_mutex_t *mutex;

    if (context == NULL || out_mutex == NULL) {
        return CD_STATUS_INVALID_ARGUMENT;
    }

    *out_mutex = NULL;
    mutex = (cd_mutex_t *)cd_context_alloc(context, sizeof(*mutex));
    if (mutex == NULL) {
        return CD_STATUS_ALLOCATION_FAILED;
    }

#if defined(_WIN32)
    InitializeCriticalSection(&mutex->cs);
#else
    {
        pthread_mutexattr_t attr;

        if (pthread_mutexattr_init(&attr) != 0) {
            cd_context_free(context, mutex);
            return CD_STATUS_ALLOCATION_FAILED;
        }
        if (pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) != 0 ||
            pthread_mutex_init(&mutex->mutex, &attr) != 0) {
            pthread_mutexattr_destroy(&attr);
            cd_context_free(context, mutex);
            return CD_STATUS_ALLOCATION_FAILED;
        }
        pthread_mutexattr_destroy(&attr);
    }
#endif

    *out_mutex = mutex;
    return CD_STATUS_OK;
}

void cd_mutex_destroy(cd_context_t *context, cd_mutex_t *mutex)
{
    if (context == NULL || mutex == NULL) {
        return;
    }

#if defined(_WIN32)
    DeleteCriticalSection(&mutex->cs);
#else
    pthread_mutex_destroy(&mutex->mutex);
#endif
    cd_context_free(context, mutex);
}

void cd_mutex_lock(cd_mutex_t *mutex)
{
    if (mutex == NULL) {
        return;
    }

#if defined(_WIN32)
    EnterCriticalSection(&mutex->cs);
#else
    (void)pthread_mutex_lock(&mutex->mutex);
#endif
}

void cd_mutex_unlock(cd_mutex_t *mutex)
{
    if (mutex == NULL) {
        return;
    }

#if defined(_WIN32)
    LeaveCriticalSection(&mutex->cs);
#else
    (void)pthread_mutex_unlock(&mutex->mutex);
#endif
}
