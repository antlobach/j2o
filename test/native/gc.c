/* Observe the real production join's condition wait, without changing its
 * result, deadline or synchronization. This library is only for the GC probe. */
#include <pthread.h>
#include <stdatomic.h>
static _Atomic int waiters;
static int measured_wait(pthread_cond_t *condition, pthread_mutex_t *mutex, const struct timespec *deadline)
{
    atomic_fetch_add(&waiters, 1);
    int result = pthread_cond_timedwait(condition, mutex, deadline);
    atomic_fetch_sub(&waiters, 1);
    return result;
}
#define pthread_cond_timedwait measured_wait
#include "../../native/j2o_bridge.c"
#undef pthread_cond_timedwait
J2O_API int j2o_test_join_waiters(void) { return atomic_load(&waiters); }
