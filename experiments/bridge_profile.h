#ifndef J2O_BRIDGE_PROFILE_H
#define J2O_BRIDGE_PROFILE_H

_Static_assert(ATOMIC_INT_LOCK_FREE == 2 && ATOMIC_LONG_LOCK_FREE == 2 &&
               ATOMIC_LLONG_LOCK_FREE == 2 && ATOMIC_POINTER_LOCK_FREE == 2,
               "Profiling scalar FFI requires lock-free native atomics");
/* Profiling stores native scalars only. No callback into Chez is permitted. */
enum profile_stage {
    P_ARRIVAL, P_QUEUED, P_DEQUEUED, P_DECODE_START, P_DECODED,
    P_FIBER_ENQUEUED, P_HANDLER_START, P_HANDLER_RETURN, P_ENCODED,
    P_SUBMIT_READY, P_SUBMIT_ENTER, P_RESPONSE_QUEUED, P_RESPONSE_RECEIVED,
    P_SEND_RETURN, P_STAGE_COUNT
};
static const char *profile_stage_names[P_STAGE_COUNT] = {
    "native-arrival", "native-queued", "native-dequeued", "decode-start", "request-decoded",
    "fiber-enqueued", "handler-start", "handler-return", "response-encoded",
    "submit-ready", "native-submit-enter", "response-queued", "response-received", "native-send-return"
};
#define PROFILE_WORDS (1 + P_STAGE_COUNT * 3 + 2)
#define PROFILE_ALL_STAGES ((UINT32_C(1) << P_STAGE_COUNT) - 1)
#define PROFILE_PACKET_BYTES (1 + P_STAGE_COUNT * 3)
#define PROFILE_RESPONSE_BYTES (PROFILE_PACKET_BYTES + 1)

typedef struct {
    /* id, wall-clock nanoseconds, managed bytes, collections, known native bytes */
    uint64_t data[PROFILE_WORDS];
    _Atomic uint32_t seen;
} profile_trace;

typedef struct {
    size_t capacity;
    unsigned stride;
    _Atomic int enabled;
    _Atomic uint64_t considered, selected, dropped, last_id;
    profile_trace *records;
} profile_state;

static uint64_t profile_now(void)
{
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) abort();
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static void profile_stamp(profile_trace *trace, unsigned stage, uint64_t allocated, uint64_t collections)
{
    if (trace == NULL) return;
    assert(stage < P_STAGE_COUNT);
    /* Failed/competing replies do not form a successful serial trace. Freeze
     * its data after completion or invalidation, including late handler returns. */
    uint32_t expected = (UINT32_C(1) << stage) - 1;
    if (atomic_load_explicit(&trace->seen, memory_order_acquire) != expected) {
        atomic_fetch_or_explicit(&trace->seen, UINT32_C(1) << 31, memory_order_release);
        return;
    }
    trace->data[1 + stage] = profile_now();
    trace->data[1 + P_STAGE_COUNT + stage] = allocated;
    trace->data[1 + P_STAGE_COUNT * 2 + stage] = collections;
    atomic_fetch_or_explicit(&trace->seen, UINT32_C(1) << stage, memory_order_release);
}

static profile_trace *profile_select(profile_state *state, uint64_t id)
{
    if (state == NULL || !atomic_load_explicit(&state->enabled, memory_order_acquire)) return NULL;
    atomic_fetch_add(&state->considered, 1);
    atomic_store(&state->last_id, id);
    /* Deterministic mixing avoids sampling the same low-bit periodic phase. */
    uint64_t mixed = id + UINT64_C(0x9e3779b97f4a7c15);
    mixed = (mixed ^ (mixed >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)) * UINT64_C(0x94d049bb133111eb);
    mixed ^= mixed >> 31;
    if ((mixed & (state->stride - 1)) != 0) return NULL;
    uint64_t index = atomic_fetch_add(&state->selected, 1);
    if (index >= state->capacity) {
        atomic_fetch_add(&state->dropped, 1);
        return NULL;
    }
    profile_trace *trace = state->records + index;
    trace->data[0] = id;
    profile_stamp(trace, P_ARRIVAL, 0, 0);
    return trace;
}

static void profile_dispose(profile_state *state)
{
    if (state == NULL) return;
    free(state->records);
    free(state);
}
#endif
