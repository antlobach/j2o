#ifndef J2O_BRIDGE_H
#define J2O_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#if defined(__GNUC__)
#define J2O_API __attribute__((visibility("default")))
#else
#define J2O_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Development revisions require an exact managed/native match. Not release ABI v1. */
#define J2O_ABI_VERSION UINT32_C(0x80000002)

typedef enum {
    J2O_OK = 0, J2O_INVALID = 1, J2O_NOMEM = 2, J2O_OS = 3,
    J2O_UNSUPPORTED = 4, J2O_QUEUE_FULL = 5, J2O_STOPPED = 6, J2O_STALE = 7
} j2o_result;

typedef enum {
    J2O_HOST = 1, J2O_PORT = 2, J2O_REUSE_PORT = 3,
    J2O_MAX_REQUEST_BYTES = 4, J2O_MAX_HEADER_BYTES = 5,
    J2O_MAX_LIVE_REQUESTS = 6, J2O_MAX_EVENT_QUEUE = 7,
    J2O_MAX_RESPONSE_QUEUE = 8, J2O_MAX_QUEUED_BYTES = 9,
    J2O_MAX_CONNECTIONS = 10, J2O_KEEPALIVE_TIMEOUT_MS = 11,
    J2O_REQUEST_TIMEOUT_MS = 12, J2O_WRITE_TIMEOUT_MS = 13,
    J2O_HANDSHAKE_TIMEOUT_MS = 14
} j2o_config_key;

typedef struct j2o_config j2o_config;
typedef struct j2o_server j2o_server;
typedef struct j2o_event j2o_event;
typedef struct j2o_response j2o_response;

typedef struct {
    uint32_t loop;
    uint32_t slot;
    uint64_t generation;
} j2o_id;

typedef struct {
    uint32_t abi_size;
    int32_t result;
    int32_t os_errno;
    char message[256];
    char syscall[32];
    char errno_name[32];
    char key[64];
    char given[128];
} j2o_error;

/* Every fallible call requires caller-owned error storage. Success zeroes it.
 * Failure sets abi_size and bounded NUL-terminated copies; no field aliases TLS,
 * native storage, or caller input. Cleanup must use separate error storage.
 * No function retains a caller buffer or a managed heap pointer.
 */
J2O_API uint32_t j2o_abi_version(void);
J2O_API j2o_config *j2o_config_create(j2o_error *error);
J2O_API j2o_result j2o_config_set_u64(j2o_config *config, uint32_t key, uint64_t value, j2o_error *error);
J2O_API j2o_result j2o_config_set_bool(j2o_config *config, uint32_t key, int value, j2o_error *error);
J2O_API j2o_result j2o_config_set_string(j2o_config *config, uint32_t key, const char *value, size_t length,
                                      j2o_error *error);
J2O_API void j2o_config_destroy(j2o_config *config);

/* Start snapshots config. Returned failures leave *server NULL and roll back
 * bridge-controlled resources. Stock H2O has process-fatal initialization AND
 * runtime paths: allocation failure, loop/queue descriptor creation failure,
 * and internal notification errors can abort the entire host. These are not
 * recoverable j2o_result values; the bridge does not intercept h2o_fatal.
 * The configuration can be reused/destroyed after start returns.
 */
J2O_API j2o_result j2o_server_start(const j2o_config *config, j2o_server **server, j2o_error *error);
/* Borrowed descriptor: valid until destroy. Never close it in managed code. */
J2O_API int j2o_server_event_fd(j2o_server *server);
/* Request fields are borrowed from the event until release. Header indexes
 * enumerate pairs; field IDs 1..5 are method, raw target, body, authority and
 * peer address. Events and response commands own their bytes, never Jolt data. */
J2O_API unsigned j2o_server_port(j2o_server *server);
J2O_API j2o_result j2o_event_take(j2o_server *server, j2o_event **event, j2o_error *error);
J2O_API unsigned j2o_event_kind(const j2o_event *event);
J2O_API void j2o_event_id(const j2o_event *event, j2o_id *id);
J2O_API int j2o_event_version(const j2o_event *event);
J2O_API size_t j2o_event_headers(const j2o_event *event);
J2O_API const char *j2o_event_field(const j2o_event *event, unsigned field, size_t index, size_t *length);
J2O_API void j2o_event_release(j2o_event *event);
/* Response submission consumes the command only on J2O_OK. Header names and
 * values are copied; headers may be added only before submission. */
J2O_API j2o_response *j2o_response_create(const j2o_id *id, int status, const void *body, size_t length, j2o_error *error);
J2O_API j2o_result j2o_response_header(j2o_response *response, const char *name, size_t name_len,
                                       const char *value, size_t value_len, j2o_error *error);
J2O_API j2o_result j2o_response_submit(j2o_server *server, j2o_response *response, j2o_error *error);
J2O_API void j2o_response_release(j2o_response *response);
J2O_API j2o_result j2o_server_begin_drain(j2o_server *server, j2o_error *error);
J2O_API j2o_result j2o_server_force_stop(j2o_server *server, j2o_error *error);
/* A timeout returns J2O_OS/ETIMEDOUT and retains the complete server. */
J2O_API j2o_result j2o_server_join(j2o_server *server, uint64_t timeout_ms, j2o_error *error);
/* Requires successful join and no concurrent foreign calls. NULL is harmless. */
J2O_API void j2o_server_destroy(j2o_server *server);

#ifdef __cplusplus
}
#endif
#endif
