#include "../../native/j2o_bridge.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

_Static_assert(sizeof(j2o_result) == 4, "result ABI");
_Static_assert(sizeof(j2o_id) == 16, "id ABI");
_Static_assert(offsetof(j2o_id, loop) == 0 && offsetof(j2o_id, slot) == 4 && offsetof(j2o_id, generation) == 8, "id fields");
_Static_assert(sizeof(j2o_error) == 524, "error ABI");
_Static_assert(offsetof(j2o_error, abi_size) == 0, "abi_size");
_Static_assert(offsetof(j2o_error, result) == 4, "result");
_Static_assert(offsetof(j2o_error, os_errno) == 8, "os_errno");
_Static_assert(offsetof(j2o_error, message) == 12, "message");
_Static_assert(offsetof(j2o_error, syscall) == 268, "syscall");
_Static_assert(offsetof(j2o_error, errno_name) == 300, "errno_name");
_Static_assert(offsetof(j2o_error, key) == 332, "key");
_Static_assert(offsetof(j2o_error, given) == 396, "given");
_Static_assert(J2O_HOST == 1 && J2O_PORT == 2 && J2O_REUSE_PORT == 3 &&
               J2O_MAX_REQUEST_BYTES == 4 && J2O_MAX_HEADER_BYTES == 5 &&
               J2O_MAX_LIVE_REQUESTS == 6 && J2O_MAX_EVENT_QUEUE == 7 &&
               J2O_MAX_RESPONSE_QUEUE == 8 && J2O_MAX_QUEUED_BYTES == 9 &&
               J2O_MAX_CONNECTIONS == 10 && J2O_KEEPALIVE_TIMEOUT_MS == 11 &&
               J2O_REQUEST_TIMEOUT_MS == 12 && J2O_WRITE_TIMEOUT_MS == 13 &&
               J2O_HANDSHAKE_TIMEOUT_MS == 14, "config keys");
_Static_assert(J2O_OK == 0 && J2O_INVALID == 1 && J2O_NOMEM == 2 && J2O_OS == 3 &&
               J2O_UNSUPPORTED == 4 && J2O_QUEUE_FULL == 5 &&
               J2O_STOPPED == 6 && J2O_STALE == 7, "result values");

int main(void)
{
    assert(j2o_abi_version() == UINT32_C(0x80000002));
    j2o_error error, zero = {0};
    memset(&error, 0xa5, sizeof(error));
    j2o_config *config = j2o_config_create(&error);
    assert(config != NULL && memcmp(&error, &zero, sizeof(error)) == 0);
    assert(j2o_config_set_u64(config, J2O_PORT, 65536, &error) == J2O_INVALID);
    assert(error.abi_size == sizeof(error) && error.result == J2O_INVALID && strcmp(error.given, "65536") == 0);
    j2o_error saved = error;
    assert(j2o_config_set_u64(config, J2O_PORT, 0, &error) == J2O_OK);
    assert(memcmp(&error, &zero, sizeof(error)) == 0);
    j2o_server *server = NULL;
    assert(j2o_server_start(config, &server, &error) == J2O_OK);
    j2o_config_destroy(config); /* Start owns an independent snapshot. */
    assert(strcmp(saved.given, "65536") == 0 && saved.result == J2O_INVALID);
    assert(j2o_server_event_fd(server) >= 0);
    assert(j2o_server_join(server, 0, &error) == J2O_OS && error.os_errno == ETIMEDOUT);
    assert(j2o_server_force_stop(server, &error) == J2O_OK);
    assert(j2o_server_join(server, 5000, &error) == J2O_OK);
    j2o_server_destroy(server);
    puts("all passed: dynamically linked development ABI and error lifetime");
    return 0;
}
