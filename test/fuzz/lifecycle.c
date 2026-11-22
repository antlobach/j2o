/* Stateful libFuzzer target: real bridge/H2O, never a replacement state model. */
#define J2O_TESTING 1
#include "../../native/j2o_bridge.c"
#include <assert.h>

static void check_error(j2o_result result, const j2o_error *error)
{
    static const j2o_error zero;
    if (result == J2O_OK) assert(memcmp(error, &zero, sizeof(*error)) == 0);
    else {
        assert(error->abi_size == sizeof(*error) && error->result == (int)result);
        assert(memchr(error->message, 0, sizeof(error->message)) != NULL);
        assert(memchr(error->syscall, 0, sizeof(error->syscall)) != NULL);
        assert(memchr(error->errno_name, 0, sizeof(error->errno_name)) != NULL);
        assert(memchr(error->key, 0, sizeof(error->key)) != NULL);
        assert(memchr(error->given, 0, sizeof(error->given)) != NULL);
    }
}
static void no_resources(size_t allocations)
{
    assert(atomic_load(&test_allocations) == allocations);
    assert(atomic_load(&test_fds) == 0 && atomic_load(&test_threads) == 0);
}
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 3) return 0;
    atomic_store(&test_step, 0);
    atomic_store(&test_fail_at, data[0] % 4 == 0 ? 1 + data[1] % 2 : 0);
    atomic_store(&test_loop_failure, 0);
    atomic_store(&test_secondary_collisions, 0);
    j2o_error error;
    j2o_config *config = j2o_config_create(&error);
    if (config == NULL) {
        check_error(J2O_NOMEM, &error); no_resources(0); return 0;
    }
    check_error(J2O_OK, &error);
    atomic_store(&test_fail_at, 0);
    assert(j2o_config_set_u64(config, J2O_PORT, 0, &error) == J2O_OK);
    static const char *hosts[] = {"127.0.0.1", "localhost", "::1", "a\0b"};
    unsigned host = data[2] % 4;
    j2o_result result = j2o_config_set_string(config, J2O_HOST, hosts[host], host == 3 ? 3 : strlen(hosts[host]), &error);
    check_error(result, &error);
    assert((host == 3) == (result == J2O_INVALID));
    /* Mutations include invalid keys, zero limits and uint64 boundaries. */
    for (size_t i = 3; i + 9 <= size && i < 75; i += 9) {
        uint64_t value = 0;
        for (unsigned j = 0; j < 8; ++j) value |= (uint64_t)data[i + 1 + j] << (8 * j);
        uint32_t key = data[i] % 18;
        char *previous_host = config->host;
        int previous_reuse_port = config->reuse_port;
        uint64_t previous_values[15];
        memcpy(previous_values, config->values, sizeof(previous_values));
        result = j2o_config_set_u64(config, key, value, &error);
        check_error(result, &error);
        if (result != J2O_OK) {
            assert(config->host == previous_host && config->reuse_port == previous_reuse_port);
            assert(memcmp(config->values, previous_values, sizeof(previous_values)) == 0);
        }
        result = j2o_config_set_bool(config, key, data[i] % 4, &error);
        check_error(result, &error);
    }
    /* Keep network binding ephemeral; arbitrary application ports are not fuzz resources. */
    assert(j2o_config_set_u64(config, J2O_PORT, 0, &error) == J2O_OK);
    for (unsigned cycle = 0; cycle < 2; ++cycle) {
        atomic_store(&test_step, 0);
        atomic_store(&test_fail_at, cycle == 0 ? data[1] % 96 : 0);
        atomic_store(&test_loop_failure, (data[2] >> (cycle + 2)) & 1);
        j2o_server *server = (j2o_server *)(uintptr_t)1;
        result = j2o_server_start(config, &server, &error);
        check_error(result, &error);
        atomic_store(&test_fail_at, 0);
        if (result != J2O_OK) { assert(server == NULL); no_resources(2); continue; }
        assert(server != NULL && j2o_server_event_fd(server) >= 0);
        /* Exercise copied response commands, malformed header bytes and stale
         * IDs against the real owner loop, including ownership on rejection. */
        j2o_id id = {0, (uint32_t)data[0], data[1]};
        size_t length = size < 128 ? size : 128;
        j2o_response *response = j2o_response_create(&id, 100 + data[2], data, length, &error);
        assert(response != NULL);
        check_error(J2O_OK, &error);
        size_t split = data[0] % (length + 1);
        result = j2o_response_header(response, (const char *)data, split,
                                     (const char *)data + split, length - split, &error);
        check_error(result, &error);
        assert(result == J2O_OK || result == J2O_INVALID || result == J2O_NOMEM);
        result = j2o_response_submit(server, response, &error);
        check_error(result, &error);
        if (result != J2O_OK) j2o_response_release(response);
        /* Legal callers retain the handle; destruction is never raced with a call. */
        for (size_t i = 3; i < size && i < 35; ++i) {
            switch (data[i] % 4) {
            case 0: result = j2o_server_begin_drain(server, &error); break;
            case 1: result = j2o_server_force_stop(server, &error); break;
            default: result = j2o_server_join(server, data[i] % 2, &error); break;
            }
            check_error(result, &error);
            assert(result == J2O_OK || (result == J2O_OS && error.os_errno == ETIMEDOUT));
            assert(fcntl(j2o_server_event_fd(server), F_GETFD) >= 0);
        }
        result = j2o_server_force_stop(server, &error); check_error(result, &error); assert(result == J2O_OK);
        result = j2o_server_join(server, 5000, &error); check_error(result, &error); assert(result == J2O_OK);
        j2o_server_destroy(server); no_resources(2);
    }
    j2o_config_destroy(config); no_resources(0);
    return 0;
}
