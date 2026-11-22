/* Includes the real implementation to inject bridge-owned startup failures.
 * The separate abi.c executable tests the externally linked public surface. */
#define J2O_TESTING 1
#include "../../native/j2o_bridge.c"
#include <dirent.h>

static void reset_fault(size_t step)
{
    atomic_store(&test_step, 0);
    atomic_store(&test_fail_at, step);
}
static void clean(size_t allocations)
{
    assert(atomic_load(&test_allocations) == allocations);
    assert(atomic_load(&test_fds) == 0);
    assert(atomic_load(&test_threads) == 0);
}
static j2o_config *configuration(const char *host)
{
    j2o_error error;
    j2o_config *config = j2o_config_create(&error);
    assert(config != NULL);
    assert(j2o_config_set_u64(config, J2O_PORT, 0, &error) == J2O_OK);
    assert(j2o_config_set_string(config, J2O_HOST, host, strlen(host), &error) == J2O_OK);
    return config;
}
static j2o_server *start(j2o_config *config)
{
    j2o_server *server = NULL; j2o_error error;
    j2o_result result = j2o_server_start(config, &server, &error);
    if (result != J2O_OK) fprintf(stderr, "start: %d %s %s\n", result, error.syscall, error.message);
    assert(result == J2O_OK && server != NULL);
    return server;
}
static void stop(j2o_server *server)
{
    j2o_error error;
    assert(j2o_server_begin_drain(server, &error) == J2O_OK);
    assert(j2o_server_force_stop(server, &error) == J2O_OK);
    assert(j2o_server_join(server, 5000, &error) == J2O_OK);
    assert(j2o_server_join(server, 0, &error) == J2O_OK);
    assert(j2o_server_begin_drain(server, &error) == J2O_OK);
    j2o_server_destroy(server);
}
static size_t entries(const char *path)
{
    DIR *directory = opendir(path);
    assert(directory != NULL);
    size_t count = 0;
    struct dirent *item;
    while ((item = readdir(directory)) != NULL) if (item->d_name[0] != '.') ++count;
    closedir(directory);
    return count;
}
static void connect_address(const struct sockaddr_storage *address, socklen_t length, unsigned port)
{
    struct sockaddr_storage target = *address;
    set_port(&target, port);
    int client = socket(target.ss_family, SOCK_STREAM, 0);
    assert(client >= 0);
    assert(connect(client, (const struct sockaddr *)&target, length) == 0);
    close(client);
}
static void config_errors(void)
{
    for (size_t i = 1; i <= 2; ++i) {
        reset_fault(i); j2o_error error;
        assert(j2o_config_create(&error) == NULL);
        assert(error.result == J2O_NOMEM);
        clean(0);
    }
    reset_fault(0);
    j2o_config *config = configuration("127.0.0.1");
    j2o_error error, zero = {0};
    memset(&error, 0xa5, sizeof(error));
    assert(j2o_config_set_u64(config, J2O_PORT, 0, &error) == J2O_OK);
    assert(memcmp(&error, &zero, sizeof(error)) == 0);
    assert(j2o_config_set_u64(config, UINT32_MAX, UINT64_MAX, &error) == J2O_INVALID);
    assert(j2o_config_set_u64(config, J2O_PORT, 65536, &error) == J2O_INVALID);
    assert(j2o_config_set_u64(config, J2O_MAX_EVENT_QUEUE, 0, &error) == J2O_INVALID);
    assert(j2o_config_set_bool(config, J2O_REUSE_PORT, 2, &error) == J2O_INVALID);
    assert(j2o_config_set_string(config, J2O_HOST, "a\0b", 3, &error) == J2O_INVALID);
    assert(j2o_config_set_string(config, J2O_HOST, "x", SIZE_MAX, &error) == J2O_INVALID);
    reset_fault(1);
    assert(j2o_config_set_string(config, J2O_HOST, "localhost", 9, &error) == J2O_NOMEM);
    assert(strcmp(config->host, "127.0.0.1") == 0);
    reset_fault(0);
    j2o_config_destroy(config); clean(0);
}
static void atomic_binding_and_faults(void)
{
    j2o_config *config = configuration("localhost");
    atomic_store(&test_secondary_collisions, 2);
    j2o_server *server = start(config);
    assert(server->listener_count >= 2);
    assert(atomic_load(&test_secondary_collisions) == 0);
    for (size_t i = 0; i < server->listener_count; ++i)
        connect_address(&server->listeners[i].address, server->listeners[i].length, server->port);
    unsigned port = server->port;
    stop(server); clean(2);
    j2o_error error;
    assert(j2o_config_set_u64(config, J2O_PORT, port, &error) == J2O_OK);
    reset_fault(0);
    server = start(config);
    size_t checkpoints = atomic_load(&test_step);
    stop(server); clean(2);
    for (size_t i = 1; i <= checkpoints; ++i) {
        reset_fault(i);
        server = (j2o_server *)(uintptr_t)1;
        assert(j2o_server_start(config, &server, &error) != J2O_OK);
        assert(server == NULL && error.result != J2O_OK && error.abi_size == sizeof(error));
        assert(error.message[sizeof(error.message) - 1] == 0);
        clean(2);
        reset_fault(0);
        server = start(config); /* The complete fixed-port set must be reusable. */
        stop(server); clean(2);
    }
    printf("PASS: every bridge startup checkpoint (%zu), atomic multi-bind, port-zero retries and rebind\n", checkpoints);
    j2o_config_destroy(config); clean(0);
}
static void bind_failure_and_error_lifetime(void)
{
    j2o_config *config = configuration("127.0.0.1");
    j2o_server *first = start(config), *second = NULL;
    j2o_error error;
    assert(j2o_config_set_u64(config, J2O_PORT, first->port, &error) == J2O_OK);
    assert(j2o_server_start(config, &second, &error) == J2O_OS);
    assert(second == NULL && error.os_errno == EADDRINUSE && strcmp(error.syscall, "bind") == 0);
    j2o_error retained = error;
    char host[1024]; memset(host, 'x', sizeof(host));
    assert(j2o_config_set_string(config, J2O_HOST, host, sizeof(host), &error) == J2O_OK);
    assert(j2o_server_start(config, &second, &error) == J2O_OS);
    assert(error.given[sizeof(error.given) - 1] == 0);
    j2o_config_destroy(config);
    assert(strcmp(retained.errno_name, "EADDRINUSE") == 0);
    assert(strcmp(retained.syscall, "bind") == 0);
    stop(first); clean(0);
}
static void *concurrent_stopper(void *data)
{
    j2o_server *server = data; j2o_error error;
    for (int i = 0; i < 8; ++i) {
        assert(j2o_server_begin_drain(server, &error) == J2O_OK);
        assert(j2o_server_force_stop(server, &error) == J2O_OK);
        assert(j2o_server_join(server, 5000, &error) == J2O_OK);
    }
    return NULL;
}
static void timeout_and_concurrent_join(void)
{
    j2o_config *config = configuration("127.0.0.1");
    j2o_server *server = start(config); j2o_error error;
    /* Reject deadline arithmetic overflow without stopping or freeing the server. */
    assert(j2o_server_join(server, UINT64_MAX, &error) == J2O_INVALID);
    assert(j2o_server_join(server, 0, &error) == J2O_OS && error.os_errno == ETIMEDOUT);
    assert(atomic_load(&server->state) == RUNNING);
    assert(fcntl(j2o_server_event_fd(server), F_GETFD) >= 0);
    pthread_t threads[8];
    for (size_t i = 0; i < 8; ++i) assert(pthread_create(threads + i, NULL, concurrent_stopper, server) == 0);
    for (size_t i = 0; i < 8; ++i) assert(pthread_join(threads[i], NULL) == 0);
    j2o_server_destroy(server);
    j2o_config_destroy(config); clean(0);
}
static void drain_and_loop_failure(void)
{
    j2o_config *config = configuration("127.0.0.1");
    for (int loop_failure = 0; loop_failure <= 1; ++loop_failure) {
        atomic_store(&test_loop_failure, loop_failure);
        j2o_server *server = start(config);
        unsigned port = server->port;
        j2o_error error;
        if (!loop_failure) assert(j2o_server_begin_drain(server, &error) == J2O_OK);
        /* Runtime failure must terminate without any caller stop request. */
        assert(j2o_server_join(server, 5000, &error) == J2O_OK);
        int borrowed = j2o_server_event_fd(server);
        assert(fcntl(borrowed, F_GETFD) >= 0);
        assert(j2o_server_force_stop(server, &error) == J2O_OK);
        assert(j2o_server_begin_drain(server, &error) == J2O_OK);
        assert(j2o_server_join(server, 0, &error) == J2O_OK);
        j2o_server_destroy(server);
        assert(fcntl(borrowed, F_GETFD) == -1 && errno == EBADF);
        clean(2);
        assert(j2o_config_set_u64(config, J2O_PORT, port, &error) == J2O_OK);
        stop(start(config)); clean(2);
        assert(j2o_config_set_u64(config, J2O_PORT, 0, &error) == J2O_OK);
    }
    j2o_config_destroy(config); clean(0);
    puts("PASS: drain-only and event-loop failure cleanup, borrowed descriptor lifetime and immediate rebind");
}

static void repeated_lifecycles(void)
{
    j2o_config *config = configuration("127.0.0.1");
    stop(start(config)); /* Warm process-global runtime initialization before OS counts. */
#ifdef __linux__
    size_t fds = entries("/proc/self/fd"), threads = entries("/proc/self/task");
#else
    size_t fds = entries("/dev/fd");
#endif
    for (unsigned i = 0; i < 300; ++i) { stop(start(config)); clean(2); }
#ifdef __linux__
    assert(entries("/proc/self/fd") == fds);
    for (unsigned attempt = 0; entries("/proc/self/task") != threads; ++attempt) {
        assert(attempt < 100);
        struct timespec pause = {0, 10000000}; nanosleep(&pause, NULL);
    }
#else
    assert(entries("/dev/fd") == fds);
#endif
    j2o_config_destroy(config); clean(0);
    puts("PASS: 300 lifecycles, zero owned allocations/fds/threads and stable OS descriptor/thread counts");
}
int main(void)
{
    config_errors();
    atomic_binding_and_faults();
    bind_failure_and_error_lifetime();
    timeout_and_concurrent_join();
    drain_and_loop_failure();
    repeated_lifecycles();
    puts("all passed: native lifecycle");
    return 0;
}
