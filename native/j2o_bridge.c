#include "j2o_bridge.h"
#include <h2o.h>
#include <h2o/socket/evloop.h>
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* Test hooks cover bridge-owned resources, never H2O's process-fatal paths. */
#ifdef J2O_TESTING
static _Atomic size_t test_step, test_fail_at, test_allocations, test_fds, test_threads;
static _Atomic unsigned test_secondary_collisions;
static _Atomic int test_loop_failure;
#define COUNT(field, delta) atomic_fetch_add(&(field), (delta))
static int checkpoint(void)
{
    if (atomic_fetch_add(&test_step, 1) + 1 != atomic_load(&test_fail_at)) return 0;
    errno = EIO;
    return 1;
}
#else
#define COUNT(field, delta) ((void)0)
static int checkpoint(void) { return 0; }
#endif

static void *allocate(size_t size)
{
    if (checkpoint()) return NULL;
    void *p = calloc(1, size);
    if (p != NULL) COUNT(test_allocations, 1);
    return p;
}
static void deallocate(void *p)
{
    if (p != NULL) { COUNT(test_allocations, (size_t)-1); free(p); }
}
static void close_fd(int *fd)
{
    if (*fd >= 0) { close(*fd); *fd = -1; COUNT(test_fds, (size_t)-1); }
}
static void copy_text(char *out, size_t capacity, const char *text)
{
    if (text == NULL) return;
    size_t length = strnlen(text, capacity - 1);
    memcpy(out, text, length);
    out[length] = 0;
}
static j2o_result fail(j2o_error *error, j2o_result result, int code, const char *call,
                       const char *name, const char *message, const char *key, const char *given)
{
    if (error != NULL) {
        memset(error, 0, sizeof(*error));
        error->abi_size = sizeof(*error); error->result = result; error->os_errno = code;
        copy_text(error->syscall, sizeof(error->syscall), call);
        copy_text(error->errno_name, sizeof(error->errno_name), name);
        copy_text(error->message, sizeof(error->message), message);
        copy_text(error->key, sizeof(error->key), key);
        copy_text(error->given, sizeof(error->given), given);
    }
    return result;
}
static void success(j2o_error *error) { memset(error, 0, sizeof(*error)); }
static const char *errno_name(int code)
{
    switch (code) {
    case EACCES: return "EACCES"; case EADDRINUSE: return "EADDRINUSE";
    case EADDRNOTAVAIL: return "EADDRNOTAVAIL"; case EAFNOSUPPORT: return "EAFNOSUPPORT";
    case EAGAIN: return "EAGAIN"; case EBADF: return "EBADF"; case EBUSY: return "EBUSY";
    case EINTR: return "EINTR"; case EINVAL: return "EINVAL"; case EIO: return "EIO";
    case EMFILE: return "EMFILE"; case ENFILE: return "ENFILE"; case ENOMEM: return "ENOMEM";
    case ENOBUFS: return "ENOBUFS"; case ENOENT: return "ENOENT"; case ENOSYS: return "ENOSYS";
    case ENOTSUP: return "ENOTSUP"; case EPERM: return "EPERM"; case ETIMEDOUT: return "ETIMEDOUT";
    default: return "OS_ERROR";
    }
}
static j2o_result os_failure(j2o_error *error, int code, const char *call)
{
    char buffer[256];
#if defined(__GLIBC__) && defined(_GNU_SOURCE)
    const char *message = strerror_r(code, buffer, sizeof(buffer));
#else
    const char *message = strerror_r(code, buffer, sizeof(buffer)) == 0 ? buffer : "OS operation failed";
#endif
    return fail(error, J2O_OS, code, call, errno_name(code), message, NULL, NULL);
}
static j2o_result no_memory(j2o_error *error)
{
    return fail(error, J2O_NOMEM, ENOMEM, "calloc", "ENOMEM", "Native allocation failed", NULL, NULL);
}
static j2o_result invalid(j2o_error *error, const char *key, const char *given, const char *message)
{
    return fail(error, J2O_INVALID, 0, NULL, NULL, message, key, given);
}

struct j2o_config { char *host; uint64_t values[15]; int reuse_port; };
typedef struct { int fd; h2o_socket_t *socket; struct sockaddr_storage address; socklen_t length; } listener;
typedef struct j2o_connection j2o_connection;
typedef struct { h2o_req_t *request; uint64_t generation; j2o_event *retired; int claimed; } request_slot;
typedef struct { char *name; size_t name_len; char *value; size_t value_len; } response_header;
struct j2o_event {
    struct j2o_event *next;
    j2o_id id;
    unsigned kind;
    int version;
    size_t headers;
    h2o_iovec_t *fields;
};
struct j2o_response {
    struct j2o_response *next;
    j2o_id id;
    int status;
    size_t length, header_count, bytes;
    response_header *headers;
    char body[];
};
struct j2o_connection {
    j2o_connection *next, **previous;
    j2o_server *server;
    h2o_socket_t *socket;
};
typedef enum { NEW, BINDING, RUNNING, DRAINING, FORCING, JOINED, DESTROYED } server_state;
struct j2o_server {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t thread;
    int mutex_ready, cond_ready, startup_done, loop_done, joining, joined;
    _Atomic int state;
    j2o_error startup_error;
    j2o_config config;
    listener *listeners;
    size_t listener_count;
    unsigned port;
    int control[2], events[2];
    h2o_globalconf_t h2o_config;
    h2o_context_t context;
    h2o_socket_t *control_socket;
    h2o_accept_ctx_t accept;
    j2o_connection *connections;
    size_t connection_count;
    request_slot *slots;
    size_t slot_count, live_requests;
    j2o_event *event_head, *event_tail;
    size_t event_count;
    j2o_response *response_head, *response_tail;
    size_t response_count, response_bytes;
};

uint32_t j2o_abi_version(void) { return J2O_ABI_VERSION; }
j2o_config *j2o_config_create(j2o_error *error)
{
    if (error == NULL) return NULL;
    success(error);
    j2o_config *config = allocate(sizeof(*config));
    if (config == NULL) { no_memory(error); return NULL; }
    config->host = allocate(sizeof("127.0.0.1"));
    if (config->host == NULL) { deallocate(config); no_memory(error); return NULL; }
    memcpy(config->host, "127.0.0.1", sizeof("127.0.0.1"));
    config->values[J2O_PORT] = 3000;
    config->values[J2O_MAX_REQUEST_BYTES] = 1048576;
    config->values[J2O_MAX_HEADER_BYTES] = 8192;
    config->values[J2O_MAX_LIVE_REQUESTS] = 1024;
    config->values[J2O_MAX_EVENT_QUEUE] = 2048;
    config->values[J2O_MAX_RESPONSE_QUEUE] = 1024;
    config->values[J2O_MAX_QUEUED_BYTES] = 64 * 1024 * 1024;
    config->values[J2O_MAX_CONNECTIONS] = 4096;
    config->values[J2O_KEEPALIVE_TIMEOUT_MS] = 30000;
    config->values[J2O_REQUEST_TIMEOUT_MS] = 60000;
    config->values[J2O_WRITE_TIMEOUT_MS] = 30000;
    config->values[J2O_HANDSHAKE_TIMEOUT_MS] = 10000;
    return config;
}
void j2o_config_destroy(j2o_config *config)
{
    if (config != NULL) { deallocate(config->host); deallocate(config); }
}
j2o_result j2o_config_set_u64(j2o_config *config, uint32_t key, uint64_t value, j2o_error *error)
{
    if (error == NULL) return J2O_INVALID;
    success(error);
    char given[32]; snprintf(given, sizeof(given), "%llu", (unsigned long long)value);
    if (config == NULL) return invalid(error, "config", NULL, "Configuration is NULL");
    if (key != J2O_PORT && (key < J2O_MAX_REQUEST_BYTES || key > J2O_HANDSHAKE_TIMEOUT_MS))
        return invalid(error, "key", given, "Key does not accept an unsigned integer");
    if ((key == J2O_PORT && value > 65535) ||
        (key != J2O_PORT && key != J2O_REQUEST_TIMEOUT_MS && key != J2O_WRITE_TIMEOUT_MS && value == 0) ||
        (key >= J2O_MAX_REQUEST_BYTES && key <= J2O_MAX_CONNECTIONS && value > SIZE_MAX))
        return invalid(error, "value", given, "Configuration value is out of range");
    config->values[key] = value;
    return J2O_OK;
}
j2o_result j2o_config_set_bool(j2o_config *config, uint32_t key, int value, j2o_error *error)
{
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (config == NULL || key != J2O_REUSE_PORT || (value != 0 && value != 1))
        return invalid(error, "reuse-port", NULL, "Expected a boolean reuse-port value");
#ifndef SO_REUSEPORT
    if (value) return fail(error, J2O_UNSUPPORTED, 0, "setsockopt", NULL, "SO_REUSEPORT is unavailable", "reuse-port", "true");
#endif
    config->reuse_port = value;
    return J2O_OK;
}
j2o_result j2o_config_set_string(j2o_config *config, uint32_t key, const char *value, size_t length, j2o_error *error)
{
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (config == NULL || key != J2O_HOST || value == NULL || length == 0 || length == SIZE_MAX || memchr(value, 0, length) != NULL)
        return invalid(error, "host", NULL, "Expected a non-empty host without NUL bytes");
    char *copy = allocate(length + 1);
    if (copy == NULL) return no_memory(error);
    memcpy(copy, value, length);
    deallocate(config->host); config->host = copy;
    return J2O_OK;
}

static int counted_socket(int family)
{
    if (checkpoint()) { errno = EIO; return -1; }
    int fd = socket(family, SOCK_STREAM, 0);
    if (fd >= 0) COUNT(test_fds, 1);
    return fd;
}
static int fd_flags(int fd)
{
    if (checkpoint()) { errno = EIO; return -1; }
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || checkpoint() || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ||
        checkpoint() || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) return -1;
    return 0;
}
static int make_pipe(int pair[2])
{
    if (checkpoint()) { errno = EIO; return -1; }
    if (pipe(pair) != 0) return -1;
    COUNT(test_fds, 2);
    if (fd_flags(pair[0]) != 0 || fd_flags(pair[1]) != 0) return -1;
    return 0;
}
static void close_raw_listeners(j2o_server *server)
{
    for (size_t i = 0; i < server->listener_count; ++i) close_fd(&server->listeners[i].fd);
}
static int same_address(const listener *a, const struct addrinfo *b)
{
    if (a->address.ss_family != b->ai_family) return 0;
    if (b->ai_family == AF_INET)
        return ((const struct sockaddr_in *)&a->address)->sin_addr.s_addr == ((const struct sockaddr_in *)b->ai_addr)->sin_addr.s_addr;
    const struct sockaddr_in6 *x = (const struct sockaddr_in6 *)&a->address, *y = (const struct sockaddr_in6 *)b->ai_addr;
    return x->sin6_scope_id == y->sin6_scope_id && memcmp(&x->sin6_addr, &y->sin6_addr, sizeof(x->sin6_addr)) == 0;
}
static void set_port(struct sockaddr_storage *address, unsigned port)
{
    if (address->ss_family == AF_INET) ((struct sockaddr_in *)address)->sin_port = htons((uint16_t)port);
    else ((struct sockaddr_in6 *)address)->sin6_port = htons((uint16_t)port);
}
static j2o_result bind_listeners(j2o_server *server, j2o_error *error)
{
    struct addrinfo hints = {0}, *addresses = NULL;
    hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM; hints.ai_flags = AI_PASSIVE;
    if (checkpoint()) return os_failure(error, EIO, "getaddrinfo");
    int result = getaddrinfo(server->config.host, NULL, &hints, &addresses);
    if (result != 0)
        return fail(error, J2O_OS, result, "getaddrinfo", "EAI_ERROR", gai_strerror(result), "host", server->config.host);
    size_t count = 0;
    for (struct addrinfo *a = addresses; a != NULL; a = a->ai_next)
        if (a->ai_family == AF_INET || a->ai_family == AF_INET6) ++count;
    if (count == 0 || count > SIZE_MAX / sizeof(listener)) {
        freeaddrinfo(addresses);
        return invalid(error, "host", server->config.host, "Host resolved to no usable addresses");
    }
    server->listeners = allocate(count * sizeof(listener));
    if (server->listeners == NULL) { freeaddrinfo(addresses); return no_memory(error); }
    for (struct addrinfo *a = addresses; a != NULL; a = a->ai_next) {
        if (a->ai_family != AF_INET && a->ai_family != AF_INET6) continue;
        size_t i;
        for (i = 0; i < server->listener_count; ++i) if (same_address(server->listeners + i, a)) break;
        if (i != server->listener_count) continue;
        listener *item = server->listeners + server->listener_count++;
        item->fd = -1; item->length = (socklen_t)a->ai_addrlen;
        assert(a->ai_addrlen <= sizeof(item->address));
        memcpy(&item->address, a->ai_addr, a->ai_addrlen);
    }
    freeaddrinfo(addresses);
    for (unsigned attempt = 0; attempt < 32; ++attempt) {
        unsigned selected_port = (unsigned)server->config.values[J2O_PORT];
        int retry = 0;
        for (size_t i = 0; i < server->listener_count; ++i) {
            listener *item = server->listeners + i;
            item->fd = counted_socket(item->address.ss_family);
            if (item->fd < 0) return os_failure(error, errno, "socket");
            if (fd_flags(item->fd) != 0) return os_failure(error, errno, "fcntl");
            int one = 1;
            if (checkpoint() || setsockopt(item->fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0)
                return os_failure(error, errno == 0 ? EIO : errno, "setsockopt");
#ifdef SO_REUSEPORT
            if (server->config.reuse_port && (checkpoint() || setsockopt(item->fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0))
                return os_failure(error, errno == 0 ? EIO : errno, "setsockopt");
#endif
            if (item->address.ss_family == AF_INET6 &&
                (checkpoint() || setsockopt(item->fd, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof(one)) != 0))
                return os_failure(error, errno == 0 ? EIO : errno, "setsockopt");
            set_port(&item->address, selected_port);
            int bind_result;
#ifdef J2O_TESTING
            if (i != 0 && server->config.values[J2O_PORT] == 0 && atomic_load(&test_secondary_collisions) != 0) {
                atomic_fetch_sub(&test_secondary_collisions, 1); errno = EADDRINUSE; bind_result = -1;
            } else
#endif
            if (checkpoint()) { errno = EIO; bind_result = -1; }
            else bind_result = bind(item->fd, (struct sockaddr *)&item->address, item->length);
            if (bind_result != 0) {
                int code = errno;
                if (code == EADDRINUSE && i != 0 && server->config.values[J2O_PORT] == 0) { retry = 1; break; }
                return os_failure(error, code, "bind");
            }
            if (selected_port == 0) {
                struct sockaddr_storage bound; socklen_t length = sizeof(bound);
                if (checkpoint()) return os_failure(error, EIO, "getsockname");
                if (getsockname(item->fd, (struct sockaddr *)&bound, &length) != 0) return os_failure(error, errno, "getsockname");
                selected_port = bound.ss_family == AF_INET ? ntohs(((struct sockaddr_in *)&bound)->sin_port)
                                                                         : ntohs(((struct sockaddr_in6 *)&bound)->sin6_port);
            }
            if (checkpoint()) return os_failure(error, EIO, "listen");
            if (listen(item->fd, 128) != 0) return os_failure(error, errno, "listen");
        }
        if (!retry) { server->port = selected_port; return J2O_OK; }
        close_raw_listeners(server);
    }
    return os_failure(error, EADDRINUSE, "bind");
}

#include "j2o_unary.inc"

static void close_adopted(h2o_socket_t **socket)
{
    if (*socket != NULL) { h2o_socket_close(*socket); *socket = NULL; COUNT(test_fds, (size_t)-1); }
}
static void control_ready(h2o_socket_t *socket, const char *error)
{
    j2o_server *server = socket->data;
    if (error != NULL) { atomic_store(&server->state, FORCING); return; }
    char buffer[256];
    ssize_t result;
    do {
        result = read(server->control[0], buffer, sizeof(buffer));
    } while (result > 0 || (result < 0 && errno == EINTR));
    if (result == 0 || (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK))
        atomic_store(&server->state, FORCING);
    process_responses(server);
}
static void publish_start(j2o_server *server)
{
    pthread_mutex_lock(&server->mutex);
    server->startup_done = 1;
    if (server->startup_error.result == J2O_OK) atomic_store(&server->state, RUNNING);
    pthread_cond_broadcast(&server->changed);
    pthread_mutex_unlock(&server->mutex);
}
static void *server_loop(void *data)
{
    j2o_server *server = data;
    h2o_config_init(&server->h2o_config);
    server->h2o_config.max_request_entity_size = server->config.values[J2O_MAX_REQUEST_BYTES];
    server->h2o_config.http1.req_io_timeout = server->config.values[J2O_KEEPALIVE_TIMEOUT_MS];
    server->h2o_config.http1.req_timeout = server->config.values[J2O_REQUEST_TIMEOUT_MS];
    server->h2o_config.http2.idle_timeout = server->config.values[J2O_KEEPALIVE_TIMEOUT_MS];
    server->h2o_config.handshake_timeout = server->config.values[J2O_HANDSHAKE_TIMEOUT_MS];
    h2o_hostconf_t *host = h2o_config_register_host(&server->h2o_config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    h2o_pathconf_t *path = h2o_config_register_path(host, "/", 0);
    unary_handler *handler = (unary_handler *)h2o_create_handler(path, sizeof(*handler));
    handler->super.on_req = on_unary_request;
    handler->server = server;
    h2o_loop_t *loop = h2o_evloop_create();
    h2o_context_init(&server->context, loop, &server->h2o_config);
    server->accept.ctx = &server->context;
    server->accept.hosts = server->h2o_config.hosts;
    int control_fd = -1;
    if (checkpoint()) os_failure(&server->startup_error, EIO, "fcntl");
    else {
        control_fd = fcntl(server->control[0], F_DUPFD_CLOEXEC, 0);
        if (control_fd < 0) os_failure(&server->startup_error, errno, "fcntl");
        else COUNT(test_fds, 1);
    }
    if (control_fd >= 0) {
        server->control_socket = h2o_evloop_socket_create(loop, control_fd, H2O_SOCKET_FLAG_DONT_READ);
        server->control_socket->data = server;
        h2o_socket_read_start(server->control_socket, control_ready);
        for (size_t i = 0; i < server->listener_count; ++i) {
            if (checkpoint()) { os_failure(&server->startup_error, EIO, "adopt-listener"); break; }
            listener *item = server->listeners + i;
            item->socket = h2o_evloop_socket_create(loop, item->fd, H2O_SOCKET_FLAG_DONT_READ);
            item->fd = -1;
            item->socket->data = server;
            h2o_socket_read_start(item->socket, accept_unary);
        }
    }
    publish_start(server);
    int draining = 0, sockets_shutdown = 0;
    if (server->startup_error.result == J2O_OK) {
        for (;;) {
            int state = atomic_load(&server->state);
            if (state != RUNNING && !draining) {
                for (size_t i = 0; i < server->listener_count; ++i) close_adopted(&server->listeners[i].socket);
                close_raw_listeners(server);
                h2o_context_request_shutdown(&server->context);
                draining = 1;
            }
            if (state == FORCING && !sockets_shutdown) {
                for (j2o_connection *item = server->connections; item != NULL; item = item->next)
                    shutdown(h2o_socket_get_fd(item->socket), SHUT_RDWR);
                sockets_shutdown = 1;
            }
            if (draining && server->connection_count == 0) break;
            uint32_t delay = h2o_cleanup_thread(h2o_now(loop), &server->context);
            int result;
#ifdef J2O_TESTING
            if (atomic_exchange(&test_loop_failure, 0)) { errno = EIO; result = -1; }
            else
#endif
                result = h2o_evloop_run(loop, delay > INT32_MAX ? INT32_MAX : (int32_t)delay);
            if (result != 0 && errno != EINTR) atomic_store(&server->state, FORCING);
        }
    }
    for (size_t i = 0; i < server->listener_count; ++i) close_adopted(&server->listeners[i].socket);
    close_raw_listeners(server);
    if (!draining) h2o_context_request_shutdown(&server->context);
    process_responses(server);
    close_adopted(&server->control_socket);
    h2o_context_dispose(&server->context);
    h2o_evloop_destroy(loop);
    h2o_config_dispose(&server->h2o_config);
    h2o_cleanup_thread(0, NULL);
    pthread_mutex_lock(&server->mutex);
    server->loop_done = 1;
    pthread_cond_broadcast(&server->changed);
    pthread_mutex_unlock(&server->mutex);
    return NULL;
}
static void release_server(j2o_server *server)
{
    close_raw_listeners(server);
    close_fd(&server->control[0]); close_fd(&server->control[1]);
    close_fd(&server->events[0]); close_fd(&server->events[1]);
    while (server->event_head != NULL) {
        j2o_event *next = server->event_head->next;
        j2o_event_release(server->event_head); server->event_head = next;
    }
    while (server->response_head != NULL) {
        j2o_response *next = server->response_head->next;
        j2o_response_release(server->response_head); server->response_head = next;
    }
    for (size_t i = 0; i < server->slot_count; ++i) j2o_event_release(server->slots[i].retired);
    deallocate(server->slots);
    deallocate(server->listeners); deallocate(server->config.host);
    if (server->cond_ready) pthread_cond_destroy(&server->changed);
    if (server->mutex_ready) pthread_mutex_destroy(&server->mutex);
    atomic_store(&server->state, DESTROYED);
    deallocate(server);
}
j2o_result j2o_server_start(const j2o_config *config, j2o_server **out, j2o_error *error)
{
    if (out != NULL) *out = NULL;
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (config == NULL || out == NULL) return invalid(error, "config", NULL, "Configuration and output are required");
    j2o_server *server = allocate(sizeof(*server));
    if (server == NULL) return no_memory(error);
    server->control[0] = server->control[1] = server->events[0] = server->events[1] = -1;
    server->config = *config;
    server->config.host = allocate(strlen(config->host) + 1);
    if (server->config.host == NULL) { no_memory(error); goto failed; }
    memcpy(server->config.host, config->host, strlen(config->host) + 1);
    if (config->values[J2O_MAX_LIVE_REQUESTS] > UINT32_MAX ||
        config->values[J2O_MAX_LIVE_REQUESTS] > SIZE_MAX / sizeof(*server->slots)) {
        invalid(error, "max-live-requests", NULL, "Request slot limit is too large"); goto failed;
    }
    server->slots = allocate((size_t)config->values[J2O_MAX_LIVE_REQUESTS] * sizeof(*server->slots));
    if (server->slots == NULL) { no_memory(error); goto failed; }
    server->slot_count = (size_t)config->values[J2O_MAX_LIVE_REQUESTS];
    int code;
    if (checkpoint()) { os_failure(error, EIO, "pthread_mutex_init"); goto failed; }
    if ((code = pthread_mutex_init(&server->mutex, NULL)) != 0) { os_failure(error, code, "pthread_mutex_init"); goto failed; }
    server->mutex_ready = 1;
    if (checkpoint()) { os_failure(error, EIO, "pthread_condattr_init"); goto failed; }
    pthread_condattr_t attr;
    if ((code = pthread_condattr_init(&attr)) != 0) { os_failure(error, code, "pthread_condattr_init"); goto failed; }
#ifndef __APPLE__
    code = checkpoint() ? EIO : pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (code != 0) { pthread_condattr_destroy(&attr); os_failure(error, code, "pthread_condattr_setclock"); goto failed; }
#endif
    code = checkpoint() ? EIO : pthread_cond_init(&server->changed, &attr);
    pthread_condattr_destroy(&attr);
    if (code != 0) { os_failure(error, code, "pthread_cond_init"); goto failed; }
    server->cond_ready = 1;
    atomic_store(&server->state, BINDING);
    if (bind_listeners(server, error) != J2O_OK) goto failed;
    if (make_pipe(server->control) != 0 || make_pipe(server->events) != 0) { os_failure(error, errno, "pipe"); goto failed; }
    if (checkpoint()) { os_failure(error, EIO, "pthread_create"); goto failed; }
    code = pthread_create(&server->thread, NULL, server_loop, server);
    if (code != 0) { os_failure(error, code, "pthread_create"); goto failed; }
    COUNT(test_threads, 1);
    pthread_mutex_lock(&server->mutex);
    while (!server->startup_done) pthread_cond_wait(&server->changed, &server->mutex);
    pthread_mutex_unlock(&server->mutex);
    if (server->startup_error.result != J2O_OK) {
        *error = server->startup_error;
        pthread_join(server->thread, NULL); COUNT(test_threads, (size_t)-1);
        goto failed;
    }
    *out = server;
    return J2O_OK;
failed:
    release_server(server);
    return (j2o_result)error->result;
}
int j2o_server_event_fd(j2o_server *server) { return server == NULL ? -1 : server->events[0]; }
static j2o_result request_stop(j2o_server *server, int force, j2o_error *error)
{
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (server == NULL) return invalid(error, "server", NULL, "Server is NULL");
    pthread_mutex_lock(&server->mutex);
    int state = atomic_load(&server->state);
    if (state == RUNNING || (force && state == DRAINING)) atomic_store(&server->state, force ? FORCING : DRAINING);
    char byte = 1;
    ssize_t result;
    do { result = write(server->control[1], &byte, 1); } while (result < 0 && errno == EINTR);
    int code = result < 0 && errno != EAGAIN && errno != EWOULDBLOCK ? errno : 0;
    pthread_mutex_unlock(&server->mutex);
    return code == 0 ? J2O_OK : os_failure(error, code, "write");
}
j2o_result j2o_server_begin_drain(j2o_server *server, j2o_error *error) { return request_stop(server, 0, error); }
j2o_result j2o_server_force_stop(j2o_server *server, j2o_error *error) { return request_stop(server, 1, error); }
static uint64_t monotonic_ms(void)
{
    struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000 + (uint64_t)now.tv_nsec / 1000000;
}
static int wait_until(j2o_server *server, uint64_t deadline)
{
    uint64_t now = monotonic_ms();
    if (now >= deadline) return ETIMEDOUT;
    uint64_t remaining = deadline - now;
#ifdef __APPLE__
    struct timespec limit = {(time_t)(remaining / 1000), (long)(remaining % 1000) * 1000000};
    return pthread_cond_timedwait_relative_np(&server->changed, &server->mutex, &limit);
#else
    struct timespec limit = {(time_t)(deadline / 1000), (long)(deadline % 1000) * 1000000};
    (void)remaining;
    return pthread_cond_timedwait(&server->changed, &server->mutex, &limit);
#endif
}
j2o_result j2o_server_join(j2o_server *server, uint64_t timeout_ms, j2o_error *error)
{
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (server == NULL || timeout_ms > (uint64_t)INT64_MAX / 1000000)
        return invalid(error, "timeout-ms", NULL, "Invalid server or join timeout");
    uint64_t deadline = monotonic_ms() + timeout_ms;
    pthread_mutex_lock(&server->mutex);
    while (!server->joined && (!server->loop_done || server->joining)) {
        int code = wait_until(server, deadline);
        if (code != 0) { pthread_mutex_unlock(&server->mutex); return os_failure(error, code, "pthread_cond_timedwait"); }
    }
    if (server->joined) { pthread_mutex_unlock(&server->mutex); return J2O_OK; }
    server->joining = 1;
    pthread_mutex_unlock(&server->mutex);
    int code = pthread_join(server->thread, NULL);
    pthread_mutex_lock(&server->mutex);
    server->joining = 0;
    if (code == 0) { server->joined = 1; atomic_store(&server->state, JOINED); COUNT(test_threads, (size_t)-1); }
    pthread_cond_broadcast(&server->changed);
    pthread_mutex_unlock(&server->mutex);
    return code == 0 ? J2O_OK : os_failure(error, code, "pthread_join");
}
void j2o_server_destroy(j2o_server *server)
{
    if (server == NULL) return;
    assert(server->joined);
    release_server(server);
}
