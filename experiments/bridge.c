/* Experimental ABI, not the j2o public API. All buffers are native-owned.
 * H2O and its callbacks never enter Chez. Request pointers stay on the loop.
 * Any exported function that allocates, locks or waits is bound :blocking.
 */
#include <h2o.h>
#include <h2o/http1.h>
#include <h2o/http2.h>
#include <h2o/multithread.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define CAPACITY 64
#define MAX_BODY 4096
#define MAX_PATH 2048
#define MAX_RESPONSE (1024 * 1024)

typedef struct probe_server probe_server;
typedef struct {
    h2o_handler_t super;
    probe_server *server;
} probe_handler;
typedef struct {
    uint64_t id;
    size_t method_len, path_len, body_len;
    int version;
    char data[];
} request_packet;
typedef struct {
    h2o_multithread_message_t super;
    uint64_t id;
    size_t len;
    int status;
    char body[];
} response_message;
typedef struct {
    probe_server *server;
    uint64_t id;
    size_t slot;
} request_guard;
typedef struct connection {
    struct connection *next, **previous;
    h2o_socket_t *socket;
} connection;

struct probe_server {
    pthread_t thread;
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    int started, error, notify[2], port;
    h2o_globalconf_t config;
    h2o_context_t context;
    h2o_accept_ctx_t accept;
    h2o_socket_t *listener;
    h2o_multithread_receiver_t receiver;
    connection *connections;
    request_packet *queue[CAPACITY];
    size_t head, count;
    struct { uint64_t id; h2o_req_t *request; } slots[CAPACITY];
    uint64_t next_id;
    _Atomic int stopping;
    _Atomic uint64_t admitted, answered, stale, disposed, rejected, packets;
};
static _Atomic int hold_active;

static void notify_locked(probe_server *server)
{
    char byte = 1;
    ssize_t result;
    do { result = write(server->notify[1], &byte, 1); } while (result < 0 && errno == EINTR);
    /* EAGAIN means an existing byte already makes the descriptor readable. */
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) abort();
}

static void request_disposed(void *data)
{
    request_guard *guard = data;
    probe_server *server = guard->server;
    if (server->slots[guard->slot].id == guard->id) {
        server->slots[guard->slot].request = NULL;
        server->slots[guard->slot].id = 0;
    }
    atomic_fetch_add(&server->disposed, 1);
}

static int on_request(h2o_handler_t *handler, h2o_req_t *request)
{
    probe_server *server = ((probe_handler *)handler)->server;
    if (request->entity.len > MAX_BODY || request->path.len > MAX_PATH || request->method.len > 32) {
        h2o_send_error_generic(request, 413, "Payload Too Large", "probe input limit", 0);
        return 0;
    }
    size_t slot;
    for (slot = 0; slot < CAPACITY; ++slot)
        if (server->slots[slot].request == NULL) break;
    if (slot == CAPACITY || atomic_load(&server->stopping)) {
        atomic_fetch_add(&server->rejected, 1);
        h2o_send_error_generic(request, 503, "Service Unavailable", "probe admission full", 0);
        return 0;
    }
    size_t bytes = request->method.len + request->path.len + request->entity.len;
    request_packet *packet = malloc(sizeof(*packet) + bytes);
    if (packet == NULL) {
        h2o_send_error_generic(request, 503, "Service Unavailable", "probe allocation failed", 0);
        return 0;
    }
    packet->id = ++server->next_id;
    packet->method_len = request->method.len;
    packet->path_len = request->path.len;
    packet->body_len = request->entity.len;
    packet->version = request->version;
    memcpy(packet->data, request->method.base, packet->method_len);
    memcpy(packet->data + packet->method_len, request->path.base, packet->path_len);
    if (packet->body_len != 0)
        memcpy(packet->data + packet->method_len + packet->path_len, request->entity.base, packet->body_len);
    server->slots[slot].id = packet->id;
    server->slots[slot].request = request;
    request_guard *guard = h2o_mem_alloc_shared(&request->pool, sizeof(*guard), request_disposed);
    *guard = (request_guard){server, packet->id, slot};
    pthread_mutex_lock(&server->mutex);
    if (server->count == CAPACITY) {
        server->slots[slot].request = NULL;
        server->slots[slot].id = 0;
        free(packet);
        atomic_fetch_add(&server->rejected, 1);
        pthread_mutex_unlock(&server->mutex);
        h2o_send_error_generic(request, 503, "Service Unavailable", "probe queue full", 0);
        return 0;
    }
    server->queue[(server->head + server->count) % CAPACITY] = packet;
    ++server->count;
    atomic_fetch_add(&server->packets, 1);
    atomic_fetch_add(&server->admitted, 1);
    notify_locked(server);
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

static void on_messages(h2o_multithread_receiver_t *receiver, h2o_linklist_t *messages)
{
    probe_server *server = H2O_STRUCT_FROM_MEMBER(probe_server, receiver, receiver);
    while (!h2o_linklist_is_empty(messages)) {
        response_message *message = H2O_STRUCT_FROM_MEMBER(response_message, super.link, messages->next);
        h2o_linklist_unlink(&message->super.link);
        h2o_req_t *request = NULL;
        for (size_t i = 0; i < CAPACITY; ++i) {
            if (server->slots[i].id == message->id) {
                request = server->slots[i].request;
                server->slots[i].id = 0;
                server->slots[i].request = NULL;
                break;
            }
        }
        if (request != NULL) {
            request->res.status = message->status;
            request->res.reason = message->status == 200 ? "OK" : "Service Unavailable";
            request->res.content_length = message->len;
            h2o_add_header(&request->pool, &request->res.headers, H2O_TOKEN_CONTENT_TYPE,
                           NULL, H2O_STRLIT("application/octet-stream"));
            h2o_send_inline(request, message->body, message->len);
            atomic_fetch_add(&server->answered, 1);
        } else {
            atomic_fetch_add(&server->stale, 1);
        }
        free(message);
    }
}

static void on_socket_closed(void *data)
{
    connection *item = data;
    *item->previous = item->next;
    if (item->next != NULL) item->next->previous = item->previous;
    free(item);
}

static void on_accept(h2o_socket_t *listener, const char *error)
{
    if (error != NULL) return;
    probe_server *server = listener->data;
    h2o_socket_t *socket = h2o_evloop_socket_accept(listener);
    if (socket == NULL) return;
    connection *item = malloc(sizeof(*item));
    if (item == NULL) { h2o_socket_close(socket); return; }
    *item = (connection){server->connections, &server->connections, socket};
    if (item->next != NULL) item->next->previous = &item->next;
    server->connections = item;
    socket->on_close.cb = on_socket_closed;
    socket->on_close.data = item;
    h2o_accept(&server->accept, socket);
}

static int open_listener(probe_server *server)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(address);
    if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0 ||
        bind(fd, (struct sockaddr *)&address, len) != 0 || listen(fd, 128) != 0 ||
        getsockname(fd, (struct sockaddr *)&address, &len) != 0) {
        int saved = errno; close(fd); errno = saved; return -1;
    }
    server->port = ntohs(address.sin_port);
    server->listener = h2o_evloop_socket_create(server->context.loop, fd, H2O_SOCKET_FLAG_DONT_READ);
    server->listener->data = server;
    h2o_socket_read_start(server->listener, on_accept);
    return 0;
}

static void *run_loop(void *data)
{
    probe_server *server = data;
    h2o_config_init(&server->config);
    server->config.max_request_entity_size = MAX_BODY;
    h2o_hostconf_t *host = h2o_config_register_host(&server->config, h2o_iovec_init(H2O_STRLIT("default")), 65535);
    h2o_pathconf_t *path = h2o_config_register_path(host, "/", 0);
    probe_handler *handler = (probe_handler *)h2o_create_handler(path, sizeof(*handler));
    handler->super.on_req = on_request;
    handler->server = server;
    h2o_context_init(&server->context, h2o_evloop_create(), &server->config);
    server->accept.ctx = &server->context;
    server->accept.hosts = server->config.hosts;
    h2o_multithread_register_receiver(server->context.queue, &server->receiver, on_messages);
    int result = open_listener(server);
    pthread_mutex_lock(&server->mutex);
    server->error = result == 0 ? 0 : errno;
    server->started = 1;
    pthread_cond_signal(&server->ready);
    pthread_mutex_unlock(&server->mutex);
    if (result == 0) {
        while (!atomic_load(&server->stopping)) {
            uint32_t max_wait = h2o_cleanup_thread(h2o_now(server->context.loop), &server->context);
            h2o_evloop_run(server->context.loop, max_wait);
        }
        h2o_evloop_run(server->context.loop, 0);
        h2o_socket_close(server->listener);
        h2o_context_request_shutdown(&server->context);
        for (size_t i = 0; i < CAPACITY; ++i) {
            h2o_req_t *request = server->slots[i].request;
            if (request != NULL) {
                server->slots[i].request = NULL;
                server->slots[i].id = 0;
                h2o_send_error_generic(request, 503, "Service Unavailable", "probe stopped",
                                       H2O_SEND_ERROR_HTTP1_CLOSE_CONNECTION);
            }
        }
        /* Force transport shutdown for this finite experiment, not a drain implementation. */
        for (connection *item = server->connections; item != NULL; item = item->next)
            shutdown(h2o_socket_get_fd(item->socket), SHUT_RDWR);
        while (server->connections != NULL) h2o_evloop_run(server->context.loop, 10);
    }
    h2o_multithread_unregister_receiver(server->context.queue, &server->receiver);
    h2o_loop_t *loop = server->context.loop;
    h2o_context_dispose(&server->context);
    h2o_evloop_destroy(loop);
    h2o_config_dispose(&server->config);
    /* These caches are thread-local: release them before the owning thread exits.
     * Explicit full clear is needed even after periodic cleanup has run. */
    h2o_buffer_clear_recycle(1);
    h2o_socket_clear_recycle(1);
    h2o_mem_clear_recycle(&h2o_mem_pool_allocator, 1);
    return NULL;
}

probe_server *j2o_probe_start(void)
{
    probe_server *server = calloc(1, sizeof(*server));
    if (server == NULL) return NULL;
    server->notify[0] = server->notify[1] = -1;
    pthread_mutex_init(&server->mutex, NULL);
    pthread_cond_init(&server->ready, NULL);
    if (pipe(server->notify) != 0) goto error;
    for (int i = 0; i < 2; ++i) {
        int flags = fcntl(server->notify[i], F_GETFL);
        if (flags < 0 || fcntl(server->notify[i], F_SETFL, flags | O_NONBLOCK) < 0 ||
            fcntl(server->notify[i], F_SETFD, FD_CLOEXEC) < 0) goto error;
    }
    if (pthread_create(&server->thread, NULL, run_loop, server) != 0) goto error;
    pthread_mutex_lock(&server->mutex);
    while (!server->started) pthread_cond_wait(&server->ready, &server->mutex);
    pthread_mutex_unlock(&server->mutex);
    if (server->error == 0) return server;
    pthread_join(server->thread, NULL);
error:
    if (server->notify[0] >= 0) close(server->notify[0]);
    if (server->notify[1] >= 0) close(server->notify[1]);
    pthread_mutex_destroy(&server->mutex);
    pthread_cond_destroy(&server->ready);
    free(server);
    return NULL;
}

int j2o_probe_port(probe_server *server) { return server->port; }
int j2o_probe_fd(probe_server *server) { return server->notify[0]; }

request_packet *j2o_probe_take(probe_server *server)
{
    pthread_mutex_lock(&server->mutex);
    char buffer[128];
    ssize_t result;
    do { result = read(server->notify[0], buffer, sizeof(buffer)); }
    while (result > 0 || (result < 0 && errno == EINTR));
    request_packet *packet = NULL;
    if (server->count != 0) {
        packet = server->queue[server->head];
        server->head = (server->head + 1) % CAPACITY;
        --server->count;
    }
    pthread_mutex_unlock(&server->mutex);
    return packet;
}

uint64_t j2o_probe_id(request_packet *packet) { return packet->id; }
int j2o_probe_version(request_packet *packet) { return packet->version; }
const char *j2o_probe_field(request_packet *packet, int field, size_t *length)
{
    switch (field) {
    case 0: *length = packet->method_len; return packet->data;
    case 1: *length = packet->path_len; return packet->data + packet->method_len;
    default: *length = packet->body_len; return packet->data + packet->method_len + packet->path_len;
    }
}
void j2o_probe_release(probe_server *server, request_packet *packet)
{
    free(packet);
    atomic_fetch_sub(&server->packets, 1);
}

int j2o_probe_respond(probe_server *server, uint64_t id, int status, const char *body, size_t len)
{
    if (len > MAX_RESPONSE || status < 200 || status > 599 || atomic_load(&server->stopping)) return -1;
    response_message *message = malloc(sizeof(*message) + len);
    if (message == NULL) return -1;
    message->super.link = (h2o_linklist_t){0};
    message->id = id;
    message->status = status;
    message->len = len;
    if (len != 0) memcpy(message->body, body, len);
    h2o_multithread_send_message(&server->receiver, &message->super);
    return 0;
}

uint64_t j2o_probe_stat(probe_server *server, int field)
{
    switch (field) {
    case 0: return atomic_load(&server->admitted);
    case 1: return atomic_load(&server->answered);
    case 2: return atomic_load(&server->stale);
    case 3: return atomic_load(&server->disposed);
    case 4: return atomic_load(&server->rejected);
    default: return atomic_load(&server->packets);
    }
}

void j2o_probe_stop(probe_server *server)
{
    atomic_store(&server->stopping, 1);
    h2o_multithread_send_message(&server->receiver, NULL);
    pthread_join(server->thread, NULL);
    while (server->count != 0) {
        free(server->queue[server->head]);
        server->head = (server->head + 1) % CAPACITY;
        --server->count;
        atomic_fetch_sub(&server->packets, 1);
    }
}
void j2o_probe_destroy(probe_server *server)
{
    close(server->notify[0]); close(server->notify[1]);
    pthread_mutex_destroy(&server->mutex);
    pthread_cond_destroy(&server->ready);
    free(server);
}

/* A deterministic negative-control hook: never touches the Scheme heap. */
void j2o_probe_hold(unsigned milliseconds)
{
    struct timespec delay = {milliseconds / 1000, (milliseconds % 1000) * 1000000L};
    atomic_store(&hold_active, 1);
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {}
    atomic_store(&hold_active, 0);
}
int j2o_probe_hold_active(void) { return atomic_load(&hold_active); }
