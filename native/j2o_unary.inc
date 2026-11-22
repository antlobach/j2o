/* Included by j2o_bridge.c: H2O request objects remain owner-thread confined. */
typedef struct { h2o_handler_t super; j2o_server *server; } unary_handler;
typedef struct { j2o_server *server; j2o_id id; } request_guard;

static void notify_event(j2o_server *server)
{
    char byte = 1;
    ssize_t result;
    do { result = write(server->events[1], &byte, 1); } while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) abort();
}
static void enqueue_event_locked(j2o_server *server, j2o_event *event)
{
    event->next = NULL;
    if (server->event_tail != NULL) server->event_tail->next = event;
    else { server->event_head = event; notify_event(server); }
    server->event_tail = event;
    ++server->event_count;
}
void j2o_event_release(j2o_event *event)
{
    if (event != NULL) { deallocate(event->fields); deallocate(event); }
}
static void retired_request(void *data)
{
    request_guard *guard = data;
    j2o_server *server = guard->server;
    request_slot *slot = server->slots + guard->id.slot;
    if (slot->generation != guard->id.generation || slot->request == NULL) return;
    slot->request = NULL;
    pthread_mutex_lock(&server->mutex);
    --server->live_requests;
    enqueue_event_locked(server, slot->retired);
    slot->retired = NULL;
    pthread_mutex_unlock(&server->mutex);
}
static int add_size(size_t *total, size_t amount)
{
    if (amount > SIZE_MAX - *total) return 0;
    *total += amount;
    return 1;
}
static int on_unary_request(h2o_handler_t *handler, h2o_req_t *request)
{
    j2o_server *server = ((unary_handler *)handler)->server;
    size_t index;
    for (index = 0; index < server->slot_count; ++index)
        if (server->slots[index].request == NULL && server->slots[index].retired == NULL) break;
    pthread_mutex_lock(&server->mutex);
    size_t capacity = server->config.values[J2O_MAX_EVENT_QUEUE];
    int full = atomic_load(&server->state) != RUNNING || index == server->slot_count ||
        server->event_count > capacity || capacity - server->event_count < 2 ||
        server->live_requests > (capacity - server->event_count - 2) / 2;
    pthread_mutex_unlock(&server->mutex);
    if (full) { h2o_send_error_generic(request, 503, "Service Unavailable", "Server busy", 0); return 0; }
    size_t headers = request->headers.size;
    if (headers > (SIZE_MAX / sizeof(h2o_iovec_t) - 5) / 2) {
        h2o_send_error_generic(request, 431, "Request Header Fields Too Large", "Headers too large", 0); return 0;
    }
    size_t fields = 5 + 2 * headers, bytes = fields * sizeof(h2o_iovec_t), header_bytes = 0;
    struct sockaddr_storage peer;
    socklen_t peer_len = request->conn->callbacks->get_peername(request->conn, (struct sockaddr *)&peer);
    char peer_text[NI_MAXHOST] = "";
    if (peer_len != 0) getnameinfo((struct sockaddr *)&peer, peer_len, peer_text, sizeof(peer_text), NULL, 0, NI_NUMERICHOST);
    h2o_iovec_t base[] = {request->input.method, request->input.path, request->entity,
                          request->input.authority, h2o_iovec_init(peer_text, strlen(peer_text))};
    for (size_t i = 0; i < 5; ++i) if (!add_size(&bytes, base[i].len)) goto too_large;
    for (size_t i = 0; i < headers; ++i) {
        h2o_header_t *h = request->headers.entries + i;
        if (!add_size(&header_bytes, h->name->len) || !add_size(&header_bytes, h->value.len) ||
            !add_size(&bytes, h->name->len) || !add_size(&bytes, h->value.len)) goto too_large;
    }
    if (header_bytes > server->config.values[J2O_MAX_HEADER_BYTES]) goto too_large;
    j2o_event *event = allocate(sizeof(*event)), *retired = allocate(sizeof(*retired));
    if (event == NULL || retired == NULL) { deallocate(event); deallocate(retired); goto exhausted; }
    event->fields = allocate(bytes);
    if (event->fields == NULL) { deallocate(event); deallocate(retired); goto exhausted; }
    char *out = (char *)(event->fields + fields);
    for (size_t i = 0; i < fields; ++i) {
        h2o_iovec_t value;
        if (i < 5) value = base[i];
        else {
            h2o_header_t *h = request->headers.entries + (i - 5) / 2;
            value = ((i - 5) % 2 == 0) ? *h->name : h->value;
        }
        event->fields[i] = h2o_iovec_init(out, value.len);
        if (value.len != 0) { memcpy(out, value.base, value.len); out += value.len; }
    }
    request_slot *slot = server->slots + index;
    if (++slot->generation == 0) ++slot->generation;
    event->id = (j2o_id){0, (uint32_t)index, slot->generation};
    event->kind = 1; event->version = request->version; event->headers = headers;
    retired->kind = 2; retired->id = event->id;
    slot->request = request; slot->retired = retired; slot->claimed = 0;
    request_guard *guard = h2o_mem_alloc_shared(&request->pool, sizeof(*guard), retired_request);
    *guard = (request_guard){server, event->id};
    pthread_mutex_lock(&server->mutex);
    ++server->live_requests;
    enqueue_event_locked(server, event);
    pthread_mutex_unlock(&server->mutex);
    return 0;
too_large:
    h2o_send_error_generic(request, 431, "Request Header Fields Too Large", "Headers too large", 0); return 0;
exhausted:
    h2o_send_error_generic(request, 503, "Service Unavailable", "Server allocation failed", 0); return 0;
}
static void connection_closed(void *data)
{
    j2o_connection *connection = data;
    *connection->previous = connection->next;
    if (connection->next != NULL) connection->next->previous = connection->previous;
    --connection->server->connection_count;
    deallocate(connection);
}
static void accept_unary(h2o_socket_t *listener_socket, const char *error)
{
    if (error != NULL) return;
    j2o_server *server = listener_socket->data;
    h2o_socket_t *socket = h2o_evloop_socket_accept(listener_socket);
    if (socket == NULL) return;
    if (server->connection_count >= server->config.values[J2O_MAX_CONNECTIONS] || atomic_load(&server->state) != RUNNING) {
        h2o_socket_close(socket); return;
    }
    j2o_connection *connection = allocate(sizeof(*connection));
    if (connection == NULL) { h2o_socket_close(socket); return; }
    *connection = (j2o_connection){server->connections, &server->connections, server, socket};
    if (connection->next != NULL) connection->next->previous = &connection->next;
    server->connections = connection; ++server->connection_count;
    socket->on_close.cb = connection_closed; socket->on_close.data = connection;
    h2o_accept(&server->accept, socket);
}
unsigned j2o_server_port(j2o_server *server) { return server == NULL ? 0 : server->port; }
j2o_result j2o_event_take(j2o_server *server, j2o_event **out, j2o_error *error)
{
    if (out != NULL) *out = NULL;
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (server == NULL || out == NULL) return invalid(error, "event", NULL, "Server and output are required");
    pthread_mutex_lock(&server->mutex);
    j2o_event *event = server->event_head;
    if (event != NULL) {
        server->event_head = event->next;
        if (server->event_head == NULL) {
            server->event_tail = NULL;
            char byte; ssize_t result;
            do { result = read(server->events[0], &byte, 1); } while (result < 0 && errno == EINTR);
        }
        --server->event_count;
    }
    *out = event;
    pthread_mutex_unlock(&server->mutex);
    return J2O_OK;
}
unsigned j2o_event_kind(const j2o_event *event) { return event->kind; }
void j2o_event_id(const j2o_event *event, j2o_id *id) { *id = event->id; }
int j2o_event_version(const j2o_event *event) { return event->version; }
size_t j2o_event_headers(const j2o_event *event) { return event->headers; }
const char *j2o_event_field(const j2o_event *event, unsigned field, size_t index, size_t *length)
{
    size_t i = field >= 1 && field <= 5 ? field - 1 :
        (field == 6 || field == 7) && index < event->headers ? 5 + index * 2 + (field - 6) : SIZE_MAX;
    if (i == SIZE_MAX || event->fields == NULL) { *length = 0; return NULL; }
    *length = event->fields[i].len; return event->fields[i].base;
}
j2o_response *j2o_response_create(const j2o_id *id, int status, const void *body, size_t length, j2o_error *error)
{
    if (error == NULL) return NULL;
    success(error);
    if (id == NULL || status < 100 || status > 599 || (length != 0 && body == NULL) || length > SIZE_MAX - sizeof(j2o_response)) {
        invalid(error, "response", NULL, "Invalid response"); return NULL;
    }
    j2o_response *response = allocate(sizeof(*response) + length);
    if (response == NULL) { no_memory(error); return NULL; }
    response->id = *id; response->status = status; response->length = length; response->bytes = sizeof(*response) + length;
    if (length != 0) memcpy(response->body, body, length);
    return response;
}
void j2o_response_release(j2o_response *response)
{
    if (response == NULL) return;
    for (size_t i = 0; i < response->header_count; ++i) deallocate(response->headers[i].name);
    deallocate(response->headers); deallocate(response);
}
j2o_result j2o_response_header(j2o_response *response, const char *name, size_t name_len,
                               const char *value, size_t value_len, j2o_error *error)
{
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (response == NULL || name == NULL || value == NULL || name_len == 0 ||
        value_len > SIZE_MAX - 2 || name_len > SIZE_MAX - value_len - 2 || memchr(value, '\r', value_len) ||
        memchr(value, '\n', value_len) || memchr(value, 0, value_len))
        return invalid(error, "header", NULL, "Invalid response header");
    for (size_t i = 0; i < name_len; ++i) {
        unsigned char c = (unsigned char)name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || (c != 0 && strchr("!#$%&'*+-.^_`|~", c) != NULL)))
            return invalid(error, "header", NULL, "Invalid response header name");
    }
    if ((name_len == 14 && strncasecmp(name, "content-length", 14) == 0) ||
        (name_len == 17 && strncasecmp(name, "transfer-encoding", 17) == 0)) return J2O_OK;
    size_t count = response->header_count, capacity = response->header_capacity;
    size_t growth = 0;
    if (count == capacity) {
        if (capacity > SIZE_MAX / 2 / sizeof(response_header)) return no_memory(error);
        size_t next = capacity == 0 ? 4 : capacity * 2;
        growth = (next - capacity) * sizeof(response_header);
        capacity = next;
    }
    size_t storage = name_len + value_len + 2;
    if (growth > SIZE_MAX - storage || response->bytes > SIZE_MAX - storage - growth) return no_memory(error);
    char *n = allocate(storage);
    if (n == NULL) return no_memory(error);
    if (growth != 0) {
        response_header *headers = allocate(capacity * sizeof(*headers));
        if (headers == NULL) { deallocate(n); return no_memory(error); }
        if (count != 0) memcpy(headers, response->headers, count * sizeof(*headers));
        deallocate(response->headers);
        response->headers = headers; response->header_capacity = capacity;
    }
    for (size_t i = 0; i < name_len; ++i)
        n[i] = name[i] >= 'A' && name[i] <= 'Z' ? (char)(name[i] + ('a' - 'A')) : name[i];
    n[name_len] = 0;
    char *v = n + name_len + 1;
    memcpy(v, value, value_len); v[value_len] = 0;
    response->headers[count] = (response_header){n, name_len, v, value_len};
    response->header_count = count + 1;
    response->bytes += storage + growth;
    return J2O_OK;
}
j2o_result j2o_response_submit(j2o_server *server, j2o_response *response, j2o_error *error)
{
    if (error == NULL) return J2O_INVALID;
    success(error);
    if (server == NULL || response == NULL) return invalid(error, "response", NULL, "Server and response are required");
    pthread_mutex_lock(&server->mutex);
    int state = atomic_load(&server->state);
    if (state != RUNNING && state != DRAINING) {
        pthread_mutex_unlock(&server->mutex); return fail(error, J2O_STOPPED, 0, NULL, NULL, "Server stopped", NULL, NULL);
    }
    if (response->bytes > server->config.values[J2O_MAX_QUEUED_BYTES]) {
        pthread_mutex_unlock(&server->mutex);
        return invalid(error, "response", NULL, "Response exceeds queued-byte limit");
    }
    if (server->response_count >= server->config.values[J2O_MAX_RESPONSE_QUEUE] ||
        response->bytes > server->config.values[J2O_MAX_QUEUED_BYTES] - server->response_bytes) {
        pthread_mutex_unlock(&server->mutex); return fail(error, J2O_QUEUE_FULL, 0, NULL, NULL, "Response queue full", NULL, NULL);
    }
    char byte = 1; ssize_t result;
    do { result = write(server->control[1], &byte, 1); } while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        int code = errno;
        pthread_mutex_unlock(&server->mutex);
        return os_failure(error, code, "write");
    }
    response->next = NULL;
    if (server->response_tail != NULL) server->response_tail->next = response; else server->response_head = response;
    server->response_tail = response; ++server->response_count; server->response_bytes += response->bytes;
    pthread_mutex_unlock(&server->mutex);
    return J2O_OK;
}
static void process_responses(j2o_server *server)
{
    pthread_mutex_lock(&server->mutex);
    j2o_response *response = server->response_head;
    server->response_head = server->response_tail = NULL;
    server->response_count = server->response_bytes = 0;
    pthread_mutex_unlock(&server->mutex);
    while (response != NULL) {
        j2o_response *next = response->next;
        if (response->id.loop == 0 && response->id.slot < server->slot_count) {
            request_slot *slot = server->slots + response->id.slot;
            h2o_req_t *request = slot->generation == response->id.generation && !slot->claimed ? slot->request : NULL;
            if (request != NULL) {
                slot->claimed = 1;
                request->res.status = response->status; request->res.reason = "Response";
                int bodyless = response->status < 200 || response->status == 204 || response->status == 304;
                request->res.content_length = response->status < 200 || response->status == 204 ? SIZE_MAX : response->length;
                for (size_t i = 0; i < response->header_count; ++i) {
                    response_header *h = response->headers + i;
                    h2o_iovec_t name = h2o_strdup(&request->pool, h->name, h->name_len);
                    h2o_iovec_t value = h2o_strdup(&request->pool, h->value, h->value_len);
                    h2o_add_header_by_str(&request->pool, &request->res.headers, name.base, name.len, 1, NULL, value.base, value.len);
                }
                h2o_send_inline(request, response->body, bodyless ? 0 : response->length);
            }
        }
        j2o_response_release(response); response = next;
    }
}
