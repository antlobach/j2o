#include "../../native/j2o_bridge.h"
#include <arpa/inet.h>
#include <assert.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int connect_to(unsigned port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons((uint16_t)port);
    assert(connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0);
    return fd;
}

static void readable(int fd)
{
    struct pollfd p = {fd, POLLIN, 0};
    assert(poll(&p, 1, 5000) == 1);
    assert((p.revents & (POLLIN | POLLHUP)) != 0);
}

static void read_exact(int fd, void *buffer, size_t length)
{
    size_t used = 0;
    while (used < length) {
        readable(fd);
        ssize_t n = read(fd, (char *)buffer + used, length - used);
        assert(n > 0);
        used += (size_t)n;
    }
}

static void read_response(int fd, char *buffer, size_t capacity)
{
    size_t used = 0;
    for (;;) {
        readable(fd);
        assert(used + 1 < capacity);
        ssize_t n = read(fd, buffer + used, capacity - used - 1);
        assert(n >= 0);
        if (n == 0) break;
        used += (size_t)n;
    }
    buffer[used] = 0;
}

static j2o_event *next_event(j2o_server *server)
{
    j2o_error error;
    j2o_event *event = NULL;
    readable(j2o_server_event_fd(server));
    assert(j2o_event_take(server, &event, &error) == J2O_OK);
    assert(event != NULL);
    return event;
}

static void field_is(j2o_event *event, unsigned field, const char *expected, size_t length)
{
    size_t actual;
    const char *value = j2o_event_field(event, field, 0, &actual);
    assert(actual == length && memcmp(value, expected, length) == 0);
}

static void send_response(j2o_server *server, j2o_id id, int status, const char *body)
{
    j2o_error error;
    j2o_response *response = j2o_response_create(&id, status, body, strlen(body), &error);
    assert(response != NULL);
    assert(j2o_response_header(response, "x-reply", 7, "yes", 3, &error) == J2O_OK);
    assert(j2o_response_submit(server, response, &error) == J2O_OK);
}

static void http2_request(j2o_server *server)
{
    int client = connect_to(j2o_server_port(server));
    static const unsigned char request[] =
        "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
        "\x00\x00\x00\x04\x00\x00\x00\x00\x00"
        "\x00\x00\x11\x01\x05\x00\x00\x00\x01"
        "\x82\x86\x84\x01\x0c" "example.test";
    assert(write(client, request, sizeof(request) - 1) == (ssize_t)(sizeof(request) - 1));
    j2o_event *event = next_event(server);
    assert(j2o_event_kind(event) == 1 && j2o_event_version(event) == 0x200);
    field_is(event, 1, "GET", 3);
    field_is(event, 2, "/", 1);
    field_is(event, 4, "example.test", 12);
    j2o_id id;
    j2o_event_id(event, &id);
    j2o_event_release(event);
    send_response(server, id, 200, "h2-reply");
    int saw_headers = 0, ended = 0;
    char body[128]; size_t body_size = 0;
    while (!ended) {
        unsigned char frame[9], payload[16384];
        read_exact(client, frame, sizeof(frame));
        size_t length = ((size_t)frame[0] << 16) | ((size_t)frame[1] << 8) | frame[2];
        assert(length <= sizeof(payload));
        read_exact(client, payload, length);
        unsigned stream = ((unsigned)(frame[5] & 0x7f) << 24) | ((unsigned)frame[6] << 16) |
                          ((unsigned)frame[7] << 8) | frame[8];
        if (stream == 1 && frame[3] == 1) saw_headers = 1;
        if (stream == 1 && frame[3] == 0) {
            assert(body_size + length <= sizeof(body));
            memcpy(body + body_size, payload, length);
            body_size += length;
        }
        if (stream == 1 && (frame[3] == 0 || frame[3] == 1) && (frame[4] & 1)) ended = 1;
    }
    assert(saw_headers && body_size == 8 && memcmp(body, "h2-reply", 8) == 0);
    close(client);
}

static void http2_reset(j2o_server *server)
{
    int client = connect_to(j2o_server_port(server));
    static const unsigned char request[] =
        "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
        "\x00\x00\x00\x04\x00\x00\x00\x00\x00"
        "\x00\x00\x11\x01\x05\x00\x00\x00\x01"
        "\x82\x86\x84\x01\x0c" "example.test";
    static const unsigned char reset[] = "\x00\x00\x04\x03\x00\x00\x00\x00\x01\x00\x00\x00\x08";
    assert(write(client, request, sizeof(request) - 1) == (ssize_t)(sizeof(request) - 1));
    j2o_event *event = next_event(server);
    assert(j2o_event_kind(event) == 1);
    j2o_id id, retired;
    j2o_event_id(event, &id);
    j2o_event_release(event);
    assert(write(client, reset, sizeof(reset) - 1) == (ssize_t)(sizeof(reset) - 1));
    event = next_event(server);
    assert(j2o_event_kind(event) == 2);
    j2o_event_id(event, &retired);
    assert(retired.slot == id.slot && retired.generation == id.generation);
    j2o_event_release(event);
    send_response(server, id, 200, "late");
    close(client);
}

int main(void)
{
    j2o_error error;
    j2o_config *config = j2o_config_create(&error);
    assert(config != NULL);
    assert(j2o_config_set_u64(config, J2O_PORT, 0, &error) == J2O_OK);
    assert(j2o_config_set_u64(config, J2O_MAX_LIVE_REQUESTS, 1, &error) == J2O_OK);
    j2o_server *server = NULL;
    assert(j2o_server_start(config, &server, &error) == J2O_OK);
    j2o_config_destroy(config);
    int client = connect_to(j2o_server_port(server));
    const char request[] = "POST /a?b=c HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\nConnection: close\r\n\r\na\0b";
    assert(write(client, request, sizeof(request) - 1) == (ssize_t)(sizeof(request) - 1));
    j2o_event *event = next_event(server);
    assert(j2o_event_kind(event) == 1 && j2o_event_version(event) == 0x101);
    field_is(event, 1, "POST", 4);
    field_is(event, 2, "/a?b=c", 6);
    field_is(event, 3, "a\0b", 3);
    field_is(event, 4, "example.test", 12);
    j2o_id old;
    j2o_event_id(event, &old);
    j2o_event_release(event);
    send_response(server, old, 200, "first");
    char wire[4096];
    read_response(client, wire, sizeof(wire));
    assert(strstr(wire, "200") != NULL && strstr(wire, "first") != NULL);
    close(client);
    event = next_event(server);
    assert(j2o_event_kind(event) == 2);
    j2o_event_release(event);

    client = connect_to(j2o_server_port(server));
    const char second[] = "GET /second HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n";
    assert(write(client, second, sizeof(second) - 1) == (ssize_t)(sizeof(second) - 1));
    event = next_event(server);
    assert(j2o_event_kind(event) == 1);
    j2o_id current;
    j2o_event_id(event, &current);
    assert(current.slot == old.slot && current.generation != old.generation);
    j2o_event_release(event);
    send_response(server, old, 200, "stale");
    send_response(server, current, 200, "second");
    read_response(client, wire, sizeof(wire));
    assert(strstr(wire, "second") != NULL && strstr(wire, "stale") == NULL);
    close(client);
    event = next_event(server);
    assert(j2o_event_kind(event) == 2);
    j2o_event_release(event);
    client = connect_to(j2o_server_port(server));
    assert(write(client, second, sizeof(second) - 1) == (ssize_t)(sizeof(second) - 1));
    event = next_event(server);
    assert(j2o_event_kind(event) == 1);
    j2o_event_id(event, &current);
    j2o_event_release(event);
    send_response(server, current, 204, "forbidden-body");
    read_response(client, wire, sizeof(wire));
    assert(strstr(wire, "204") != NULL && strstr(wire, "forbidden-body") == NULL);
    close(client);
    event = next_event(server);
    assert(j2o_event_kind(event) == 2);
    j2o_event_release(event);
    http2_request(server);
    event = next_event(server);
    assert(j2o_event_kind(event) == 2);
    j2o_event_release(event);
    http2_reset(server);
    assert(j2o_server_force_stop(server, &error) == J2O_OK);
    assert(j2o_server_join(server, 5000, &error) == J2O_OK);
    j2o_server_destroy(server);
    puts("PASS: HTTP/1 and HTTP/2 unary requests, binary body, bodyless status and generation isolation");
    return 0;
}
