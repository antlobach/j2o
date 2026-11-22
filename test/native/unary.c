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

static void send_response(j2o_server *server, j2o_id id, const char *body)
{
    j2o_error error;
    j2o_response *response = j2o_response_create(&id, 200, body, strlen(body), &error);
    assert(response != NULL);
    assert(j2o_response_header(response, "x-reply", 7, "yes", 3, &error) == J2O_OK);
    assert(j2o_response_submit(server, response, &error) == J2O_OK);
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
    send_response(server, old, "first");
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
    send_response(server, old, "stale");
    send_response(server, current, "second");
    read_response(client, wire, sizeof(wire));
    assert(strstr(wire, "second") != NULL && strstr(wire, "stale") == NULL);
    close(client);
    assert(j2o_server_force_stop(server, &error) == J2O_OK);
    assert(j2o_server_join(server, 5000, &error) == J2O_OK);
    j2o_server_destroy(server);
    puts("PASS: unary HTTP request, binary body, response and generation isolation");
    return 0;
}
