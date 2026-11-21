/* Exercise the exact bridge implementation under ASan/UBSan, without Chez. */
#include "bridge.c"
#include <assert.h>
#include <poll.h>
#include <stdio.h>
#include <sys/time.h>

static int connect_client(probe_server *server)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(j2o_probe_port(server));
    assert(connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0);
    struct timeval deadline = {3, 0};
    assert(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &deadline, sizeof(deadline)) == 0);
    return fd;
}

static void send_all(int fd, const void *data, size_t length)
{
    size_t offset = 0;
    while (offset < length) {
        ssize_t count = send(fd, (const char *)data + offset, length - offset, 0);
        assert(count > 0);
        offset += (size_t)count;
    }
}

static request_packet *take_packet(probe_server *server)
{
    struct pollfd fd = {j2o_probe_fd(server), POLLIN, 0};
    assert(poll(&fd, 1, 3000) == 1);
    request_packet *packet = j2o_probe_take(server);
    assert(packet != NULL);
    return packet;
}

static int read_header(int fd)
{
    char header[8192];
    size_t count = 0;
    for (;;) {
        assert(count + 1 < sizeof(header));
        assert(recv(fd, header + count, 1, 0) == 1);
        ++count;
        if (count >= 4 && memcmp(header + count - 4, "\r\n\r\n", 4) == 0) break;
    }
    header[count] = 0;
    int status = 0;
    assert(sscanf(header, "HTTP/%*s %d", &status) == 1);
    return status;
}

int main(void)
{
    unsigned char payload[MAX_BODY + 1], result[MAX_BODY];
    for (size_t i = 0; i < sizeof(payload); ++i) payload[i] = (unsigned char)i;
    for (int round = 0; round < 10; ++round) {
        probe_server *server = j2o_probe_start();
        assert(server != NULL);
        const size_t lengths[] = {0, 1, MAX_BODY};
        for (size_t test = 0; test < sizeof(lengths) / sizeof(lengths[0]); ++test) {
            int client = connect_client(server);
            size_t length = lengths[test];
            char header[256];
            int n = snprintf(header, sizeof(header),
                "POST /echo HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: %zu\r\n\r\n", length);
            send_all(client, header, (size_t)n);
            send_all(client, payload, length);
            request_packet *packet = take_packet(server);
            size_t received;
            const char *body = j2o_probe_field(packet, 2, &received);
            assert(received == length);
            assert(memcmp(body, payload, length) == 0);
            uint64_t id = j2o_probe_id(packet);
            assert(j2o_probe_respond(server, id, 200, NULL, MAX_RESPONSE + 1) == -1);
            assert(j2o_probe_respond(server, id, 200, body, length) == 0);
            j2o_probe_release(server, packet);
            assert(read_header(client) == 200);
            size_t offset = 0;
            while (offset < length) {
                ssize_t count = recv(client, result + offset, length - offset, 0);
                assert(count > 0);
                offset += (size_t)count;
            }
            assert(memcmp(result, payload, length) == 0);
            close(client);
            assert(j2o_probe_respond(server, id, 200, "late", 4) == 0);
        }
        int oversized = connect_client(server);
        const char *header = "POST /echo HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 4097\r\n\r\n";
        send_all(oversized, header, strlen(header));
        assert(read_header(oversized) == 413);
        close(oversized);

        int pending = connect_client(server);
        const char *request = "GET /pending HTTP/1.1\r\nHost: localhost\r\n\r\n";
        send_all(pending, request, strlen(request));
        request_packet *packet = take_packet(server);
        j2o_probe_release(server, packet);
        j2o_probe_stop(server);
        assert(j2o_probe_stat(server, 5) == 0);
        assert(j2o_probe_stat(server, 2) == 3);
        close(pending);
        j2o_probe_destroy(server);
    }
    puts("PASS: native binary boundaries, stale replies, and pending-request teardown (10 lifecycles)");
    return 0;
}
