/* Exercise the exact bridge implementation under ASan/UBSan, without Chez. */
#include "bridge.c"
#include <assert.h>
#include <poll.h>
#include <stdio.h>
#include <sys/time.h>
#include <spawn.h>
#include <signal.h>
#include <sys/wait.h>

extern char **environ;

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
    if (server->ingress_mode == 1) return j2o_probe_await(server);
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
    if (status == 201) assert(strncmp(header, "HTTP/1.1 201 Created\r\n", 22) == 0);
    if (status == 500) assert(strncmp(header, "HTTP/1.1 500 Internal Server Error\r\n", 36) == 0);
    if (status == 599) assert(strncmp(header, "HTTP/1.1 599 Unknown\r\n", 22) == 0);
    return status;
}

static void submit_response(probe_server *server, uint64_t id, int status,
                            const char *body, size_t length, profile_trace *trace)
{
    response_message *message = j2o_probe_response_create(id, status, body, length, trace);
    assert(message != NULL);
    assert(j2o_probe_response_submit(server, message) == 0);
}

static void queued_readiness(void)
{
    probe_server *server = j2o_probe_start();
    assert(server != NULL);
    int clients[3];
    const char *request = "GET /queued HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    for (size_t i = 0; i < 3; ++i) {
        clients[i] = connect_client(server);
        send_all(clients[i], request, strlen(request));
    }
    for (unsigned attempt = 0; j2o_probe_stat(server, 0) != 3; ++attempt) {
        assert(attempt < 3000);
        struct timespec pause = {0, 1000000};
        nanosleep(&pause, NULL);
    }
    /* All producer writes precede these three independent readiness cycles. */
    for (size_t i = 0; i < 3; ++i) {
        request_packet *packet = take_packet(server);
        submit_response(server, packet->id, 200, "", 0, NULL);
        j2o_probe_release(server, packet);
    }
    struct pollfd fd = {j2o_probe_fd(server), POLLIN, 0};
    assert(poll(&fd, 1, 0) == 0);
    for (size_t i = 0; i < 3; ++i) {
        assert(read_header(clients[i]) == 200);
        close(clients[i]);
    }
    j2o_probe_stop(server);
    assert(j2o_probe_stat(server, 5) == 0);
    j2o_probe_destroy(server);
}

static void *idle_waiter(void *data)
{
    assert(j2o_probe_await(data) == NULL);
    return NULL;
}

int main(void)
{
    queued_readiness();
    unsigned char payload[MAX_BODY + 1], result[MAX_BODY];
    for (size_t i = 0; i < sizeof(payload); ++i) payload[i] = (unsigned char)i;
    for (int round = 0; round < 10; ++round) {
        probe_server *server = j2o_probe_start();
        assert(server != NULL);
        assert(j2o_probe_ingress_mode(server, round % 2) == 0);
        if (round % 2 != 0) {
            assert(j2o_probe_profile_configure(server, 2, 1) == 0);
            assert(j2o_probe_profile_recording(server, 1) == 0);
        }
        const size_t lengths[] = {0, 1, MAX_BODY};
        const int statuses[] = {201, 500, 599};
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
            assert(j2o_probe_response_create(id, 200, NULL, MAX_RESPONSE + 1, NULL) == NULL);
            submit_response(server, id, statuses[test], body, length, j2o_probe_profile_trace(packet));
            j2o_probe_release(server, packet);
            assert(read_header(client) == statuses[test]);
            size_t offset = 0;
            while (offset < length) {
                ssize_t count = recv(client, result + offset, length - offset, 0);
                assert(count > 0);
                offset += (size_t)count;
            }
            assert(memcmp(result, payload, length) == 0);
            close(client);
            submit_response(server, id, 200, "late", 4, NULL);
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
        pid_t child;
        char *arguments[] = {"sleep", "10", NULL};
        assert(posix_spawn(&child, "/bin/sleep", NULL, NULL, arguments, environ) == 0);
        int port = j2o_probe_port(server);
        pthread_t waiter;
        assert(pthread_create(&waiter, NULL, idle_waiter, server) == 0);
        j2o_probe_stop(server);
        assert(pthread_join(waiter, NULL) == 0);
        assert(j2o_probe_stat(server, 5) == 0);
        assert(j2o_probe_stat(server, 6) == 0);
        assert(j2o_probe_stat(server, 2) == 3);
        response_message *unsubmitted = j2o_probe_response_create(1, 200, "stopped", 7, NULL);
        assert(unsubmitted != NULL);
        assert(j2o_probe_response_submit(server, unsubmitted) == -1);
        j2o_probe_response_release(unsubmitted);
        if (round % 2 != 0) {
            uint64_t data[PROFILE_WORDS];
            assert(j2o_probe_profile_info(server, 1) == 2);
            assert(j2o_probe_profile_info(server, 2) == j2o_probe_profile_info(server, 0) - 2);
            /* Native-only replies have no managed stages: never export them as complete. */
            assert(j2o_probe_profile_copy(server, 0, data, PROFILE_WORDS) == 0);
        }
        close(pending);
        j2o_probe_destroy(server);
        int extra;
        struct sockaddr_in local = {0};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        /* A stopped ephemeral listener port can otherwise become the client's
         * own source port on Linux, producing a TCP self-connection. */
        for (int attempt = 0;; ++attempt) {
            assert(attempt < 100);
            extra = socket(AF_INET, SOCK_STREAM, 0);
            assert(extra >= 0);
            local.sin_port = 0;
            assert(bind(extra, (struct sockaddr *)&local, sizeof(local)) == 0);
            socklen_t length = sizeof(local);
            assert(getsockname(extra, (struct sockaddr *)&local, &length) == 0);
            if (ntohs(local.sin_port) != port) break;
            close(extra);
        }
        struct sockaddr_in address = {0};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        int accepted = connect(extra, (struct sockaddr *)&address, sizeof(address));
        int error = errno;
        close(extra);
        assert(kill(child, SIGTERM) == 0);
        assert(waitpid(child, NULL, 0) == child);
        if (accepted != -1 || error != ECONNREFUSED)
            fprintf(stderr, "Stopped-port probe: connect=%d errno=%d source=%u target=%d\n",
                    accepted, error, ntohs(local.sin_port), port);
        assert(accepted == -1 && error == ECONNREFUSED);
    }
    puts("PASS: native binary boundaries, both ingress paths, and waiter teardown (10 lifecycles)");
    return 0;
}
