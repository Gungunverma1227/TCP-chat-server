// Simple chat client: uses select() to watch stdin and the socket at the same time.
// Usage: ./client [host] [port]

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    const char *port = argc > 2 ? argv[2] : "5000";

    addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0 || !res) {
        fprintf(stderr, "Could not resolve %s:%s\n", host, port);
        return 1;
    }

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0 || connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect");
        return 1;
    }
    freeaddrinfo(res);

    char buf[4096];
    bool stdin_open = true;
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        if (stdin_open) FD_SET(STDIN_FILENO, &rfds);
        FD_SET(fd, &rfds);

        if (select(fd + 1, &rfds, nullptr, nullptr, nullptr) < 0) {
            perror("select");
            break;
        }

        if (FD_ISSET(fd, &rfds)) {
            ssize_t n = recv(fd, buf, sizeof buf, 0);
            if (n <= 0) {
                printf("Disconnected from server.\n");
                break;
            }
            fwrite(buf, 1, (size_t)n, stdout);
            fflush(stdout);
        }

        if (stdin_open && FD_ISSET(STDIN_FILENO, &rfds)) {
            ssize_t n = read(STDIN_FILENO, buf, sizeof buf);
            if (n <= 0) {
                // stdin closed (e.g. piped input): half-close and keep reading replies
                shutdown(fd, SHUT_WR);
                stdin_open = false;
                continue;
            }
            ssize_t off = 0;
            while (off < n) {
                ssize_t w = send(fd, buf + off, (size_t)(n - off), MSG_NOSIGNAL);
                if (w <= 0) { perror("send"); close(fd); return 1; }
                off += w;
            }
        }
    }
    close(fd);
    return 0;
}
