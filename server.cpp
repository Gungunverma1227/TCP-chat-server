// Multi-client TCP chat server using non-blocking sockets + epoll (Linux).
// Single-threaded event loop: handles thousands of clients without a thread per client.
//
// Commands:
//   /nick <name>         change nickname
//   /list                list online users
//   /msg <name> <text>   private message
//   /help                show commands
//   /quit                disconnect

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <unordered_map>
#include <unordered_set>

static const int MAX_EVENTS = 128;
static const size_t MAX_LINE = 1024;
static const size_t MAX_OUTBUF = 1 << 20;  // drop slow clients above 1 MB backlog

struct Client {
    int fd;
    std::string nick;
    std::string inbuf;   // partial line accumulator
    std::string outbuf;  // pending data to write
    std::string addr;
    bool want_close = false;
};

static std::unordered_map<int, Client> clients;
static std::unordered_map<std::string, int> nick_to_fd;
static int epfd = -1;
static volatile sig_atomic_t running = 1;

static std::string now_str() {
    char buf[16];
    time_t t = time(nullptr);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(buf, sizeof buf, "%H:%M:%S", &tmv);
    return buf;
}

static void log(const std::string &s) {
    fprintf(stderr, "[%s] %s\n", now_str().c_str(), s.c_str());
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void update_events(Client &c) {
    epoll_event ev{};
    ev.data.fd = c.fd;
    ev.events = EPOLLIN | EPOLLRDHUP;
    if (!c.outbuf.empty()) ev.events |= EPOLLOUT;
    epoll_ctl(epfd, EPOLL_CTL_MOD, c.fd, &ev);
}

static void flush_client(Client &c) {
    while (!c.outbuf.empty()) {
        ssize_t n = send(c.fd, c.outbuf.data(), c.outbuf.size(), MSG_NOSIGNAL);
        if (n > 0) {
            c.outbuf.erase(0, (size_t)n);
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else {
            c.want_close = true;
            break;
        }
    }
    update_events(c);
}

static void send_to(Client &c, const std::string &msg) {
    if (c.outbuf.size() + msg.size() > MAX_OUTBUF) {
        c.want_close = true;  // too slow, cut it off
        return;
    }
    c.outbuf += msg;
    flush_client(c);
}

static void broadcast(const std::string &msg, int except_fd = -1) {
    for (auto &kv : clients) {
        if (kv.first == except_fd) continue;
        send_to(kv.second, msg);
    }
}

static void remove_client(int fd) {
    auto it = clients.find(fd);
    if (it == clients.end()) return;
    std::string nick = it->second.nick;
    nick_to_fd.erase(nick);
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    log(nick + " (" + it->second.addr + ") disconnected");
    clients.erase(it);
    broadcast("* " + nick + " left the chat\n");
}

static bool valid_nick(const std::string &n) {
    if (n.empty() || n.size() > 16) return false;
    for (char ch : n)
        if (!isalnum((unsigned char)ch) && ch != '_' && ch != '-') return false;
    return true;
}

static std::string trim(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r");
    return s.substr(a, b - a + 1);
}

static void handle_line(Client &c, const std::string &raw) {
    std::string line = trim(raw);
    if (line.empty()) return;

    if (line[0] != '/') {
        broadcast("[" + now_str() + "] " + c.nick + ": " + line + "\n", c.fd);
        send_to(c, "[" + now_str() + "] you: " + line + "\n");
        return;
    }

    std::string cmd, rest;
    size_t sp = line.find(' ');
    if (sp == std::string::npos) cmd = line;
    else { cmd = line.substr(0, sp); rest = trim(line.substr(sp + 1)); }

    if (cmd == "/quit") {
        send_to(c, "Bye!\n");
        c.want_close = true;
    } else if (cmd == "/help") {
        send_to(c, "Commands: /nick <name>, /list, /msg <name> <text>, /help, /quit\n");
    } else if (cmd == "/list") {
        std::string out = "Online (" + std::to_string(clients.size()) + "):";
        for (auto &kv : clients) out += " " + kv.second.nick;
        send_to(c, out + "\n");
    } else if (cmd == "/nick") {
        if (!valid_nick(rest)) {
            send_to(c, "! Invalid nick (1-16 chars: letters, digits, _ or -)\n");
        } else if (nick_to_fd.count(rest)) {
            send_to(c, "! Nick already taken\n");
        } else {
            std::string old = c.nick;
            nick_to_fd.erase(old);
            c.nick = rest;
            nick_to_fd[rest] = c.fd;
            broadcast("* " + old + " is now known as " + rest + "\n");
        }
    } else if (cmd == "/msg") {
        size_t s2 = rest.find(' ');
        if (s2 == std::string::npos) {
            send_to(c, "! Usage: /msg <name> <text>\n");
            return;
        }
        std::string target = rest.substr(0, s2);
        std::string text = trim(rest.substr(s2 + 1));
        auto it = nick_to_fd.find(target);
        if (it == nick_to_fd.end()) {
            send_to(c, "! No such user: " + target + "\n");
        } else if (text.empty()) {
            send_to(c, "! Empty message\n");
        } else {
            send_to(clients[it->second], "[" + now_str() + "] (private) " + c.nick + ": " + text + "\n");
            send_to(c, "[" + now_str() + "] (private to " + target + "): " + text + "\n");
        }
    } else {
        send_to(c, "! Unknown command. Try /help\n");
    }
}

static void handle_read(int fd) {
    auto it = clients.find(fd);
    if (it == clients.end()) return;
    Client &c = it->second;

    char buf[4096];
    for (;;) {
        ssize_t n = recv(fd, buf, sizeof buf, 0);
        if (n > 0) {
            c.inbuf.append(buf, (size_t)n);
            size_t pos;
            while ((pos = c.inbuf.find('\n')) != std::string::npos) {
                std::string line = c.inbuf.substr(0, pos);
                c.inbuf.erase(0, pos + 1);
                handle_line(c, line);
                if (c.want_close) return;
            }
            if (c.inbuf.size() > MAX_LINE) {
                send_to(c, "! Line too long, disconnecting\n");
                c.want_close = true;
                return;
            }
        } else if (n == 0) {
            c.want_close = true;
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            c.want_close = true;
            return;
        }
    }
}

static void handle_accept(int lfd) {
    for (;;) {
        sockaddr_in sa{};
        socklen_t len = sizeof sa;
        int fd = accept(lfd, (sockaddr *)&sa, &len);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            perror("accept");
            return;
        }
        set_nonblocking(fd);
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &sa.sin_addr, ip, sizeof ip);

        Client c;
        c.fd = fd;
        c.addr = std::string(ip) + ":" + std::to_string(ntohs(sa.sin_port));
        // auto nick: guestN, unique
        static int counter = 0;
        do { c.nick = "guest" + std::to_string(++counter); } while (nick_to_fd.count(c.nick));

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

        nick_to_fd[c.nick] = fd;
        std::string nick = c.nick;
        clients.emplace(fd, std::move(c));
        log(nick + " connected from " + clients[fd].addr);

        send_to(clients[fd], "Welcome! You are " + nick + ". Type /help for commands.\n");
        broadcast("* " + nick + " joined the chat\n", fd);
    }
}

static void on_signal(int) { running = 0; }

int main(int argc, char **argv) {
    int port = argc > 1 ? atoi(argv[1]) : 5000;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(lfd, (sockaddr *)&addr, sizeof addr) < 0) { perror("bind"); return 1; }
    if (listen(lfd, SOMAXCONN) < 0) { perror("listen"); return 1; }
    set_nonblocking(lfd);

    epfd = epoll_create1(0);
    epoll_event lev{};
    lev.events = EPOLLIN;
    lev.data.fd = lfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &lev);

    log("Chat server listening on port " + std::to_string(port));

    epoll_event events[MAX_EVENTS];
    while (running) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, 1000);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;
            if (fd == lfd) { handle_accept(lfd); continue; }

            auto it = clients.find(fd);
            if (it == clients.end()) continue;

            if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) it->second.want_close = true;
            if (!it->second.want_close && (ev & EPOLLIN)) handle_read(fd);
            if (clients.count(fd) && (ev & EPOLLOUT)) flush_client(clients[fd]);
            if (clients.count(fd) && clients[fd].want_close) {
                // best effort flush of final message
                flush_client(clients[fd]);
                remove_client(fd);
            }
        }
        // sweep clients flagged for close by broadcasts to others
        std::unordered_set<int> dead;
        for (auto &kv : clients) if (kv.second.want_close) dead.insert(kv.first);
        for (int fd : dead) remove_client(fd);
    }

    log("Shutting down");
    for (auto &kv : clients) close(kv.first);
    close(lfd);
    close(epfd);
    return 0;
}
