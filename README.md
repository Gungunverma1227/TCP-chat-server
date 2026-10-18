# Multi-client TCP Chat Server (C++)

Single-threaded chat server built on non-blocking sockets and `epoll` (Linux). One event loop handles every client, so there is no thread per connection and no locking.

## Build and run

```bash
make
./server 5000            # start server (default port 5000)
./client 127.0.0.1 5000  # open in as many terminals as you like
```

Test from any machine with `nc <server-ip> 5000` as well.

## Commands

| Command | What it does |
|---|---|
| `text` | broadcast to everyone |
| `/nick <name>` | change nickname (1-16 chars: letters, digits, `_`, `-`) |
| `/list` | show online users |
| `/msg <name> <text>` | private message |
| `/help` | list commands |
| `/quit` | disconnect |

## How it works

- **Listening socket** is non-blocking and registered with epoll. On `EPOLLIN`, `accept()` runs in a loop until `EAGAIN`.
- **Reads**: each client has an `inbuf`. TCP is a byte stream, so data is split on `\n` only after a full line has arrived. Lines above 1024 bytes disconnect the client.
- **Writes**: each client has an `outbuf`. `send()` is tried immediately; if the socket buffer is full (`EAGAIN`), the rest waits and `EPOLLOUT` is enabled until it drains. Clients whose backlog passes 1 MB are dropped so one slow reader cannot eat all memory.
- **State**: `fd -> Client` map plus `nick -> fd` map for O(1) private messages and nick uniqueness checks.
- **Robustness**: `SIGPIPE` ignored, `MSG_NOSIGNAL` on send, `EPOLLRDHUP` for fast disconnect detection, `SIGINT`/`SIGTERM` for clean shutdown.
- **Client** uses `select()` to watch stdin and the socket together, so incoming messages show up while you type.

## Tested

- Broadcast, private messages, nick change, duplicate nick rejection, unknown user, `/list`, `/quit`.
- 300 concurrent clients connecting, chatting and disconnecting with no crash or leak of connections.

## Ideas to extend (for day 2)

- Thread-pool version (`std::thread` + mutex) to compare against epoll.
- Chat rooms / channels (`/join <room>`).
- Message history for new joiners.
- Rate limiting per client.
- Simple length-prefixed protocol instead of newline-delimited text.
- TLS with OpenSSL.
