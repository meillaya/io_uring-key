#pragma once
// Redis-compatible RESP2 server over TCP.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "engine/engine.h"
#include "server/command_handler.h"
#include "server/resp.h"
#include "util/logging.h"

namespace kv {

struct ServerOptions {
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    int backlog = 128;
    size_t max_connections = 1024;
    int idle_timeout_ms = 300000;  // 5 minutes; 0 disables the timeout.
    std::string requirepass;       // Empty means auth is disabled.
};

class Server {
public:
    Server(Engine& engine, ServerOptions opts = {})
        : engine_(engine), handler_(engine), opts_(std::move(opts)),
          listen_fd_(-1), running_(false) {}

    ~Server() { Stop(); }

    Status Start() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return Status::IOError(std::string("socket: ") + std::strerror(errno));
        }

        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(opts_.port);
        if (::inet_pton(AF_INET, opts_.host.c_str(), &addr.sin_addr) != 1) {
            ::close(listen_fd_);
            listen_fd_ = -1;
            return Status::InvalidArgument("invalid IPv4 host: " + opts_.host);
        }

        if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
            return Status::IOError(std::string("bind: ") + std::strerror(errno));
        }

        if (::listen(listen_fd_, opts_.backlog) != 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
            return Status::IOError(std::string("listen: ") + std::strerror(errno));
        }

        // If the caller requested an ephemeral port (0), capture the actual
        // port assigned by the OS so tests/clients can connect to it.
        if (opts_.port == 0) {
            struct sockaddr_in bound{};
            socklen_t blen = sizeof(bound);
            if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&bound), &blen) == 0) {
                opts_.port = ntohs(bound.sin_port);
            }
        }

        running_ = true;
        KV_LOG_INFO("Server listening on %s:%d", opts_.host.c_str(), opts_.port);

        accept_thread_ = std::thread([this] { AcceptLoop(); });
        return Status::OK();
    }

    void Stop() {
        running_ = false;
        // Atomically take the listener fd so the accept loop cannot race on it,
        // then shut it down to unblock accept().
        int fd = listen_fd_.exchange(-1);
        if (fd >= 0) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
        }
        if (accept_thread_.joinable()) accept_thread_.join();

        // Unblock handlers that are parked in recv. The accept loop is gone, so
        // no new descriptors can enter the set.
        {
            std::lock_guard lock(clients_mu_);
            for (int client_fd : client_fds_) {
                ::shutdown(client_fd, SHUT_RDWR);
            }
        }

        // The accept loop owns client_threads_ while running; now that it is
        // joined, join any handlers still live (finished ones were already reaped).
        for (auto& c : client_threads_) {
            if (c.th.joinable()) c.th.join();
        }
        client_threads_.clear();
    }

    [[nodiscard]] uint16_t port() const { return opts_.port; }
    [[nodiscard]] uint16_t bound_port() const { return opts_.port; }
    [[nodiscard]] bool running() const { return running_; }

private:
    void AcceptLoop() {
        while (running_) {
            struct sockaddr_in client_addr{};
            socklen_t addr_len = sizeof(client_addr);
            int client_fd = ::accept(listen_fd_,
                reinterpret_cast<struct sockaddr*>(&client_addr), &addr_len);
            if (client_fd < 0) {
                if (errno == EINTR) continue;
                if (!running_) break;
                KV_LOG_ERROR("accept: %s", std::strerror(errno));
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            if (live_connections_.load() >= opts_.max_connections) {
                SendValue(client_fd, RespValue::Error("ERR max clients reached"));
                ::close(client_fd);
                continue;
            }

            // Disable Nagle for low latency.
            int opt = 1;
            ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

            if (opts_.idle_timeout_ms > 0) {
                struct timeval tv{};
                tv.tv_sec = opts_.idle_timeout_ms / 1000;
                tv.tv_usec = (opts_.idle_timeout_ms % 1000) * 1000;
                ::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                ::setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            }

            // Reap finished connection handlers so the thread list stays bounded
            // by LIVE connections rather than every connection ever accepted.
            ReapClients();
            {
                std::lock_guard lock(clients_mu_);
                client_fds_.insert(client_fd);
                live_connections_.fetch_add(1);
            }

            auto done = std::make_shared<std::atomic<bool>>(false);
            try {
                client_threads_.push_back(ClientConn{
                    std::thread([this, client_fd, done] {
                        HandleClient(client_fd);
                        done->store(true, std::memory_order_release);
                    }),
                    done});
            } catch (const std::exception& e) {
                KV_LOG_ERROR("client thread start failed: %s", e.what());
                RemoveClient(client_fd);
            }
        }
    }

    // Join and drop client handlers that have finished. Only the accept loop
    // calls this while it owns client_threads_; Stop() touches the list only
    // after joining the accept loop, so no extra lock is required here.
    void ReapClients() {
        for (auto& c : client_threads_) {
            if (c.done->load(std::memory_order_acquire) && c.th.joinable()) c.th.join();
        }
        std::erase_if(client_threads_, [](const ClientConn& c) { return !c.th.joinable(); });
    }

    static bool ConstantTimeEquals(std::string_view a, std::string_view b) {
        if (a.size() != b.size()) return false;
        unsigned char diff = 0;
        for (size_t i = 0; i < a.size(); i++) {
            diff |= static_cast<unsigned char>(a[i] ^ b[i]);
        }
        return diff == 0;
    }

    RespValue HandleAuth(const std::vector<std::string>& args, bool* authenticated) {
        std::string cmd = args[0];
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
        if (cmd != "AUTH" || args.size() != 2) {
            return RespValue::Error("NOAUTH Authentication required.");
        }
        if (!ConstantTimeEquals(args[1], opts_.requirepass)) {
            return RespValue::Error("ERR invalid password");
        }
        *authenticated = true;
        return RespValue::SimpleString("OK");
    }

    void RemoveClient(int fd) {
        std::lock_guard lock(clients_mu_);
        if (client_fds_.erase(fd) > 0) {
            live_connections_.fetch_sub(1);
        }
        ::close(fd);
    }

    void CloseClient(int fd) { RemoveClient(fd); }

    void HandleClient(int fd) {
        std::string buffer;
        char recv_buf[65536];
        bool authenticated = opts_.requirepass.empty();

        // Defense in depth (G008): no exception may escape the connection loop.
        // Any throw (parse, alloc, dispatch) is converted into a RESP -ERR reply
        // so a malformed/huge request can never SIGABRT the process.
        try {
            while (running_) {
                ssize_t n = ::recv(fd, recv_buf, sizeof(recv_buf), 0);
                if (n <= 0) break;

                buffer.append(recv_buf, static_cast<size_t>(n));
                if (buffer.size() > kMaxConnectionBuffer) {
                    SendError(fd, "ERR request too large");
                    CloseClient(fd);
                    return;
                }

                // Process all complete commands in the buffer.
                while (true) {
                    ParsedCommand parsed;
                    try {
                        parsed = RespParser::ParseCommandSafe(buffer);
                    } catch (const std::exception& e) {
                        SendError(fd, std::string("ERR internal error: ") + e.what());
                        CloseClient(fd);
                        return;
                    }

                    if (parsed.status == ParseStatus::kIncomplete) break;  // need more bytes

                    if (parsed.status == ParseStatus::kError) {
                        // Malformed protocol input: reply with an error and drop
                        // the connection (matches Redis behavior). The server
                        // itself keeps running.
                        SendError(fd, "ERR protocol error: " + parsed.error);
                        CloseClient(fd);
                        return;
                    }

                    buffer.erase(0, parsed.consumed);

                    RespValue response;
                    try {
                        if (parsed.args.empty()) {
                            response = RespValue::Error("ERR empty command");
                        } else if (!authenticated) {
                            response = HandleAuth(parsed.args, &authenticated);
                        } else {
                            response = handler_.Execute(parsed.args);
                        }
                    } catch (const std::exception& e) {
                        response = RespValue::Error(std::string("ERR ") + e.what());
                    } catch (...) {
                        response = RespValue::Error("ERR internal error");
                    }

                    if (!SendValue(fd, response)) break;
                }
            }
        } catch (const std::exception& e) {
            KV_LOG_ERROR("connection error: %s", e.what());
        } catch (...) {
            KV_LOG_ERROR("connection error: unknown");
        }

        CloseClient(fd);
    }

    // Serialize and fully send a RESP value. Returns false on socket failure.
    bool SendValue(int fd, const RespValue& response) {
        std::string serialized = RespSerializer::Serialize(response);
        size_t sent = 0;
        while (sent < serialized.size()) {
            ssize_t w = ::send(fd, serialized.data() + sent,
                               serialized.size() - sent, MSG_NOSIGNAL);
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) return false;
            sent += static_cast<size_t>(w);
        }
        return true;
    }

    void SendError(int fd, const std::string& msg) {
        SendValue(fd, RespValue::Error(msg));
    }

    static constexpr size_t kMaxConnectionBuffer = 64 * 1024 * 1024;

    Engine& engine_;
    CommandHandler handler_;
    ServerOptions opts_;
    std::atomic<int> listen_fd_;
    std::atomic<bool> running_;
    std::thread accept_thread_;
    std::mutex clients_mu_;
    std::set<int> client_fds_;
    std::atomic<size_t> live_connections_{0};
    // A tracked client handler plus a flag it sets on completion so the accept
    // loop can reap finished threads (list bounded by live connections).
    struct ClientConn {
        std::thread th;
        std::shared_ptr<std::atomic<bool>> done;
    };
    std::vector<ClientConn> client_threads_;
};

} // namespace kv
