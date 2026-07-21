#pragma once
// Redis-compatible RESP2 server over TCP.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <functional>
#include <string>
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
        ::inet_pton(AF_INET, opts_.host.c_str(), &addr.sin_addr);

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
        if (listen_fd_ >= 0) {
            ::shutdown(listen_fd_, SHUT_RDWR);
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (accept_thread_.joinable()) accept_thread_.join();
        for (auto& t : client_threads_) {
            if (t.joinable()) t.join();
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
                continue;
            }

            // Disable Nagle for low latency.
            int opt = 1;
            ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

            client_threads_.emplace_back([this, client_fd] {
                HandleClient(client_fd);
            });
        }
    }

    void HandleClient(int fd) {
        std::string buffer;
        char recv_buf[65536];

        // Defense in depth (G008): no exception may escape the connection loop.
        // Any throw (parse, alloc, dispatch) is converted into a RESP -ERR reply
        // so a malformed/huge request can never SIGABRT the process.
        try {
            while (running_) {
                ssize_t n = ::recv(fd, recv_buf, sizeof(recv_buf), 0);
                if (n <= 0) break;

                buffer.append(recv_buf, static_cast<size_t>(n));

                // Process all complete commands in the buffer.
                while (true) {
                    ParsedCommand parsed;
                    try {
                        parsed = RespParser::ParseCommandSafe(buffer);
                    } catch (const std::exception& e) {
                        SendError(fd, std::string("ERR internal error: ") + e.what());
                        ::close(fd);
                        return;
                    }

                    if (parsed.status == ParseStatus::kIncomplete) break;  // need more bytes

                    if (parsed.status == ParseStatus::kError) {
                        // Malformed protocol input: reply with an error and drop
                        // the connection (matches Redis behavior). The server
                        // itself keeps running.
                        SendError(fd, "ERR protocol error: " + parsed.error);
                        ::close(fd);
                        return;
                    }

                    buffer.erase(0, parsed.consumed);

                    RespValue response;
                    try {
                        if (parsed.args.empty()) {
                            response = RespValue::Error("ERR empty command");
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

        ::close(fd);
    }

    // Serialize and fully send a RESP value. Returns false on socket failure.
    bool SendValue(int fd, const RespValue& response) {
        std::string serialized = RespSerializer::Serialize(response);
        size_t sent = 0;
        while (sent < serialized.size()) {
            ssize_t w = ::send(fd, serialized.data() + sent,
                               serialized.size() - sent, MSG_NOSIGNAL);
            if (w <= 0) return false;
            sent += static_cast<size_t>(w);
        }
        return true;
    }

    void SendError(int fd, const std::string& msg) {
        SendValue(fd, RespValue::Error(msg));
    }

    Engine& engine_;
    CommandHandler handler_;
    ServerOptions opts_;
    int listen_fd_;
    bool running_;
    std::thread accept_thread_;
    std::vector<std::thread> client_threads_;
};

} // namespace kv
