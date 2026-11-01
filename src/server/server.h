#pragma once
// Redis-compatible RESP2 server over TCP, with optional TLS.

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

#ifdef KV_HAS_TLS
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

#include "engine/kv_store.h"
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
    std::string tls_cert;          // PEM chain; empty disables TLS.
    std::string tls_key;           // PEM private key.
};

class Server {
public:
    Server(KVStore& engine, ServerOptions opts = {})
        : engine_(engine), handler_(engine), opts_(std::move(opts)),
          listen_fd_(-1), running_(false) {}

    ~Server() {
        Stop();
#ifdef KV_HAS_TLS
        if (tls_ctx_ != nullptr) {
            SSL_CTX_free(tls_ctx_);
            tls_ctx_ = nullptr;
        }
#endif
    }

    Status Start() {
        auto tls_status = InitTls();
        if (!tls_status.ok()) return tls_status;

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
        KV_LOG_INFO("Server listening on %s:%d (tls: %s)", opts_.host.c_str(),
                    opts_.port, tls_enabled() ? "on" : "off");

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

        // Unblock handlers that are parked in recv or SSL_read. The accept loop
        // is gone, so no new descriptors can enter the set.
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
    [[nodiscard]] bool tls_enabled() const {
#ifdef KV_HAS_TLS
        return tls_ctx_ != nullptr;
#else
        return false;
#endif
    }

private:
    struct ClientSocket {
        int fd = -1;
#ifdef KV_HAS_TLS
        SSL* ssl = nullptr;
#endif
    };

    Status InitTls() {
#ifdef KV_HAS_TLS
        if (opts_.tls_cert.empty() && opts_.tls_key.empty()) {
            return Status::OK();
        }
        if (opts_.tls_cert.empty() || opts_.tls_key.empty()) {
            return Status::InvalidArgument("both tls_cert and tls_key are required");
        }

        OPENSSL_init_ssl(0, nullptr);
        tls_ctx_ = SSL_CTX_new(TLS_server_method());
        if (tls_ctx_ == nullptr) {
            return Status::IOError("SSL_CTX_new: " + SslError());
        }
        SSL_CTX_set_min_proto_version(tls_ctx_, TLS1_2_VERSION);
        if (SSL_CTX_use_certificate_chain_file(tls_ctx_, opts_.tls_cert.c_str()) != 1) {
            return Status::IOError("load TLS certificate: " + SslError());
        }
        if (SSL_CTX_use_PrivateKey_file(tls_ctx_, opts_.tls_key.c_str(),
                                        SSL_FILETYPE_PEM) != 1) {
            return Status::IOError("load TLS private key: " + SslError());
        }
        if (SSL_CTX_check_private_key(tls_ctx_) != 1) {
            return Status::IOError("TLS private key does not match certificate: " +
                                   SslError());
        }
        return Status::OK();
#else
        if (!opts_.tls_cert.empty() || !opts_.tls_key.empty()) {
            return Status::NotSupported("TLS support was not compiled in");
        }
        return Status::OK();
#endif
    }

#ifdef KV_HAS_TLS
    static std::string SslError() {
        char buffer[256];
        ERR_error_string_n(ERR_get_error(), buffer, sizeof(buffer));
        return std::string(buffer);
    }
#endif

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

            ClientSocket socket;
            socket.fd = client_fd;

            if (live_connections_.load() >= opts_.max_connections) {
                SendValue(socket, RespValue::Error("ERR max clients reached"));
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

#ifdef KV_HAS_TLS
            if (tls_ctx_ != nullptr) {
                socket.ssl = SSL_new(tls_ctx_);
                if (socket.ssl == nullptr) {
                    KV_LOG_ERROR("SSL_new failed: %s", SslError().c_str());
                    RemoveClient(client_fd);
                    continue;
                }
                SSL_set_fd(socket.ssl, client_fd);
                if (SSL_accept(socket.ssl) <= 0) {
                    KV_LOG_WARN("TLS handshake failed: %s", SslError().c_str());
                    SSL_free(socket.ssl);
                    RemoveClient(client_fd);
                    continue;
                }
            }
#endif

            auto done = std::make_shared<std::atomic<bool>>(false);
            try {
                client_threads_.push_back(ClientConn{
                    std::thread([this, socket, done] {
                        HandleClient(socket);
                        done->store(true, std::memory_order_release);
                    }),
                    done});
            } catch (const std::exception& e) {
                KV_LOG_ERROR("client thread start failed: %s", e.what());
#ifdef KV_HAS_TLS
                if (socket.ssl != nullptr) SSL_free(socket.ssl);
#endif
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

    void CloseClient(ClientSocket& socket) {
#ifdef KV_HAS_TLS
        if (socket.ssl != nullptr) {
            SSL_free(socket.ssl);
            socket.ssl = nullptr;
        }
#endif
        RemoveClient(socket.fd);
    }

    ssize_t ReadSome(ClientSocket& socket, char* buffer, size_t length) {
#ifdef KV_HAS_TLS
        if (socket.ssl != nullptr) {
            int n = SSL_read(socket.ssl, buffer, static_cast<int>(length));
            if (n > 0) return n;
            int error = SSL_get_error(socket.ssl, n);
            if (error == SSL_ERROR_SYSCALL && errno == EINTR) return 0;
            return n;
        }
#endif
        return ::recv(socket.fd, buffer, length, 0);
    }

    void HandleClient(ClientSocket socket) {
        std::string buffer;
        char recv_buf[65536];
        bool authenticated = opts_.requirepass.empty();

        // Defense in depth (G008): no exception may escape the connection loop.
        // Any throw (parse, alloc, dispatch) is converted into a RESP -ERR reply
        // so a malformed/huge request can never SIGABRT the process.
        try {
            while (running_) {
                ssize_t n = ReadSome(socket, recv_buf, sizeof(recv_buf));
                if (n <= 0) break;

                buffer.append(recv_buf, static_cast<size_t>(n));
                if (buffer.size() > kMaxConnectionBuffer) {
                    SendError(socket, "ERR request too large");
                    CloseClient(socket);
                    return;
                }

                // Process all complete commands in the buffer.
                while (true) {
                    ParsedCommand parsed;
                    try {
                        parsed = RespParser::ParseCommandSafe(buffer);
                    } catch (const std::exception& e) {
                        SendError(socket, std::string("ERR internal error: ") + e.what());
                        CloseClient(socket);
                        return;
                    }

                    if (parsed.status == ParseStatus::kIncomplete) break;  // need more bytes

                    if (parsed.status == ParseStatus::kError) {
                        // Malformed protocol input: reply with an error and drop
                        // the connection (matches Redis behavior). The server
                        // itself keeps running.
                        SendError(socket, "ERR protocol error: " + parsed.error);
                        CloseClient(socket);
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

                    if (!SendValue(socket, response)) break;
                }
            }
        } catch (const std::exception& e) {
            KV_LOG_ERROR("connection error: %s", e.what());
        } catch (...) {
            KV_LOG_ERROR("connection error: unknown");
        }

        CloseClient(socket);
    }

    // Serialize and fully send a RESP value. Returns false on socket failure.
    bool SendValue(ClientSocket& socket, const RespValue& response) {
        std::string serialized = RespSerializer::Serialize(response);
        size_t sent = 0;
        while (sent < serialized.size()) {
#ifdef KV_HAS_TLS
            if (socket.ssl != nullptr) {
                int w = SSL_write(socket.ssl, serialized.data() + sent,
                                  static_cast<int>(serialized.size() - sent));
                if (w <= 0) {
                    int error = SSL_get_error(socket.ssl, w);
                    if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        continue;
                    }
                    return false;
                }
                sent += static_cast<size_t>(w);
                continue;
            }
#endif
            ssize_t w = ::send(socket.fd, serialized.data() + sent,
                               serialized.size() - sent, MSG_NOSIGNAL);
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) return false;
            sent += static_cast<size_t>(w);
        }
        return true;
    }

    void SendError(ClientSocket& socket, const std::string& msg) {
        SendValue(socket, RespValue::Error(msg));
    }

    static constexpr size_t kMaxConnectionBuffer = 64 * 1024 * 1024;

    KVStore& engine_;
    CommandHandler handler_;
    ServerOptions opts_;
    std::atomic<int> listen_fd_;
    std::atomic<bool> running_;
    std::thread accept_thread_;
    std::mutex clients_mu_;
    std::set<int> client_fds_;
    std::atomic<size_t> live_connections_{0};
#ifdef KV_HAS_TLS
    SSL_CTX* tls_ctx_ = nullptr;
#endif
    // A tracked client handler plus a flag it sets on completion so the accept
    // loop can reap finished threads (list bounded by live connections).
    struct ClientConn {
        std::thread th;
        std::shared_ptr<std::atomic<bool>> done;
    };
    std::vector<ClientConn> client_threads_;
};

} // namespace kv
