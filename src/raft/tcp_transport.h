#pragma once
// Real TCP transport for Raft-lite over POSIX sockets (localhost or routable).
//
// This is an actual byte-over-sockets implementation of the RaftTransport
// interface (not the in-memory test transport). Each node runs a listener
// thread that accepts connections, decodes one request, dispatches it to the
// node's RPC handlers, and writes the reply. Clients open a short-lived
// connection per RPC (connect -> send request -> recv reply -> close).
//
// Wire format (little-endian):
//   request  : [u8 type][u32 payload_len][payload]
//   reply    : [u32 payload_len][payload]
// Payloads are the serialized RPC structs (see Writer/Reader below).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "raft/raft.h"
#include "util/logging.h"
#include "util/status.h"

namespace kv {

// --- Minimal binary serialization helpers -----------------------------------

class RpcWriter {
public:
    void U8(uint8_t v) { buf_.push_back(static_cast<char>(v)); }
    void U64(uint64_t v) { buf_.append(reinterpret_cast<char*>(&v), 8); }
    void Str(const std::string& s) {
        U32(static_cast<uint32_t>(s.size()));
        buf_.append(s);
    }
    void U32(uint32_t v) { buf_.append(reinterpret_cast<char*>(&v), 4); }
    void Map(const std::map<std::string, std::string>& m) {
        U32(static_cast<uint32_t>(m.size()));
        for (auto& [k, v] : m) { Str(k); Str(v); }
    }
    void Entries(const std::vector<LogEntry>& es) {
        U32(static_cast<uint32_t>(es.size()));
        for (auto& e : es) {
            U64(e.term); U64(e.index); U8(e.is_delete ? 1 : 0); Str(e.key); Str(e.value);
        }
    }
    const std::string& str() const { return buf_; }

private:
    std::string buf_;
};

class RpcReader {
public:
    RpcReader(const char* data, size_t len) : p_(data), end_(data + len) {}
    bool ok() const { return ok_; }
    uint8_t U8() {
        if (p_ + 1 > end_) { ok_ = false; return 0; }
        return static_cast<uint8_t>(*p_++);
    }
    uint32_t U32() {
        if (p_ + 4 > end_) { ok_ = false; return 0; }
        uint32_t v; std::memcpy(&v, p_, 4); p_ += 4; return v;
    }
    uint64_t U64() {
        if (p_ + 8 > end_) { ok_ = false; return 0; }
        uint64_t v; std::memcpy(&v, p_, 8); p_ += 8; return v;
    }
    std::string Str() {
        uint32_t n = U32();
        if (!ok_ || p_ + n > end_) { ok_ = false; return {}; }
        std::string s(p_, n); p_ += n; return s;
    }
    std::map<std::string, std::string> Map() {
        std::map<std::string, std::string> m;
        uint32_t n = U32();
        for (uint32_t i = 0; i < n && ok_; i++) {
            std::string k = Str();
            std::string v = Str();
            m[k] = v;
        }
        return m;
    }
    std::vector<LogEntry> Entries() {
        std::vector<LogEntry> es;
        uint32_t n = U32();
        for (uint32_t i = 0; i < n && ok_; i++) {
            LogEntry e;
            e.term = U64(); e.index = U64(); e.is_delete = (U8() != 0);
            e.key = Str(); e.value = Str();
            es.push_back(std::move(e));
        }
        return es;
    }

private:
    const char* p_;
    const char* end_;
    bool ok_ = true;
};

// --- RPC payload (de)serialization ------------------------------------------

namespace rpc_detail {

inline std::string SerVoteArgs(const RequestVoteArgs& a) {
    RpcWriter w; w.U64(a.term); w.Str(a.candidate_id);
    w.U64(a.last_log_index); w.U64(a.last_log_term); return w.str();
}
inline bool DeVoteArgs(const std::string& b, RequestVoteArgs* a) {
    RpcReader r(b.data(), b.size());
    a->term = r.U64(); a->candidate_id = r.Str();
    a->last_log_index = r.U64(); a->last_log_term = r.U64(); return r.ok();
}
inline std::string SerVoteReply(const RequestVoteReply& r) {
    RpcWriter w; w.U64(r.term); w.U8(r.vote_granted ? 1 : 0); return w.str();
}
inline bool DeVoteReply(const std::string& b, RequestVoteReply* r) {
    RpcReader rd(b.data(), b.size());
    r->term = rd.U64(); r->vote_granted = (rd.U8() != 0); return rd.ok();
}

inline std::string SerAppendArgs(const AppendEntriesArgs& a) {
    RpcWriter w; w.U64(a.term); w.Str(a.leader_id);
    w.U64(a.prev_log_index); w.U64(a.prev_log_term);
    w.Entries(a.entries); w.U64(a.leader_commit); return w.str();
}
inline bool DeAppendArgs(const std::string& b, AppendEntriesArgs* a) {
    RpcReader r(b.data(), b.size());
    a->term = r.U64(); a->leader_id = r.Str();
    a->prev_log_index = r.U64(); a->prev_log_term = r.U64();
    a->entries = r.Entries(); a->leader_commit = r.U64(); return r.ok();
}
inline std::string SerAppendReply(const AppendEntriesReply& r) {
    RpcWriter w; w.U64(r.term); w.U8(r.success ? 1 : 0); w.U64(r.match_index); return w.str();
}
inline bool DeAppendReply(const std::string& b, AppendEntriesReply* r) {
    RpcReader rd(b.data(), b.size());
    r->term = rd.U64(); r->success = (rd.U8() != 0); r->match_index = rd.U64(); return rd.ok();
}

inline std::string SerSnapArgs(const InstallSnapshotArgs& a) {
    RpcWriter w; w.U64(a.term); w.Str(a.leader_id);
    w.U64(a.last_included_index); w.U64(a.last_included_term);
    w.Map(a.data); return w.str();
}
inline bool DeSnapArgs(const std::string& b, InstallSnapshotArgs* a) {
    RpcReader r(b.data(), b.size());
    a->term = r.U64(); a->leader_id = r.Str();
    a->last_included_index = r.U64(); a->last_included_term = r.U64();
    a->data = r.Map(); return r.ok();
}
inline std::string SerSnapReply(const InstallSnapshotReply& r) {
    RpcWriter w; w.U64(r.term); w.U8(r.success ? 1 : 0); return w.str();
}
inline bool DeSnapReply(const std::string& b, InstallSnapshotReply* r) {
    RpcReader rd(b.data(), b.size());
    r->term = rd.U64(); r->success = (rd.U8() != 0); return rd.ok();
}

} // namespace rpc_detail

// --- TCP transport ----------------------------------------------------------

class TcpRaftTransport final : public RaftTransport {
public:
    using VoteHandler = std::function<RequestVoteReply(const RequestVoteArgs&)>;
    using AppendHandler = std::function<AppendEntriesReply(const AppendEntriesArgs&)>;
    using SnapshotHandler = std::function<InstallSnapshotReply(const InstallSnapshotArgs&)>;

    TcpRaftTransport(std::string host, uint16_t port)
        : host_(std::move(host)), port_(port), listen_fd_(-1), running_(false) {}

    ~TcpRaftTransport() override { Stop(); }

    void SetHandlers(VoteHandler vote, AppendHandler append, SnapshotHandler snap) {
        vote_handler_ = std::move(vote);
        append_handler_ = std::move(append);
        snapshot_handler_ = std::move(snap);
    }

    void SetPeer(const std::string& id, std::string host, uint16_t port) {
        std::lock_guard lock(mu_);
        peers_[id] = {std::move(host), port};
    }

    Status Start() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) return Status::IOError("socket");
        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port_);
        ::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);
        if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(listen_fd_); listen_fd_ = -1;
            return Status::IOError(std::string("bind: ") + std::strerror(errno));
        }
        if (::listen(listen_fd_, 64) != 0) {
            ::close(listen_fd_); listen_fd_ = -1;
            return Status::IOError("listen");
        }
        // Capture ephemeral port if requested.
        struct sockaddr_in bound{};
        socklen_t blen = sizeof(bound);
        if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&bound), &blen) == 0) {
            port_ = ntohs(bound.sin_port);
        }
        running_ = true;
        accept_thread_ = std::thread([this] { AcceptLoop(); });
        return Status::OK();
    }

    void Stop() {
        if (!running_.exchange(false)) {
            // Already stopped (or never started): just ensure the listener is closed.
            int fd = listen_fd_.exchange(-1);
            if (fd >= 0) ::close(fd);
            return;
        }
        // Atomically take the listener fd so the accept loop cannot race on it,
        // then shut it down to unblock accept().
        int fd = listen_fd_.exchange(-1);
        if (fd >= 0) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
        }
        if (accept_thread_.joinable()) accept_thread_.join();
        // Drain in-flight connection handlers (each is bounded by the socket
        // recv timeout, so this join is short).
        std::vector<Conn> conns;
        {
            std::lock_guard lock(conn_mu_);
            conns.swap(conns_);
        }
        for (auto& c : conns) {
            if (c.th.joinable()) c.th.join();
        }
    }

    [[nodiscard]] uint16_t port() const { return port_; }

    Status SendRequestVote(const std::string& target, const RequestVoteArgs& args,
                           RequestVoteReply* reply) override {
        std::string payload = rpc_detail::SerVoteArgs(args);
        std::string resp;
        auto s = RoundTrip(target, 1, payload, &resp);
        if (!s.ok()) return s;
        if (!rpc_detail::DeVoteReply(resp, reply)) return Status::IOError("bad vote reply");
        return Status::OK();
    }

    Status SendAppendEntries(const std::string& target, const AppendEntriesArgs& args,
                             AppendEntriesReply* reply) override {
        std::string payload = rpc_detail::SerAppendArgs(args);
        std::string resp;
        auto s = RoundTrip(target, 2, payload, &resp);
        if (!s.ok()) return s;
        if (!rpc_detail::DeAppendReply(resp, reply)) return Status::IOError("bad append reply");
        return Status::OK();
    }

    Status SendInstallSnapshot(const std::string& target, const InstallSnapshotArgs& args,
                               InstallSnapshotReply* reply) override {
        std::string payload = rpc_detail::SerSnapArgs(args);
        std::string resp;
        auto s = RoundTrip(target, 3, payload, &resp);
        if (!s.ok()) return s;
        if (!rpc_detail::DeSnapReply(resp, reply)) return Status::IOError("bad snapshot reply");
        return Status::OK();
    }

private:
    struct PeerAddr { std::string host; uint16_t port; };

    void AcceptLoop() {
        while (running_) {
            struct sockaddr_in cli{};
            socklen_t len = sizeof(cli);
            int fd = ::accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&cli), &len);
            if (fd < 0) {
                if (errno == EINTR) continue;
                if (!running_) break;
                continue;
            }
            // Handle each connection on its own thread so a slow peer cannot
            // stall the accept loop. Finished handlers are reaped on every accept
            // so the thread list stays bounded by LIVE connections rather than
            // growing with every RPC ever served.
            std::lock_guard lock(conn_mu_);
            ReapConnsLocked();
            auto done = std::make_shared<std::atomic<bool>>(false);
            conns_.push_back(Conn{
                std::thread([this, fd, done] {
                    HandleConn(fd);
                    done->store(true, std::memory_order_release);
                }),
                done});
        }
    }

    // Join and drop connection handlers that have finished. Caller holds conn_mu_.
    void ReapConnsLocked() {
        for (auto& c : conns_) {
            if (c.done->load(std::memory_order_acquire) && c.th.joinable()) c.th.join();
        }
        std::erase_if(conns_, [](const Conn& c) { return !c.th.joinable(); });
    }

    void HandleConn(int fd) {
        SetTimeouts(fd);
        uint8_t type = 0;
        std::string payload;
        if (!ReadFrame(fd, &type, &payload)) { ::close(fd); return; }

        std::string reply_payload;
        switch (type) {
            case 1: {
                RequestVoteArgs a; RequestVoteReply r;
                if (rpc_detail::DeVoteArgs(payload, &a) && vote_handler_) r = vote_handler_(a);
                reply_payload = rpc_detail::SerVoteReply(r);
                break;
            }
            case 2: {
                AppendEntriesArgs a; AppendEntriesReply r;
                if (rpc_detail::DeAppendArgs(payload, &a) && append_handler_) r = append_handler_(a);
                reply_payload = rpc_detail::SerAppendReply(r);
                break;
            }
            case 3: {
                InstallSnapshotArgs a; InstallSnapshotReply r;
                if (rpc_detail::DeSnapArgs(payload, &a) && snapshot_handler_) r = snapshot_handler_(a);
                reply_payload = rpc_detail::SerSnapReply(r);
                break;
            }
            default:
                break;
        }
        WriteReply(fd, reply_payload);
        ::close(fd);
    }

    Status RoundTrip(const std::string& target, uint8_t type,
                     const std::string& payload, std::string* resp) {
        PeerAddr pa;
        {
            std::lock_guard lock(mu_);
            auto it = peers_.find(target);
            if (it == peers_.end()) return Status::IOError("unknown peer: " + target);
            pa = it->second;
        }
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return Status::IOError("socket");
        SetTimeouts(fd);
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(pa.port);
        ::inet_pton(AF_INET, pa.host.c_str(), &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            return Status::IOError("connect " + target + ": " + std::strerror(errno));
        }
        if (!WriteRequest(fd, type, payload)) {
            ::close(fd);
            return Status::IOError("send request");
        }
        uint8_t ignored_type = 0;
        if (!ReadReply(fd, resp)) {
            ::close(fd);
            return Status::IOError("recv reply");
        }
        (void)ignored_type;
        ::close(fd);
        return Status::OK();
    }

    static void SetTimeouts(int fd) {
        struct timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 300 * 1000;  // 300 ms
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        int opt = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    }

    static bool WriteAll(int fd, const char* data, size_t len) {
        size_t sent = 0;
        while (sent < len) {
            ssize_t w = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
            if (w <= 0) return false;
            sent += static_cast<size_t>(w);
        }
        return true;
    }

    static bool ReadAll(int fd, char* data, size_t len) {
        size_t got = 0;
        while (got < len) {
            ssize_t r = ::recv(fd, data + got, len - got, 0);
            if (r <= 0) return false;
            got += static_cast<size_t>(r);
        }
        return true;
    }

    static bool WriteRequest(int fd, uint8_t type, const std::string& payload) {
        char hdr[5];
        hdr[0] = static_cast<char>(type);
        uint32_t n = static_cast<uint32_t>(payload.size());
        std::memcpy(hdr + 1, &n, 4);
        if (!WriteAll(fd, hdr, 5)) return false;
        if (!payload.empty() && !WriteAll(fd, payload.data(), payload.size())) return false;
        return true;
    }

    static bool ReadFrame(int fd, uint8_t* type, std::string* payload) {
        char hdr[5];
        if (!ReadAll(fd, hdr, 5)) return false;
        *type = static_cast<uint8_t>(hdr[0]);
        uint32_t n; std::memcpy(&n, hdr + 1, 4);
        if (n > 256 * 1024 * 1024) return false;  // sanity cap
        payload->resize(n);
        if (n > 0 && !ReadAll(fd, payload->data(), n)) return false;
        return true;
    }

    static bool WriteReply(int fd, const std::string& payload) {
        char hdr[4];
        uint32_t n = static_cast<uint32_t>(payload.size());
        std::memcpy(hdr, &n, 4);
        if (!WriteAll(fd, hdr, 4)) return false;
        if (!payload.empty() && !WriteAll(fd, payload.data(), payload.size())) return false;
        return true;
    }

    static bool ReadReply(int fd, std::string* payload) {
        char hdr[4];
        if (!ReadAll(fd, hdr, 4)) return false;
        uint32_t n; std::memcpy(&n, hdr, 4);
        if (n > 256 * 1024 * 1024) return false;
        payload->resize(n);
        if (n > 0 && !ReadAll(fd, payload->data(), n)) return false;
        return true;
    }

    std::string host_;
    uint16_t port_;
    std::atomic<int> listen_fd_;
    std::atomic<bool> running_;
    std::thread accept_thread_;
    std::mutex conn_mu_;
    // A tracked connection handler plus a flag it sets on completion so the
    // accept loop can reap finished threads (list bounded by live connections).
    struct Conn {
        std::thread th;
        std::shared_ptr<std::atomic<bool>> done;
    };
    std::vector<Conn> conns_;

    std::mutex mu_;
    std::map<std::string, PeerAddr> peers_;

    VoteHandler vote_handler_;
    AppendHandler append_handler_;
    SnapshotHandler snapshot_handler_;
};

} // namespace kv
