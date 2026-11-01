#pragma once
// Raft-backed KVStore. Writes go through the Raft log and are applied to an
// Engine; reads are served only by the leader.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "engine/engine.h"
#include "engine/kv_store.h"
#include "raft/raft.h"
#include "raft/tcp_transport.h"
#include "util/logging.h"

namespace kv {

class RaftStore final : public KVStore {
public:
    struct Peer {
        std::string id;
        std::string host;
        uint16_t port = 0;
    };

    RaftStore(std::unique_ptr<Engine> engine, RaftConfig config,
              std::string raft_host, uint16_t raft_port,
              std::vector<Peer> peers,
              std::chrono::milliseconds commit_timeout = std::chrono::milliseconds(5000))
        : engine_(std::move(engine)),
          config_(std::move(config)),
          transport_(std::move(raft_host), raft_port),
          peers_(std::move(peers)),
          commit_timeout_(commit_timeout) {}

    ~RaftStore() override { Stop(); }

    Status Start() {
        auto s = engine_->Open();
        if (!s.ok()) return s;

        s = transport_.Start();
        if (!s.ok()) return s;

        for (auto& peer : peers_) {
            if (peer.id != config_.node_id) {
                transport_.SetPeer(peer.id, peer.host, peer.port);
            }
        }

        node_ = std::make_unique<RaftNode>(
            config_, &transport_, [this](const LogEntry& entry) { Apply(entry); });

        transport_.SetHandlers(
            [this](const RequestVoteArgs& args) {
                return node_->HandleRequestVote(args);
            },
            [this](const AppendEntriesArgs& args) {
                return node_->HandleAppendEntries(args);
            },
            [this](const InstallSnapshotArgs& args) {
                return node_->HandleInstallSnapshot(args);
            });

        s = node_->Start();
        if (!s.ok()) {
            node_.reset();
            transport_.Stop();
            return s;
        }
        return Status::OK();
    }

    void Stop() {
        if (node_) {
            node_->Stop();
            node_.reset();
        }
        transport_.Stop();
    }

    Status Get(std::string_view key, std::optional<std::string>* out) override {
        auto s = RequireLeader();
        if (!s.ok()) return s;
        return engine_->Get(key, out);
    }

    std::optional<std::string> Get(std::string_view key) {
        std::optional<std::string> out;
        auto s = Get(key, &out);
        if (!s.ok()) return std::nullopt;
        return out;
    }

    Status Put(std::string_view key, std::string_view value) override {
        auto s = RequireLeader();
        if (!s.ok()) return s;
        return node_->ProposeAndWait(std::string(key), std::string(value), false,
                                     commit_timeout_);
    }

    Status PutIfAbsent(std::string_view, std::string_view, bool*) override {
        return Status::NotSupported("conditional writes are not supported in Raft mode");
    }

    Status PutIfPresent(std::string_view, std::string_view, bool*) override {
        return Status::NotSupported("conditional writes are not supported in Raft mode");
    }

    Status PutBatch(
        const std::vector<std::pair<std::string, std::string>>&) override {
        return Status::NotSupported("batched writes are not supported in Raft mode");
    }

    Status Delete(std::string_view key) override {
        auto s = RequireLeader();
        if (!s.ok()) return s;
        return node_->ProposeAndWait(std::string(key), "", true, commit_timeout_);
    }

    Status DbSize(size_t* out) override {
        auto s = RequireLeader();
        if (!s.ok()) return s;
        return engine_->DbSize(out);
    }

    Status ScanAfter(std::string_view start, bool inclusive, size_t limit,
                     std::vector<std::pair<std::string, std::string>>* out,
                     bool* done, std::string* cursor) override {
        auto s = RequireLeader();
        if (!s.ok()) return s;
        return engine_->ScanAfter(start, inclusive, limit, out, done, cursor);
    }

    Status Clear() override {
        return Status::NotSupported("FLUSHDB is not supported in Raft mode");
    }

    Status FlushAndSync() override { return engine_->FlushAndSync(); }

    [[nodiscard]] IOBackend* backend() override { return engine_->backend(); }

    [[nodiscard]] bool is_leader() const override {
        return node_ != nullptr && node_->is_leader();
    }

    [[nodiscard]] uint16_t raft_port() const { return transport_.port(); }
    [[nodiscard]] RaftNode* node() { return node_.get(); }

private:
    Status RequireLeader() const {
        if (!is_leader()) return Status::NotSupported("not the leader");
        return Status::OK();
    }

    void Apply(const LogEntry& entry) {
        auto s = entry.is_delete ? engine_->Delete(entry.key)
                                 : engine_->Put(entry.key, entry.value);
        if (!s.ok()) {
            KV_LOG_ERROR("Raft apply failed for key '%s': %s",
                         entry.key.c_str(), s.ToString().c_str());
        }
    }

    std::unique_ptr<Engine> engine_;
    RaftConfig config_;
    TcpRaftTransport transport_;
    std::unique_ptr<RaftNode> node_;
    std::vector<Peer> peers_;
    std::chrono::milliseconds commit_timeout_;
};

} // namespace kv
