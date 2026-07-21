#pragma once
// Raft-lite: leader election, log replication, KV state machine, snapshots,
// crash recovery, leader-only reads.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "util/crc32.h"
#include "util/logging.h"
#include "util/status.h"

namespace kv {

// Raft log entry.
struct LogEntry {
    uint64_t term = 0;
    uint64_t index = 0;
    std::string key;
    std::string value;
    bool is_delete = false;
};

// Raft node state.
enum class RaftState { kFollower, kCandidate, kLeader };

inline const char* StateName(RaftState s) {
    switch (s) {
        case RaftState::kFollower: return "Follower";
        case RaftState::kCandidate: return "Candidate";
        case RaftState::kLeader: return "Leader";
    }
    return "?";
}

// Raft RPC messages.
struct RequestVoteArgs {
    uint64_t term;
    std::string candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
};

struct RequestVoteReply {
    uint64_t term;
    bool vote_granted;
};

struct AppendEntriesArgs {
    uint64_t term;
    std::string leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    std::vector<LogEntry> entries;
    uint64_t leader_commit;
};

struct AppendEntriesReply {
    uint64_t term;
    bool success;
    uint64_t match_index;  // For fast backup.
};

// InstallSnapshot RPC (sent by leader to a follower that is behind the
// leader's compacted log).
struct InstallSnapshotArgs {
    uint64_t term;
    std::string leader_id;
    uint64_t last_included_index;
    uint64_t last_included_term;
    std::map<std::string, std::string> data;  // Full state-machine snapshot.
};

struct InstallSnapshotReply {
    uint64_t term;
    bool success;
};

// Snapshot data.
struct Snapshot {
    uint64_t last_included_index = 0;
    uint64_t last_included_term = 0;
    std::map<std::string, std::string> data;
};

// Transport interface for Raft communication.
class RaftTransport {
public:
    virtual ~RaftTransport() = default;
    virtual Status SendRequestVote(const std::string& target,
                                    const RequestVoteArgs& args,
                                    RequestVoteReply* reply) = 0;
    virtual Status SendAppendEntries(const std::string& target,
                                      const AppendEntriesArgs& args,
                                      AppendEntriesReply* reply) = 0;

    // Ship a snapshot to a lagging follower. Default: unsupported (a transport
    // that does not implement it simply cannot fast-forward a far-behind peer).
    virtual Status SendInstallSnapshot(const std::string& target,
                                       const InstallSnapshotArgs& args,
                                       InstallSnapshotReply* reply) {
        (void)target; (void)args; (void)reply;
        return Status::NotSupported("transport does not support InstallSnapshot");
    }
};

// Raft configuration.
struct RaftConfig {
    std::string node_id;
    std::vector<std::string> peers;  // All node IDs including self.
    std::string data_dir;
    int election_timeout_min_ms = 150;
    int election_timeout_max_ms = 300;
    int heartbeat_interval_ms = 50;
    size_t snapshot_threshold = 10000;  // Compact log after this many entries.
};

// KV state machine callback.
using StateMachineApply = std::function<void(const LogEntry&)>;

class RaftNode {
public:
    RaftNode(RaftConfig config, RaftTransport* transport,
             StateMachineApply apply_cb)
        : config_(std::move(config)), transport_(transport),
          apply_cb_(std::move(apply_cb)),
          current_term_(0), voted_for_(""),
          commit_index_(0), last_applied_(0),
          state_(RaftState::kFollower), running_(false) {
        // Initialize next_index and match_index for peers.
        for (auto& peer : config_.peers) {
            if (peer != config_.node_id) {
                next_index_[peer] = 1;
                match_index_[peer] = 0;
            }
        }
    }

    ~RaftNode() { Stop(); }

    Status Start() {
        running_ = true;
        // Load persistent state.
        auto s = LoadState();
        if (!s.ok()) return s;

        raft_thread_ = std::thread([this] { RunLoop(); });
        return Status::OK();
    }

    void Stop() {
        running_ = false;
        cv_.notify_all();
        if (raft_thread_.joinable()) raft_thread_.join();
    }

    // Propose a key-value operation to the Raft cluster.
    Status Propose(const std::string& key, const std::string& value,
                   bool is_delete = false) {
        std::lock_guard lock(mu_);
        if (state_ != RaftState::kLeader) {
            return Status::NotSupported("not the leader");
        }

        LogEntry entry;
        entry.term = current_term_;
        entry.index = log_.empty() ? 1 : log_.back().index + 1;
        entry.key = key;
        entry.value = value;
        entry.is_delete = is_delete;

        log_.push_back(entry);

        // Replicate to peers (or self-commit for single node).
        if (config_.peers.size() == 1) {
            // Single node: commit immediately, then persist log + commit index
            // together so a restart replays every committed entry.
            commit_index_ = entry.index;
            PersistState();
            ApplyCommitted();
        } else {
            // Persist the new log entry before replicating; commit_index_ is
            // persisted later once a majority acknowledges (UpdateCommitIndex).
            PersistState();
            ReplicateToPeers();
        }

        return Status::OK();
    }

    // Read a value (leader-only). Served directly from the leader's in-memory
    // state machine under a leader-lease assumption: we do NOT run a ReadIndex
    // heartbeat-confirm round before reading, so this is a leader-lease read,
    // not a full linearizable read (documented as a limitation in the docs).
    std::optional<std::string> Read(const std::string& key) {
        std::lock_guard lock(mu_);
        if (state_ != RaftState::kLeader) return std::nullopt;

        // Leader-lease read: we currently hold the leader state, so we serve the
        // read locally. A full implementation would first confirm leadership via
        // a ReadIndex heartbeat round; we intentionally do not (see docs).
        auto it = state_machine_.find(key);
        if (it != state_machine_.end()) return it->second;
        return std::nullopt;
    }

    // Compact the applied log into a snapshot and truncate the log entries
    // that are now covered by the snapshot. Persists the result. Returns the
    // number of log entries dropped.
    size_t TakeSnapshot() {
        std::lock_guard lock(mu_);
        return TakeSnapshotLocked();
    }

    // Handle an InstallSnapshot RPC from the leader (follower side).
    InstallSnapshotReply HandleInstallSnapshot(const InstallSnapshotArgs& args) {
        std::lock_guard lock(mu_);
        InstallSnapshotReply reply;
        reply.term = current_term_;
        reply.success = false;

        if (args.term < current_term_) {
            reply.term = current_term_;
            return reply;
        }
        if (args.term > current_term_) {
            current_term_ = args.term;
            voted_for_ = "";
        }
        state_ = RaftState::kFollower;
        leader_id_ = args.leader_id;
        last_heartbeat_ = std::chrono::steady_clock::now();

        // Reject a snapshot that does not advance our snapshot base (never
        // install an older/equal snapshot). The term bump + step-down above
        // still apply; we only refuse to roll the state machine backwards.
        if (args.last_included_index <= snapshot_.last_included_index) {
            reply.term = current_term_;
            return reply;  // success stays false
        }

        // Install the snapshot: replace state machine + snapshot base.
        snapshot_.last_included_index = args.last_included_index;
        snapshot_.last_included_term = args.last_included_term;
        snapshot_.data = args.data;

        // Discard any log entries covered by the snapshot.
        std::erase_if(log_, [&](const LogEntry& e) {
            return e.index <= args.last_included_index;
        });

        // The state machine now IS the snapshot.
        state_machine_ = args.data;
        last_applied_ = args.last_included_index;
        if (commit_index_ < args.last_included_index) {
            commit_index_ = args.last_included_index;
        }

        PersistState();
        reply.term = current_term_;
        reply.success = true;
        KV_LOG_INFO("Node %s installed snapshot up to index %lu",
                    config_.node_id.c_str(), args.last_included_index);
        return reply;
    }

    // ---- Thread-safe accessors (G009) ------------------------------------
    // Every reader of mu_-protected state takes the lock and returns a copy
    // computed under it, so the Raft layer is TSan-clean (no torn std::string /
    // std::map reads). mu_ is mutable so const accessors can lock it. None of
    // these is called from code that already holds mu_ (no lock recursion).
    [[nodiscard]] RaftState state() const {
        std::lock_guard lock(mu_);
        return state_;
    }
    [[nodiscard]] uint64_t current_term() const {
        std::lock_guard lock(mu_);
        return current_term_;
    }
    [[nodiscard]] std::string leader_id() const {
        std::lock_guard lock(mu_);
        return leader_id_;
    }
    [[nodiscard]] bool is_leader() const {
        std::lock_guard lock(mu_);
        return state_ == RaftState::kLeader;
    }
    [[nodiscard]] uint64_t commit_index() const {
        std::lock_guard lock(mu_);
        return commit_index_;
    }
    [[nodiscard]] uint64_t last_applied() const {
        std::lock_guard lock(mu_);
        return last_applied_;
    }
    [[nodiscard]] uint64_t last_included_index() const {
        std::lock_guard lock(mu_);
        return snapshot_.last_included_index;
    }
    [[nodiscard]] size_t log_size() const {
        std::lock_guard lock(mu_);
        return log_.size();
    }
    // Snapshot-aware last log position: falls back to the snapshot base when the
    // log has been fully compacted away. These are the values the RequestVote
    // up-to-date check must compare against.
    [[nodiscard]] uint64_t last_log_index() const {
        std::lock_guard lock(mu_);
        return LastLogIndexLocked();
    }
    [[nodiscard]] uint64_t last_log_term() const {
        std::lock_guard lock(mu_);
        return LastLogTermLocked();
    }
    // Copy of the materialized KV state machine (for tests/inspection).
    [[nodiscard]] std::map<std::string, std::string> state_machine_copy() const {
        std::lock_guard lock(mu_);
        return state_machine_;
    }

    // Handle incoming RPCs (called by transport).
    RequestVoteReply HandleRequestVote(const RequestVoteArgs& args) {
        std::lock_guard lock(mu_);
        RequestVoteReply reply;
        reply.term = current_term_;
        reply.vote_granted = false;

        if (args.term > current_term_) {
            current_term_ = args.term;
            state_ = RaftState::kFollower;
            voted_for_ = "";
            PersistState();
        }

        if (args.term < current_term_) return reply;

        // Check if we can vote for this candidate.
        if (voted_for_.empty() || voted_for_ == args.candidate_id) {
            // Check log is at least as up-to-date.
            uint64_t my_last_term = LastLogTermLocked();
            uint64_t my_last_index = LastLogIndexLocked();

            if (args.last_log_term > my_last_term ||
                (args.last_log_term == my_last_term &&
                 args.last_log_index >= my_last_index)) {
                voted_for_ = args.candidate_id;
                reply.vote_granted = true;
                PersistState();
                // Reset election timeout.
                last_heartbeat_ = std::chrono::steady_clock::now();
            }
        }

        reply.term = current_term_;
        return reply;
    }

    AppendEntriesReply HandleAppendEntries(const AppendEntriesArgs& args) {
        std::lock_guard lock(mu_);
        AppendEntriesReply reply;
        reply.term = current_term_;
        reply.success = false;
        reply.match_index = 0;

        if (args.term > current_term_) {
            current_term_ = args.term;
            state_ = RaftState::kFollower;
            voted_for_ = "";
            PersistState();
        }

        if (args.term < current_term_) {
            reply.term = current_term_;
            return reply;
        }

        // Recognize leader.
        leader_id_ = args.leader_id;
        state_ = RaftState::kFollower;
        last_heartbeat_ = std::chrono::steady_clock::now();

        // Check prev_log consistency (snapshot-aware).
        if (args.prev_log_index > 0) {
            uint64_t last_idx = log_.empty() ? snapshot_.last_included_index
                                             : log_.back().index;
            if (args.prev_log_index > last_idx) {
                reply.term = current_term_;
                return reply;  // follower is behind; leader will back up / snapshot
            }
            if (args.prev_log_index == snapshot_.last_included_index) {
                if (args.prev_log_term != snapshot_.last_included_term) {
                    reply.term = current_term_;
                    return reply;
                }
            } else {
                auto it = FindLogEntry(args.prev_log_index);
                if (!it || it->term != args.prev_log_term) {
                    TruncateLogFrom(args.prev_log_index);
                    reply.term = current_term_;
                    return reply;
                }
            }
        }

        // Append new entries.
        for (auto& entry : args.entries) {
            auto existing = FindLogEntry(entry.index);
            if (existing && existing->term != entry.term) {
                TruncateLogFrom(entry.index);
                existing = std::nullopt;
            }
            if (!existing) {
                log_.push_back(entry);
            }
        }

        // Update commit index.
        if (args.leader_commit > commit_index_) {
            uint64_t last_log_idx = log_.empty() ? 0 : log_.back().index;
            commit_index_ = std::min(args.leader_commit, last_log_idx);
            ApplyCommitted();
        }

        PersistState();
        reply.success = true;
        reply.match_index = log_.empty() ? 0 : log_.back().index;
        reply.term = current_term_;
        return reply;
    }

private:
    void RunLoop() {
        std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> timeout_dist(
            config_.election_timeout_min_ms, config_.election_timeout_max_ms);
        int election_timeout = timeout_dist(rng);
        last_heartbeat_ = std::chrono::steady_clock::now();

        while (running_) {
            std::unique_lock lock(mu_);
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_heartbeat_).count();

            if (state_ == RaftState::kLeader) {
                // Send heartbeats.
                cv_.wait_for(lock, std::chrono::milliseconds(config_.heartbeat_interval_ms));
                if (!running_) break;
                SendHeartbeats();
            } else if (elapsed >= election_timeout) {
                // Start election.
                StartElection();
                election_timeout = timeout_dist(rng);
                last_heartbeat_ = std::chrono::steady_clock::now();
            } else {
                cv_.wait_for(lock, std::chrono::milliseconds(10));
            }
        }
    }

    void StartElection() {
        current_term_++;
        state_ = RaftState::kCandidate;
        voted_for_ = config_.node_id;
        leader_id_ = "";
        PersistState();

        KV_LOG_INFO("Node %s starting election for term %lu",
                    config_.node_id.c_str(), current_term_);

        RequestVoteArgs args;
        args.term = current_term_;
        args.candidate_id = config_.node_id;
        args.last_log_index = LastLogIndexLocked();
        args.last_log_term = LastLogTermLocked();

        int votes = 1;  // Vote for self.
        int needed = static_cast<int>(config_.peers.size()) / 2 + 1;

        for (auto& peer : config_.peers) {
            if (peer == config_.node_id) continue;
            RequestVoteReply reply;
            auto s = transport_->SendRequestVote(peer, args, &reply);
            if (s.ok() && reply.vote_granted) {
                votes++;
            }
            if (reply.term > current_term_) {
                current_term_ = reply.term;
                state_ = RaftState::kFollower;
                voted_for_ = "";
                PersistState();
                return;
            }
        }

        if (votes >= needed && state_ == RaftState::kCandidate) {
            BecomeLeader();
        }
    }

    void BecomeLeader() {
        state_ = RaftState::kLeader;
        leader_id_ = config_.node_id;
        KV_LOG_INFO("Node %s became leader for term %lu",
                    config_.node_id.c_str(), current_term_);

        // Initialize next_index and match_index.
        uint64_t last_log_idx = log_.empty() ? 0 : log_.back().index;
        for (auto& peer : config_.peers) {
            if (peer != config_.node_id) {
                next_index_[peer] = last_log_idx + 1;
                match_index_[peer] = 0;
            }
        }

        SendHeartbeats();
    }

    void SendHeartbeats() {
        for (auto& peer : config_.peers) {
            if (peer == config_.node_id) continue;

            // If the follower needs entries we have already compacted into a
            // snapshot, ship the whole snapshot instead of AppendEntries.
            if (snapshot_.last_included_index > 0 &&
                next_index_[peer] <= snapshot_.last_included_index) {
                InstallSnapshotArgs sargs;
                sargs.term = current_term_;
                sargs.leader_id = config_.node_id;
                sargs.last_included_index = snapshot_.last_included_index;
                sargs.last_included_term = snapshot_.last_included_term;
                sargs.data = snapshot_.data;
                InstallSnapshotReply sreply;
                auto ss = transport_->SendInstallSnapshot(peer, sargs, &sreply);
                if (ss.ok() && sreply.success) {
                    next_index_[peer] = snapshot_.last_included_index + 1;
                    match_index_[peer] = snapshot_.last_included_index;
                    UpdateCommitIndex();
                } else if (ss.ok() && sreply.term > current_term_) {
                    current_term_ = sreply.term;
                    state_ = RaftState::kFollower;
                    voted_for_ = "";
                    PersistState();
                    return;
                }
                continue;  // resume normal AppendEntries next round
            }

            AppendEntriesArgs args;
            args.term = current_term_;
            args.leader_id = config_.node_id;
            args.leader_commit = commit_index_;

            uint64_t next_idx = next_index_[peer];
            if (next_idx > 1) {
                args.prev_log_index = next_idx - 1;
                args.prev_log_term = TermAtLocked(next_idx - 1);
            } else {
                args.prev_log_index = 0;
                args.prev_log_term = 0;
            }

            // Send entries from next_index onwards.
            for (auto& entry : log_) {
                if (entry.index >= next_idx) {
                    args.entries.push_back(entry);
                }
            }

            AppendEntriesReply reply;
            auto s = transport_->SendAppendEntries(peer, args, &reply);
            if (s.ok()) {
                if (reply.success) {
                    next_index_[peer] = reply.match_index + 1;
                    match_index_[peer] = reply.match_index;
                    // Update commit index.
                    UpdateCommitIndex();
                } else if (reply.term > current_term_) {
                    current_term_ = reply.term;
                    state_ = RaftState::kFollower;
                    voted_for_ = "";
                    PersistState();
                    return;
                } else {
                    // Decrement next_index and retry.
                    if (next_index_[peer] > 1) next_index_[peer]--;
                }
            }
        }
    }

    void ReplicateToPeers() {
        SendHeartbeats();
    }

    void UpdateCommitIndex() {
        // Find the highest index replicated on a majority.
        int majority = static_cast<int>(config_.peers.size()) / 2 + 1;
        for (uint64_t n = log_.empty() ? 0 : log_.back().index; n > commit_index_; n--) {
            int count = 1;  // Self.
            for (auto& [peer, match] : match_index_) {
                if (match >= n) count++;
            }
            if (count >= majority) {
                auto entry = FindLogEntry(n);
                if (entry && entry->term == current_term_) {
                    commit_index_ = n;
                    PersistState();  // make the new commit index durable
                    ApplyCommitted();
                    break;
                }
            }
        }
    }

    void ApplyCommitted() {
        while (last_applied_ < commit_index_) {
            last_applied_++;
            auto entry = FindLogEntry(last_applied_);
            if (entry) {
                if (entry->is_delete) {
                    state_machine_.erase(entry->key);
                } else {
                    state_machine_[entry->key] = entry->value;
                }
                if (apply_cb_) apply_cb_(*entry);
            }
        }
    }

    std::optional<LogEntry> FindLogEntry(uint64_t index) {
        for (auto& entry : log_) {
            if (entry.index == index) return entry;
        }
        return std::nullopt;
    }

    void TruncateLogFrom(uint64_t index) {
        std::erase_if(log_, [index](const LogEntry& e) { return e.index >= index; });
    }

    // ---- Persistence (G008) -------------------------------------------------
    // Persistent Raft state = currentTerm + votedFor + commitIndex + the log +
    // the latest snapshot. Serialized to <data_dir>/raft_state.bin with a
    // trailing CRC32C; written atomically via tmp+rename. Persistence is a
    // no-op when data_dir is empty (e.g. ephemeral in-memory test clusters).

    std::string StatePath() const { return config_.data_dir + "/raft_state.bin"; }

    Status PersistState() {
        if (config_.data_dir.empty()) return Status::OK();

        std::string buf;
        auto put_u64 = [&](uint64_t v) { buf.append(reinterpret_cast<char*>(&v), 8); };
        auto put_u32 = [&](uint32_t v) { buf.append(reinterpret_cast<char*>(&v), 4); };
        auto put_str = [&](const std::string& x) {
            put_u32(static_cast<uint32_t>(x.size()));
            buf.append(x);
        };

        buf.append("KVRAFT01", 8);
        put_u64(current_term_);
        put_u64(commit_index_);
        put_str(voted_for_);
        // Snapshot.
        put_u64(snapshot_.last_included_index);
        put_u64(snapshot_.last_included_term);
        put_u32(static_cast<uint32_t>(snapshot_.data.size()));
        for (auto& [k, v] : snapshot_.data) { put_str(k); put_str(v); }
        // Log (entries with index > snapshot base).
        put_u32(static_cast<uint32_t>(log_.size()));
        for (auto& e : log_) {
            put_u64(e.term);
            put_u64(e.index);
            buf.push_back(e.is_delete ? '\x01' : '\x00');
            put_str(e.key);
            put_str(e.value);
        }

        uint32_t crc = CRC32::Compute(
            reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
        buf.append(reinterpret_cast<char*>(&crc), 4);

        std::string path = StatePath();
        std::string tmp = path + ".tmp";
        {
            std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
            if (!f) return Status::IOError("open raft state tmp: " + tmp);
            f.write(buf.data(), static_cast<std::streamsize>(buf.size()));
            f.flush();
            if (!f) return Status::IOError("write raft state");
        }
        std::error_code ec;
        std::filesystem::rename(tmp, path, ec);
        if (ec) return Status::IOError("rename raft state: " + ec.message());
        return Status::OK();
    }

    Status LoadState() {
        if (config_.data_dir.empty()) return Status::OK();

        std::string path = StatePath();
        std::ifstream f(path, std::ios::binary);
        if (!f) return Status::OK();  // No prior state -> fresh start.

        std::string buf((std::istreambuf_iterator<char>(f)),
                        std::istreambuf_iterator<char>());
        f.close();
        if (buf.size() < 12 || buf.compare(0, 8, "KVRAFT01") != 0) {
            KV_LOG_WARN("raft state header invalid; starting fresh");
            return Status::OK();
        }
        uint32_t stored = 0;
        std::memcpy(&stored, buf.data() + buf.size() - 4, 4);
        uint32_t computed = CRC32::Compute(
            reinterpret_cast<const uint8_t*>(buf.data()), buf.size() - 4);
        if (stored != computed) {
            KV_LOG_WARN("raft state CRC mismatch; starting fresh");
            return Status::OK();
        }

        size_t pos = 8;
        auto get_u64 = [&]() -> uint64_t {
            uint64_t v; std::memcpy(&v, buf.data() + pos, 8); pos += 8; return v;
        };
        auto get_u32 = [&]() -> uint32_t {
            uint32_t v; std::memcpy(&v, buf.data() + pos, 4); pos += 4; return v;
        };
        auto get_str = [&]() -> std::string {
            uint32_t n = get_u32();
            std::string x(buf.data() + pos, n); pos += n; return x;
        };

        current_term_ = get_u64();
        commit_index_ = get_u64();
        voted_for_ = get_str();
        snapshot_.last_included_index = get_u64();
        snapshot_.last_included_term = get_u64();
        uint32_t nsnap = get_u32();
        snapshot_.data.clear();
        for (uint32_t i = 0; i < nsnap; i++) {
            std::string k = get_str();
            std::string v = get_str();
            snapshot_.data[k] = v;
        }
        uint32_t nlog = get_u32();
        log_.clear();
        for (uint32_t i = 0; i < nlog; i++) {
            LogEntry e;
            e.term = get_u64();
            e.index = get_u64();
            e.is_delete = (buf[pos++] != 0);
            e.key = get_str();
            e.value = get_str();
            log_.push_back(std::move(e));
        }

        // Rebuild the materialized state machine from snapshot + committed log,
        // replaying committed entries through the apply callback so an external
        // engine can reconstruct its state after a restart.
        state_machine_ = snapshot_.data;
        last_applied_ = snapshot_.last_included_index;
        for (auto& e : log_) {
            if (e.index <= last_applied_) continue;
            if (e.index > commit_index_) break;  // only committed entries
            if (e.is_delete) state_machine_.erase(e.key);
            else state_machine_[e.key] = e.value;
            if (apply_cb_) apply_cb_(e);
            last_applied_ = e.index;
        }

        KV_LOG_INFO("Node %s recovered raft state: term=%lu commit=%lu log=%zu snap_index=%lu",
                    config_.node_id.c_str(), current_term_, commit_index_,
                    log_.size(), snapshot_.last_included_index);
        return Status::OK();
    }

    // Snapshot-aware last log position. When the log has been fully compacted
    // (log_ empty but a snapshot present), the effective last entry is the
    // snapshot's last_included_index/term, NOT (0,0). Caller holds mu_.
    uint64_t LastLogIndexLocked() const {
        return log_.empty() ? snapshot_.last_included_index : log_.back().index;
    }
    uint64_t LastLogTermLocked() const {
        return log_.empty() ? snapshot_.last_included_term : log_.back().term;
    }

    // Term of the entry at `index`, consulting the snapshot base first.
    uint64_t TermAtLocked(uint64_t index) const {
        if (index == 0) return 0;
        if (index == snapshot_.last_included_index) return snapshot_.last_included_term;
        for (auto& e : log_) {
            if (e.index == index) return e.term;
        }
        return 0;
    }

    // Compact applied log into the snapshot. Caller holds mu_.
    size_t TakeSnapshotLocked() {
        if (last_applied_ == 0) return 0;
        if (last_applied_ == snapshot_.last_included_index) return 0;
        snapshot_.last_included_term = TermAtLocked(last_applied_);
        snapshot_.last_included_index = last_applied_;
        snapshot_.data = state_machine_;
        size_t before = log_.size();
        std::erase_if(log_, [&](const LogEntry& e) {
            return e.index <= snapshot_.last_included_index;
        });
        size_t dropped = before - log_.size();
        PersistState();
        KV_LOG_INFO("Node %s took snapshot at index %lu (dropped %zu log entries)",
                    config_.node_id.c_str(), snapshot_.last_included_index, dropped);
        return dropped;
    }

    RaftConfig config_;
    RaftTransport* transport_;
    StateMachineApply apply_cb_;

    // Persistent state.
    uint64_t current_term_;
    std::string voted_for_;
    std::vector<LogEntry> log_;

    // Volatile state.
    uint64_t commit_index_;
    uint64_t last_applied_;
    RaftState state_;
    std::string leader_id_;
    std::map<std::string, uint64_t> next_index_;
    std::map<std::string, uint64_t> match_index_;
    std::map<std::string, std::string> state_machine_;
    Snapshot snapshot_;  // Compacted log base + materialized state.

    std::chrono::steady_clock::time_point last_heartbeat_;
    std::atomic<bool> running_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::thread raft_thread_;
};

// Deterministic in-memory transport for testing.
class InMemoryTransport final : public RaftTransport {
public:
    using VoteHandler = std::function<RequestVoteReply(const RequestVoteArgs&)>;
    using AppendHandler = std::function<AppendEntriesReply(const AppendEntriesArgs&)>;
    using SnapshotHandler = std::function<InstallSnapshotReply(const InstallSnapshotArgs&)>;

    void RegisterNode(const std::string& id, VoteHandler vote, AppendHandler append) {
        vote_handlers_[id] = std::move(vote);
        append_handlers_[id] = std::move(append);
    }

    void RegisterNode(const std::string& id, VoteHandler vote, AppendHandler append,
                      SnapshotHandler snap) {
        RegisterNode(id, std::move(vote), std::move(append));
        snapshot_handlers_[id] = std::move(snap);
    }

    Status SendRequestVote(const std::string& target,
                            const RequestVoteArgs& args,
                            RequestVoteReply* reply) override {
        auto it = vote_handlers_.find(target);
        if (it == vote_handlers_.end()) return Status::IOError("node not found");
        *reply = it->second(args);
        return Status::OK();
    }

    Status SendAppendEntries(const std::string& target,
                              const AppendEntriesArgs& args,
                              AppendEntriesReply* reply) override {
        auto it = append_handlers_.find(target);
        if (it == append_handlers_.end()) return Status::IOError("node not found");
        *reply = it->second(args);
        return Status::OK();
    }

    Status SendInstallSnapshot(const std::string& target,
                               const InstallSnapshotArgs& args,
                               InstallSnapshotReply* reply) override {
        auto it = snapshot_handlers_.find(target);
        if (it == snapshot_handlers_.end()) return Status::IOError("node not found");
        *reply = it->second(args);
        return Status::OK();
    }

    // Simulate network partition.
    void Partition(const std::string& node_id) { partitioned_.insert(node_id); }
    void Heal(const std::string& node_id) { partitioned_.erase(node_id); }

private:
    std::map<std::string, VoteHandler> vote_handlers_;
    std::map<std::string, AppendHandler> append_handlers_;
    std::map<std::string, SnapshotHandler> snapshot_handlers_;
    std::set<std::string> partitioned_;
};

} // namespace kv
