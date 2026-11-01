#pragma once
// Raft-lite: leader election, log replication, KV state machine, snapshots,
// crash recovery, leader-only reads.

#include <atomic>
#include <cerrno>
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

#include <fcntl.h>
#include <unistd.h>

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
    uint64_t term = 0;
    bool vote_granted = false;
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
    uint64_t term = 0;
    bool success = false;
    uint64_t match_index = 0;  // For fast backup.
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
    uint64_t term = 0;
    bool success = false;
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

        LogEntry entry{};
        entry.term = current_term_;
        entry.index = log_.empty() ? 1 : log_.back().index + 1;
        entry.key = key;
        entry.value = value;
        entry.is_delete = is_delete;

        log_.push_back(entry);

        // Replicate to peers (or self-commit for single node).
        if (config_.peers.size() == 1) {
            uint64_t old_commit = commit_index_;
            commit_index_ = entry.index;
            auto persisted = PersistState();
            if (!persisted.ok()) {
                log_.pop_back();
                commit_index_ = old_commit;
                return persisted;
            }
            ApplyCommitted();
        } else {
            // Persist the new log entry before replicating; commit_index_ is
            // persisted later once a majority acknowledges (UpdateCommitIndex).
            auto persisted = PersistState();
            if (!persisted.ok()) {
                log_.pop_back();
                return persisted;
            }
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
        std::unique_lock<std::mutex> lock(mu_, std::defer_lock);
        if (!lock.try_lock()) {
            return InstallSnapshotReply{};
        }
        InstallSnapshotReply reply{};
        reply.term = current_term_;
        reply.success = false;

        if (args.term < current_term_) {
            return reply;
        }

        if (args.term > current_term_) {
            current_term_ = args.term;
            voted_for_ = "";
            auto persisted = PersistState();
            if (!persisted.ok()) {
                KV_LOG_ERROR("Node %s cannot persist term %lu: %s",
                             config_.node_id.c_str(), current_term_,
                             persisted.ToString().c_str());
                reply.term = current_term_;
                return reply;
            }
        }

        state_ = RaftState::kFollower;
        leader_id_ = args.leader_id;
        last_heartbeat_ = std::chrono::steady_clock::now();

        if (args.last_included_index < snapshot_.last_included_index) {
            reply.term = current_term_;
            return reply;
        }

        // An equal base is already installed. Report success so the leader
        // advances next_index instead of resending the same snapshot forever.
        if (args.last_included_index == snapshot_.last_included_index) {
            reply.term = current_term_;
            reply.success = true;
            return reply;
        }

        // Save the old state so a failed persistence does not leave memory
        // ahead of disk.
        Snapshot old_snapshot = snapshot_;
        std::vector<LogEntry> old_log = log_;
        std::map<std::string, std::string> old_state_machine = state_machine_;
        uint64_t old_last_applied = last_applied_;
        uint64_t old_commit = commit_index_;

        snapshot_.last_included_index = args.last_included_index;
        snapshot_.last_included_term = args.last_included_term;
        snapshot_.data = args.data;

        std::erase_if(log_, [&](const LogEntry& e) {
            return e.index <= args.last_included_index;
        });

        state_machine_ = args.data;
        last_applied_ = args.last_included_index;
        if (commit_index_ < args.last_included_index) {
            commit_index_ = args.last_included_index;
        }

        auto persisted = PersistState();
        if (!persisted.ok()) {
            snapshot_ = std::move(old_snapshot);
            log_ = std::move(old_log);
            state_machine_ = std::move(old_state_machine);
            last_applied_ = old_last_applied;
            commit_index_ = old_commit;
            KV_LOG_ERROR("Node %s cannot persist snapshot: %s",
                         config_.node_id.c_str(), persisted.ToString().c_str());
            reply.term = current_term_;
            return reply;
        }

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
        std::unique_lock<std::mutex> lock(mu_, std::defer_lock);
        if (!lock.try_lock()) {
            return RequestVoteReply{};
        }
        RequestVoteReply reply{};
        reply.term = current_term_;
        reply.vote_granted = false;

        bool term_changed = false;
        if (args.term > current_term_) {
            current_term_ = args.term;
            state_ = RaftState::kFollower;
            voted_for_ = "";
            term_changed = true;
        }

        if (args.term < current_term_) return reply;

        bool granted = false;
        if (voted_for_.empty() || voted_for_ == args.candidate_id) {
            uint64_t my_last_term = LastLogTermLocked();
            uint64_t my_last_index = LastLogIndexLocked();

            if (args.last_log_term > my_last_term ||
                (args.last_log_term == my_last_term &&
                 args.last_log_index >= my_last_index)) {
                voted_for_ = args.candidate_id;
                granted = true;
            }
        }

        // Term and vote must reach disk before the reply leaves the node.
        if (term_changed || granted) {
            auto persisted = PersistState();
            if (!persisted.ok()) {
                KV_LOG_ERROR("Node %s cannot persist vote: %s",
                             config_.node_id.c_str(), persisted.ToString().c_str());
                if (granted) voted_for_ = "";
                reply.term = current_term_;
                return reply;
            }
        }

        if (granted) {
            reply.vote_granted = true;
            last_heartbeat_ = std::chrono::steady_clock::now();
        }

        reply.term = current_term_;
        return reply;
    }

    AppendEntriesReply HandleAppendEntries(const AppendEntriesArgs& args) {
        std::unique_lock<std::mutex> lock(mu_, std::defer_lock);
        if (!lock.try_lock()) {
            return AppendEntriesReply{};
        }
        AppendEntriesReply reply{};
        reply.term = current_term_;
        reply.success = false;
        reply.match_index = 0;

        if (args.term > current_term_) {
            current_term_ = args.term;
            state_ = RaftState::kFollower;
            voted_for_ = "";
            auto persisted = PersistState();
            if (!persisted.ok()) {
                KV_LOG_ERROR("Node %s cannot persist term %lu: %s",
                             config_.node_id.c_str(), current_term_,
                             persisted.ToString().c_str());
                reply.term = current_term_;
                return reply;
            }
        }

        if (args.term < current_term_) {
            reply.term = current_term_;
            return reply;
        }

        // Recognize leader.
        leader_id_ = args.leader_id;
        state_ = RaftState::kFollower;
        last_heartbeat_ = std::chrono::steady_clock::now();

        bool log_changed = false;
        bool commit_changed = false;

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
                    log_changed = true;
                    auto persisted = PersistState();
                    if (!persisted.ok()) {
                        KV_LOG_ERROR("Node %s cannot persist log truncation: %s",
                                     config_.node_id.c_str(),
                                     persisted.ToString().c_str());
                    }
                    reply.term = current_term_;
                    return reply;
                }
            }
        }

        // Append new entries. The reply reports the last index this request
        // covered, never the follower's own tail, so an old uncommitted suffix
        // cannot be mistaken for replicated data.
        uint64_t last_covered = args.prev_log_index;
        for (auto& entry : args.entries) {
            auto existing = FindLogEntry(entry.index);
            if (existing && existing->term != entry.term) {
                TruncateLogFrom(entry.index);
                existing = std::nullopt;
                log_changed = true;
            }
            if (!existing) {
                log_.push_back(entry);
                log_changed = true;
            }
            last_covered = entry.index;
        }

        // Update commit index, persist it, then apply. A failed persist rejects
        // the request instead of advancing the state machine.
        uint64_t old_commit = commit_index_;
        if (args.leader_commit > commit_index_) {
            uint64_t last_log_idx = log_.empty() ? 0 : log_.back().index;
            uint64_t next_commit = std::min(args.leader_commit, last_log_idx);
            if (next_commit > commit_index_) {
                commit_index_ = next_commit;
                commit_changed = true;
            }
        }

        if (log_changed || commit_changed) {
            auto persisted = PersistState();
            if (!persisted.ok()) {
                commit_index_ = old_commit;
                KV_LOG_ERROR("Node %s cannot persist append: %s",
                             config_.node_id.c_str(), persisted.ToString().c_str());
                reply.term = current_term_;
                return reply;
            }
        }

        ApplyCommitted();
        reply.success = true;
        reply.match_index = last_covered;
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
        auto persisted = PersistState();
        if (!persisted.ok()) {
            KV_LOG_ERROR("Node %s cannot persist election: %s",
                         config_.node_id.c_str(), persisted.ToString().c_str());
            state_ = RaftState::kFollower;
            voted_for_ = "";
            return;
        }

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
            RequestVoteReply reply{};
            auto s = transport_->SendRequestVote(peer, args, &reply);
            if (!s.ok()) continue;

            if (reply.vote_granted) votes++;
            if (reply.term > current_term_) {
                current_term_ = reply.term;
                state_ = RaftState::kFollower;
                voted_for_ = "";
                auto step_down = PersistState();
                if (!step_down.ok()) {
                    KV_LOG_ERROR("Node %s cannot persist term %lu: %s",
                                 config_.node_id.c_str(), current_term_,
                                 step_down.ToString().c_str());
                }
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
                InstallSnapshotArgs sargs{};
                sargs.term = current_term_;
                sargs.leader_id = config_.node_id;
                sargs.last_included_index = snapshot_.last_included_index;
                sargs.last_included_term = snapshot_.last_included_term;
                sargs.data = snapshot_.data;
                InstallSnapshotReply sreply{};
                auto ss = transport_->SendInstallSnapshot(peer, sargs, &sreply);
                if (ss.ok() && sreply.success) {
                    next_index_[peer] = snapshot_.last_included_index + 1;
                    match_index_[peer] = snapshot_.last_included_index;
                    UpdateCommitIndex();
                } else if (ss.ok() && sreply.term > current_term_) {
                    current_term_ = sreply.term;
                    state_ = RaftState::kFollower;
                    voted_for_ = "";
                    auto persisted = PersistState();
                    if (!persisted.ok()) {
                        KV_LOG_ERROR("Node %s cannot persist term %lu: %s",
                                     config_.node_id.c_str(), current_term_,
                                     persisted.ToString().c_str());
                    }
                    return;
                }
                continue;  // resume normal AppendEntries next round
            }

            AppendEntriesArgs args{};
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

            AppendEntriesReply reply{};
            auto s = transport_->SendAppendEntries(peer, args, &reply);
            if (s.ok()) {
                if (reply.success) {
                    uint64_t matched = std::min(reply.match_index, LastLogIndexLocked());
                    next_index_[peer] = matched + 1;
                    match_index_[peer] = matched;
                    // Update commit index.
                    UpdateCommitIndex();
                } else if (reply.term > current_term_) {
                    current_term_ = reply.term;
                    state_ = RaftState::kFollower;
                    voted_for_ = "";
                    auto persisted = PersistState();
                    if (!persisted.ok()) {
                        KV_LOG_ERROR("Node %s cannot persist term %lu: %s",
                                     config_.node_id.c_str(), current_term_,
                                     persisted.ToString().c_str());
                    }
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
                    uint64_t old_commit = commit_index_;
                    commit_index_ = n;
                    auto persisted = PersistState();
                    if (!persisted.ok()) {
                        commit_index_ = old_commit;
                        KV_LOG_ERROR("Node %s cannot persist commit index %lu: %s",
                                     config_.node_id.c_str(), commit_index_,
                                     persisted.ToString().c_str());
                        state_ = RaftState::kFollower;
                        leader_id_ = "";
                        return;
                    }
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

    // ---- Persistence (G008, power-loss-durable via G010) --------------------
    // Persistent Raft state = currentTerm + votedFor + commitIndex + the log +
    // the latest snapshot. Serialized to <data_dir>/raft_state.bin with a
    // trailing CRC32C; written atomically via tmp+rename. Persistence is a
    // no-op when data_dir is empty (e.g. ephemeral in-memory test clusters).
    //
    // Durability (G010): the temp file is fsync'd before rename and the
    // containing directory is fsync'd after rename, so votedFor/currentTerm/
    // log/snapshot survive power loss. This closes the renamed-but-not-fsync'd
    // window that could otherwise permit a double-vote or two leaders in one
    // term after a crash.

    std::string StatePath() const { return config_.data_dir + "/raft_state.bin"; }

    // ---- Durable-write helpers (G010 power-loss safety) ---------------------
    // fsync a file by path (open fd, ::fsync, close) so its data + metadata
    // reach stable storage. Used to make the just-written temp state file
    // durable BEFORE renaming it into place.
    static Status SyncFileNamed(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            return Status::IOError("open for fsync: " + path + ": " +
                                   std::strerror(errno));
        }
        int rc = ::fsync(fd);
        int e = errno;
        ::close(fd);
        if (rc != 0) {
            return Status::IOError("fsync: " + path + ": " + std::strerror(e));
        }
        return Status::OK();
    }

    // fsync a directory so a rename/unlink within it is durable across power
    // loss (the directory-entry update itself must reach stable storage).
    static Status SyncDir(const std::string& dir) {
        int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
        if (fd < 0) {
            return Status::IOError("open dir for fsync: " + dir + ": " +
                                   std::strerror(errno));
        }
        int rc = ::fsync(fd);
        int e = errno;
        ::close(fd);
        if (rc != 0) {
            return Status::IOError("fsync dir: " + dir + ": " + std::strerror(e));
        }
        return Status::OK();
    }

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
        // (a) fsync the just-written temp file so its data + metadata reach
        //     stable storage BEFORE the rename. Without this a power loss can
        //     leave a renamed-but-empty/partial raft_state.bin, corrupting
        //     votedFor/currentTerm and opening a double-vote / two-leaders-in-
        //     one-term window.
        auto fs = SyncFileNamed(tmp);
        if (!fs.ok()) return fs;
        std::error_code ec;
        std::filesystem::rename(tmp, path, ec);
        if (ec) return Status::IOError("rename raft state: " + ec.message());
        // (b) fsync the containing directory so the rename (the directory-entry
        //     update) is itself durable across power loss.
        auto ds = SyncDir(config_.data_dir);
        if (!ds.ok()) return ds;
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
            return Status::Corruption("raft state: invalid header");
        }

        uint32_t stored = 0;
        std::memcpy(&stored, buf.data() + buf.size() - 4, 4);
        uint32_t computed = CRC32::Compute(
            reinterpret_cast<const uint8_t*>(buf.data()), buf.size() - 4);
        if (stored != computed) {
            return Status::Corruption("raft state: CRC mismatch");
        }

        size_t pos = 8;
        auto remaining = [&]() { return buf.size() - 4 - pos; };
        auto get_u64 = [&](uint64_t* out) -> bool {
            if (remaining() < 8) return false;
            std::memcpy(out, buf.data() + pos, 8);
            pos += 8;
            return true;
        };
        auto get_u32 = [&](uint32_t* out) -> bool {
            if (remaining() < 4) return false;
            std::memcpy(out, buf.data() + pos, 4);
            pos += 4;
            return true;
        };
        auto get_str = [&](std::string* out) -> bool {
            uint32_t n = 0;
            if (!get_u32(&n) || n > remaining()) return false;
            out->assign(buf.data() + pos, n);
            pos += n;
            return true;
        };

        if (!get_u64(&current_term_) || !get_u64(&commit_index_) ||
            !get_str(&voted_for_)) {
            return Status::Corruption("raft state: truncated header");
        }

        if (!get_u64(&snapshot_.last_included_index) ||
            !get_u64(&snapshot_.last_included_term)) {
            return Status::Corruption("raft state: truncated snapshot header");
        }

        uint32_t nsnap = 0;
        if (!get_u32(&nsnap)) {
            return Status::Corruption("raft state: missing snapshot count");
        }
        if (nsnap > remaining() / 8) {
            return Status::Corruption("raft state: snapshot count exceeds file");
        }
        snapshot_.data.clear();
        for (uint32_t i = 0; i < nsnap; i++) {
            std::string key;
            std::string value;
            if (!get_str(&key) || !get_str(&value)) {
                return Status::Corruption("raft state: truncated snapshot entry");
            }
            snapshot_.data[key] = value;
        }

        uint32_t nlog = 0;
        if (!get_u32(&nlog)) {
            return Status::Corruption("raft state: missing log count");
        }
        if (nlog > remaining() / 25) {
            return Status::Corruption("raft state: log count exceeds file");
        }

        log_.clear();
        uint64_t previous_index = snapshot_.last_included_index;
        for (uint32_t i = 0; i < nlog; i++) {
            LogEntry e{};
            uint8_t tombstone = 0;
            if (!get_u64(&e.term) || !get_u64(&e.index)) {
                return Status::Corruption("raft state: truncated log entry");
            }
            if (remaining() < 1) {
                return Status::Corruption("raft state: truncated tombstone");
            }
            tombstone = static_cast<uint8_t>(buf[pos++]);
            if (tombstone > 1) {
                return Status::Corruption("raft state: invalid tombstone");
            }
            e.is_delete = tombstone != 0;
            if (!get_str(&e.key) || !get_str(&e.value)) {
                return Status::Corruption("raft state: truncated log strings");
            }
            if (e.index != previous_index + 1) {
                return Status::Corruption("raft state: log index is not contiguous");
            }
            previous_index = e.index;
            log_.push_back(std::move(e));
        }

        if (pos != buf.size() - 4) {
            return Status::Corruption("raft state: trailing bytes");
        }

        if (commit_index_ < snapshot_.last_included_index) {
            return Status::Corruption("raft state: commit is below snapshot");
        }

        // Rebuild the materialized state machine from the snapshot and the
        // committed part of the log.
        state_machine_ = snapshot_.data;
        last_applied_ = snapshot_.last_included_index;
        for (auto& e : log_) {
            if (e.index <= last_applied_) continue;
            if (e.index > commit_index_) break;
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

        Snapshot old_snapshot = snapshot_;
        std::vector<LogEntry> old_log = log_;

        snapshot_.last_included_term = TermAtLocked(last_applied_);
        snapshot_.last_included_index = last_applied_;
        snapshot_.data = state_machine_;
        size_t before = log_.size();
        std::erase_if(log_, [&](const LogEntry& e) {
            return e.index <= snapshot_.last_included_index;
        });
        size_t dropped = before - log_.size();

        auto persisted = PersistState();
        if (!persisted.ok()) {
            snapshot_ = std::move(old_snapshot);
            log_ = std::move(old_log);
            KV_LOG_ERROR("Node %s cannot persist snapshot: %s",
                         config_.node_id.c_str(), persisted.ToString().c_str());
            return 0;
        }

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
        if (partitioned_.contains(target)) return Status::IOError("node partitioned");
        auto it = vote_handlers_.find(target);
        if (it == vote_handlers_.end()) return Status::IOError("node not found");
        *reply = it->second(args);
        return Status::OK();
    }

    Status SendAppendEntries(const std::string& target,
                              const AppendEntriesArgs& args,
                              AppendEntriesReply* reply) override {
        if (partitioned_.contains(target)) return Status::IOError("node partitioned");
        auto it = append_handlers_.find(target);
        if (it == append_handlers_.end()) return Status::IOError("node not found");
        *reply = it->second(args);
        return Status::OK();
    }

    Status SendInstallSnapshot(const std::string& target,
                               const InstallSnapshotArgs& args,
                               InstallSnapshotReply* reply) override {
        if (partitioned_.contains(target)) return Status::IOError("node partitioned");
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
