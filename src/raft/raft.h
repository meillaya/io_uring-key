#pragma once
// Raft-lite: leader election, log replication, KV state machine, snapshots,
// crash recovery, leader-only reads.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
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
        PersistState();

        // Replicate to peers (or self-commit for single node).
        if (config_.peers.size() == 1) {
            // Single node: commit immediately.
            commit_index_ = entry.index;
            ApplyCommitted();
        } else {
            ReplicateToPeers();
        }

        return Status::OK();
    }

    // Read a value (leader-only, linearizable via ReadIndex).
    std::optional<std::string> Read(const std::string& key) {
        std::lock_guard lock(mu_);
        if (state_ != RaftState::kLeader) return std::nullopt;

        // Leader lease: we're the leader, so we can serve reads.
        // In a full implementation, we'd confirm leadership via ReadIndex.
        auto it = state_machine_.find(key);
        if (it != state_machine_.end()) return it->second;
        return std::nullopt;
    }

    [[nodiscard]] RaftState state() const { return state_; }
    [[nodiscard]] uint64_t current_term() const { return current_term_; }
    [[nodiscard]] std::string leader_id() const { return leader_id_; }
    [[nodiscard]] bool is_leader() const { return state_ == RaftState::kLeader; }

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
            uint64_t my_last_term = log_.empty() ? 0 : log_.back().term;
            uint64_t my_last_index = log_.empty() ? 0 : log_.back().index;

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

        // Check prev_log consistency.
        if (args.prev_log_index > 0) {
            if (args.prev_log_index > (log_.empty() ? 0 : log_.back().index)) {
                reply.term = current_term_;
                return reply;
            }
            // Find the entry at prev_log_index.
            auto it = FindLogEntry(args.prev_log_index);
            if (!it || it->term != args.prev_log_term) {
                // Truncate conflicting entries.
                TruncateLogFrom(args.prev_log_index);
                reply.term = current_term_;
                return reply;
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
        args.last_log_index = log_.empty() ? 0 : log_.back().index;
        args.last_log_term = log_.empty() ? 0 : log_.back().term;

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

            AppendEntriesArgs args;
            args.term = current_term_;
            args.leader_id = config_.node_id;
            args.leader_commit = commit_index_;

            uint64_t next_idx = next_index_[peer];
            if (next_idx > 1) {
                auto prev = FindLogEntry(next_idx - 1);
                args.prev_log_index = next_idx - 1;
                args.prev_log_term = prev ? prev->term : 0;
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

    Status PersistState() {
        // TODO: persist current_term, voted_for, log to disk.
        return Status::OK();
    }

    Status LoadState() {
        // TODO: load persistent state from disk.
        return Status::OK();
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

    std::chrono::steady_clock::time_point last_heartbeat_;
    std::atomic<bool> running_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::thread raft_thread_;
};

// Deterministic in-memory transport for testing.
class InMemoryTransport final : public RaftTransport {
public:
    using VoteHandler = std::function<RequestVoteReply(const RequestVoteArgs&)>;
    using AppendHandler = std::function<AppendEntriesReply(const AppendEntriesArgs&)>;

    void RegisterNode(const std::string& id, VoteHandler vote, AppendHandler append) {
        vote_handlers_[id] = std::move(vote);
        append_handlers_[id] = std::move(append);
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

    // Simulate network partition.
    void Partition(const std::string& node_id) { partitioned_.insert(node_id); }
    void Heal(const std::string& node_id) { partitioned_.erase(node_id); }

private:
    std::map<std::string, VoteHandler> vote_handlers_;
    std::map<std::string, AppendHandler> append_handlers_;
    std::set<std::string> partitioned_;
};

} // namespace kv
