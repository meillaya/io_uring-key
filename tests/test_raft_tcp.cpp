#include <doctest/doctest.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "raft/raft.h"
#include "raft/tcp_transport.h"

using namespace kv;

namespace {

std::string MakeRaftTcpDir() {
    char tmpl[] = "/tmp/kv_raft_tcp_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

// A 3-node cluster over real TCP on localhost. Each node owns its own
// TcpRaftTransport (its own listening port) and a recording state machine.
struct TcpCluster {
    std::vector<std::string> dirs;
    std::vector<std::unique_ptr<TcpRaftTransport>> transports;
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::array<std::map<std::string, std::string>, 3> sms;
    std::array<std::mutex, 3> sms_mu;
    std::vector<std::string> ids = {"node1", "node2", "node3"};

    void Build() {
        for (int i = 0; i < 3; i++) dirs.push_back(MakeRaftTcpDir());

        // Create transports first (ephemeral ports), then learn each port.
        for (int i = 0; i < 3; i++) {
            auto t = std::make_unique<TcpRaftTransport>("127.0.0.1", 0);
            REQUIRE(t->Start().ok());
            transports.push_back(std::move(t));
        }
        // Wire peer maps: every node knows every other node's address.
        for (int i = 0; 0 + i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                if (i == j) continue;
                transports[i]->SetPeer(ids[j], "127.0.0.1", transports[j]->port());
            }
        }
        // Create nodes + bind transport handlers to the node's RPC methods.
        for (int i = 0; i < 3; i++) {
            RaftConfig cfg;
            cfg.node_id = ids[i];
            cfg.peers = ids;
            cfg.data_dir = dirs[i];
            cfg.election_timeout_min_ms = 150 + i * 60;  // stagger
            cfg.election_timeout_max_ms = 300 + i * 60;
            cfg.heartbeat_interval_ms = 40;

            auto* sm = &sms[i];
            auto* smu = &sms_mu[i];
            auto node = std::make_unique<RaftNode>(cfg, transports[i].get(),
                [sm, smu](const LogEntry& e) {
                    std::lock_guard lock(*smu);
                    if (e.is_delete) sm->erase(e.key);
                    else (*sm)[e.key] = e.value;
                });
            auto* np = node.get();
            transports[i]->SetHandlers(
                [np](const RequestVoteArgs& a) { return np->HandleRequestVote(a); },
                [np](const AppendEntriesArgs& a) { return np->HandleAppendEntries(a); },
                [np](const InstallSnapshotArgs& a) { return np->HandleInstallSnapshot(a); });
            nodes.push_back(std::move(node));
        }
    }

    void StartAll() {
        for (auto& n : nodes) REQUIRE(n->Start().ok());
    }

    void StopAll() {
        for (auto& n : nodes) if (n) n->Stop();
        for (auto& t : transports) if (t) t->Stop();
    }

    int LeaderIndex() {
        int leader = -1, count = 0;
        for (int i = 0; i < 3; i++) {
            if (nodes[i] && nodes[i]->is_leader()) { leader = i; count++; }
        }
        return count == 1 ? leader : -1;
    }

    std::map<std::string, std::string> SM(int i) {
        std::lock_guard lock(sms_mu[i]);
        return sms[i];
    }

    void Cleanup() {
        for (auto& d : dirs) std::filesystem::remove_all(d);
    }
};

} // namespace

TEST_CASE("raft tcp: 3-process-style cluster elects and replicates over TCP") {
    TcpCluster c;
    c.Build();
    c.StartAll();

    // Wait for a single leader to emerge over real TCP.
    int leader = -1;
    for (int attempt = 0; attempt < 50; attempt++) {
        leader = c.LeaderIndex();
        if (leader >= 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    REQUIRE_MESSAGE(leader >= 0, "no leader elected over TCP");

    // Replicate entries through the leader.
    for (int i = 0; i < 10; i++) {
        REQUIRE(c.nodes[leader]->Propose("key" + std::to_string(i),
                                         "value" + std::to_string(i)).ok());
    }

    // Wait until all three state machines converge.
    bool converged = false;
    for (int attempt = 0; attempt < 50; attempt++) {
        converged = true;
        for (int i = 0; i < 3; i++) {
            auto sm = c.SM(i);
            for (int j = 0; j < 10; j++) {
                auto it = sm.find("key" + std::to_string(j));
                if (it == sm.end() || it->second != "value" + std::to_string(j)) {
                    converged = false;
                }
            }
        }
        if (converged) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    CHECK_MESSAGE(converged, "state did not replicate to all 3 nodes over TCP");

    // ---- Failover: stop the leader; the remaining two must elect a new one. ----
    c.nodes[leader]->Stop();
    c.transports[leader]->Stop();

    int survivors[2];
    int sc = 0;
    for (int i = 0; i < 3; i++) if (i != leader) survivors[sc++] = i;

    int new_leader = -1;
    for (int attempt = 0; attempt < 60; attempt++) {
        int l0 = c.nodes[survivors[0]]->is_leader() ? survivors[0] : -1;
        int l1 = c.nodes[survivors[1]]->is_leader() ? survivors[1] : -1;
        if (l0 >= 0 && l1 < 0) { new_leader = l0; break; }
        if (l1 >= 0 && l0 < 0) { new_leader = l1; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    REQUIRE_MESSAGE(new_leader >= 0, "no new leader after killing the old one");

    // The new leader must still replicate a fresh write to the other survivor.
    REQUIRE(c.nodes[new_leader]->Propose("after_failover", "ok").ok());
    int other = (survivors[0] == new_leader) ? survivors[1] : survivors[0];
    bool replicated = false;
    for (int attempt = 0; attempt < 50; attempt++) {
        auto sm = c.SM(other);
        if (sm.count("after_failover") && sm["after_failover"] == "ok") {
            replicated = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    CHECK_MESSAGE(replicated, "post-failover write did not replicate over TCP");

    c.StopAll();
    c.Cleanup();
}
