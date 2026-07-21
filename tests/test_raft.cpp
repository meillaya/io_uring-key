#include <doctest/doctest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "raft/raft.h"

using namespace kv;

TEST_CASE("raft: single node becomes leader") {
    InMemoryTransport transport;

    RaftConfig config;
    config.node_id = "node1";
    config.peers = {"node1"};
    config.election_timeout_min_ms = 50;
    config.election_timeout_max_ms = 100;

    std::vector<LogEntry> applied;
    RaftNode node(config, &transport, [&](const LogEntry& e) {
        applied.push_back(e);
    });

    transport.RegisterNode("node1",
        [&](const RequestVoteArgs& args) { return node.HandleRequestVote(args); },
        [&](const AppendEntriesArgs& args) { return node.HandleAppendEntries(args); });

    REQUIRE(node.Start().ok());

    // Wait for election.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    CHECK(node.is_leader());
    CHECK(node.current_term() >= 1);

    // Propose a value.
    REQUIRE(node.Propose("key1", "value1").ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Single node commits immediately.
    auto val = node.Read("key1");
    CHECK(val.has_value());
    if (val) CHECK(*val == "value1");

    node.Stop();
}

TEST_CASE("raft: 3-node election") {
    auto transport = std::make_shared<InMemoryTransport>();

    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<RaftConfig> configs;

    for (int i = 0; i < 3; i++) {
        RaftConfig config;
        config.node_id = "node" + std::to_string(i + 1);
        config.peers = {"node1", "node2", "node3"};
        config.election_timeout_min_ms = 100 + i * 50;  // Stagger timeouts.
        config.election_timeout_max_ms = 200 + i * 50;
        configs.push_back(config);
    }

    for (int i = 0; i < 3; i++) {
        auto node = std::make_unique<RaftNode>(configs[i], transport.get(), nullptr);
        auto* node_ptr = node.get();
        std::string id = configs[i].node_id;

        transport->RegisterNode(id,
            [node_ptr](const RequestVoteArgs& args) { return node_ptr->HandleRequestVote(args); },
            [node_ptr](const AppendEntriesArgs& args) { return node_ptr->HandleAppendEntries(args); });

        nodes.push_back(std::move(node));
    }

    for (auto& node : nodes) {
        REQUIRE(node->Start().ok());
    }

    // Wait for election.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Exactly one leader.
    int leaders = 0;
    for (auto& node : nodes) {
        if (node->is_leader()) leaders++;
    }
    CHECK(leaders == 1);

    for (auto& node : nodes) {
        node->Stop();
    }
}

TEST_CASE("raft: log replication across 3 nodes") {
    auto transport = std::make_shared<InMemoryTransport>();

    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::map<std::string, std::string>> state_machines(3);

    for (int i = 0; i < 3; i++) {
        RaftConfig config;
        config.node_id = "node" + std::to_string(i + 1);
        config.peers = {"node1", "node2", "node3"};
        config.election_timeout_min_ms = 100 + i * 50;
        config.election_timeout_max_ms = 200 + i * 50;

        auto* sm = &state_machines[i];
        auto node = std::make_unique<RaftNode>(config, transport.get(),
            [sm](const LogEntry& e) {
                if (e.is_delete) sm->erase(e.key);
                else (*sm)[e.key] = e.value;
            });

        auto* node_ptr = node.get();
        std::string id = config.node_id;

        transport->RegisterNode(id,
            [node_ptr](const RequestVoteArgs& args) { return node_ptr->HandleRequestVote(args); },
            [node_ptr](const AppendEntriesArgs& args) { return node_ptr->HandleAppendEntries(args); });

        nodes.push_back(std::move(node));
    }

    for (auto& node : nodes) {
        REQUIRE(node->Start().ok());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Find the leader.
    RaftNode* leader = nullptr;
    for (auto& node : nodes) {
        if (node->is_leader()) { leader = node.get(); break; }
    }
    REQUIRE(leader != nullptr);

    // Propose values.
    for (int i = 0; i < 10; i++) {
        REQUIRE(leader->Propose("key" + std::to_string(i),
                                "value" + std::to_string(i)).ok());
    }

    // Wait for replication.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify all nodes have the same state.
    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 10; j++) {
            std::string key = "key" + std::to_string(j);
            auto it = state_machines[i].find(key);
            CHECK_MESSAGE(it != state_machines[i].end(),
                          "node", i + 1, " missing key ", key);
            if (it != state_machines[i].end()) {
                CHECK(it->second == "value" + std::to_string(j));
            }
        }
    }

    for (auto& node : nodes) {
        node->Stop();
    }
}

TEST_CASE("raft: leader-only reads") {
    InMemoryTransport transport;

    RaftConfig config;
    config.node_id = "node1";
    config.peers = {"node1"};
    config.election_timeout_min_ms = 50;
    config.election_timeout_max_ms = 100;

    RaftNode node(config, &transport, nullptr);
    transport.RegisterNode("node1",
        [&](const RequestVoteArgs& args) { return node.HandleRequestVote(args); },
        [&](const AppendEntriesArgs& args) { return node.HandleAppendEntries(args); });

    REQUIRE(node.Start().ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    REQUIRE(node.is_leader());
    REQUIRE(node.Propose("readkey", "readval").ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Leader can read.
    auto val = node.Read("readkey");
    CHECK(val.has_value());
    if (val) CHECK(*val == "readval");

    // Non-existent key returns nullopt.
    CHECK(!node.Read("nonexistent").has_value());

    node.Stop();
}
