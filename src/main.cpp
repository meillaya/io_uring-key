// kernel-v2: io_uring-based log-structured KV store with a Redis-compatible API.

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "engine/engine.h"
#include "engine/kv_store.h"
#include "io/sync_backend.h"
#include "raft/raft_store.h"
#include "server/server.h"

#ifdef KV_HAS_IOURING
#include "io/iouring_backend.h"
#endif

using namespace kv;

static volatile sig_atomic_t g_shutdown = 0;

static void SignalHandler(int) { g_shutdown = 1; }

static bool ParsePeer(std::string_view text, RaftStore::Peer* peer) {
    auto at = text.find('@');
    if (at == std::string_view::npos) return false;
    auto colon = text.rfind(':');
    if (colon == std::string_view::npos || colon <= at + 1) return false;

    peer->id = std::string(text.substr(0, at));
    peer->host = std::string(text.substr(at + 1, colon - at - 1));
    if (peer->id.empty() || peer->host.empty()) return false;

    try {
        int parsed = std::stoi(std::string(text.substr(colon + 1)));
        if (parsed < 0 || parsed > 65535) return false;
        peer->port = static_cast<uint16_t>(parsed);
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

int main(int argc, char* argv[]) {
    std::string data_dir = "/tmp/kv_server_data";
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    std::string backend_name = "sync";
    std::string requirepass;
    if (const char* env_pass = std::getenv("KV_REQUIREPASS")) requirepass = env_pass;
    std::string tls_cert;
    std::string tls_key;
    if (const char* env_cert = std::getenv("KV_TLS_CERT")) tls_cert = env_cert;
    if (const char* env_key = std::getenv("KV_TLS_KEY")) tls_key = env_key;
    size_t max_connections = 1024;
    int idle_timeout_ms = 300000;

    std::string raft_id;
    std::string raft_host = "127.0.0.1";
    uint16_t raft_port = 7000;
    std::vector<RaftStore::Peer> raft_peers;
    int raft_commit_timeout_ms = 5000;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--data-dir" && i + 1 < argc) data_dir = argv[++i];
        else if (arg == "--host" && i + 1 < argc) host = argv[++i];
        else if (arg == "--requirepass" && i + 1 < argc) requirepass = argv[++i];
        else if (arg == "--tls-cert" && i + 1 < argc) tls_cert = argv[++i];
        else if (arg == "--tls-key" && i + 1 < argc) tls_key = argv[++i];
        else if (arg == "--raft-id" && i + 1 < argc) raft_id = argv[++i];
        else if (arg == "--raft-host" && i + 1 < argc) raft_host = argv[++i];
        else if (arg == "--raft-peer" && i + 1 < argc) {
            RaftStore::Peer peer;
            if (!ParsePeer(argv[++i], &peer)) {
                std::fprintf(stderr, "Invalid --raft-peer (expected id@host:port)\n");
                return 1;
            }
            raft_peers.push_back(std::move(peer));
        }
        else if (arg == "--raft-commit-timeout-ms" && i + 1 < argc) {
            try {
                raft_commit_timeout_ms = std::stoi(argv[++i]);
                if (raft_commit_timeout_ms <= 0) throw std::out_of_range("non-positive");
            } catch (const std::exception&) {
                std::fprintf(stderr, "Invalid --raft-commit-timeout-ms\n");
                return 1;
            }
        }
        else if (arg == "--max-connections" && i + 1 < argc) {
            try {
                max_connections = static_cast<size_t>(std::stoul(argv[++i]));
            } catch (const std::exception&) {
                std::fprintf(stderr, "Invalid --max-connections\n");
                return 1;
            }
        }
        else if (arg == "--idle-timeout-ms" && i + 1 < argc) {
            try {
                idle_timeout_ms = std::stoi(argv[++i]);
                if (idle_timeout_ms < 0) throw std::out_of_range("negative");
            } catch (const std::exception&) {
                std::fprintf(stderr, "Invalid --idle-timeout-ms\n");
                return 1;
            }
        }
        else if (arg == "--port" && i + 1 < argc) {
            try {
                int parsed = std::stoi(argv[++i]);
                if (parsed < 0 || parsed > 65535) throw std::out_of_range("port");
                port = static_cast<uint16_t>(parsed);
            } catch (const std::exception&) {
                std::fprintf(stderr, "Invalid --port (expected 0-65535)\n");
                return 1;
            }
        }
        else if (arg == "--raft-port" && i + 1 < argc) {
            try {
                int parsed = std::stoi(argv[++i]);
                if (parsed < 0 || parsed > 65535) throw std::out_of_range("port");
                raft_port = static_cast<uint16_t>(parsed);
            } catch (const std::exception&) {
                std::fprintf(stderr, "Invalid --raft-port (expected 0-65535)\n");
                return 1;
            }
        }
        else if (arg == "--backend" && i + 1 < argc) backend_name = argv[++i];
        else {
            std::fprintf(stderr, "Unknown or incomplete option: %s\n", arg.c_str());
            return 1;
        }
    }

    if (raft_peers.size() > 0 && raft_id.empty()) {
        std::fprintf(stderr, "--raft-peer requires --raft-id\n");
        return 1;
    }

    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    std::unique_ptr<IOBackend> backend;
    if (backend_name == "sync") {
        backend = std::make_unique<SyncBackend>();
    }
#ifdef KV_HAS_IOURING
    else if (backend_name == "io_uring") {
        auto b = std::make_unique<IOUringBackend>();
        auto s = b->Init();
        if (!s.ok()) {
            std::fprintf(stderr, "Failed to init io_uring backend: %s\n",
                         s.ToString().c_str());
            return 1;
        }
        backend = std::move(b);
    }
#endif
    else {
        std::fprintf(stderr, "Unknown backend: %s\n", backend_name.c_str());
        return 1;
    }

    EngineOptions opts;
    opts.data_dir = data_dir;
    opts.sync_wal = true;

    std::unique_ptr<KVStore> store;
    std::unique_ptr<RaftStore> raft_store;
    uint16_t bound_raft_port = 0;

    if (raft_id.empty()) {
        auto engine = std::make_unique<Engine>(std::move(backend), opts);
        auto s = engine->Open();
        if (!s.ok()) {
            std::fprintf(stderr, "Failed to open engine: %s\n", s.ToString().c_str());
            return 1;
        }
        store = std::move(engine);
    } else {
        RaftConfig config;
        config.node_id = raft_id;
        config.data_dir = data_dir;
        config.peers.push_back(raft_id);
        for (auto& peer : raft_peers) {
            bool known = false;
            for (auto& id : config.peers) {
                if (id == peer.id) known = true;
            }
            if (!known) config.peers.push_back(peer.id);
        }

        auto engine = std::make_unique<Engine>(std::move(backend), opts);
        raft_store = std::make_unique<RaftStore>(
            std::move(engine), std::move(config), raft_host, raft_port,
            std::move(raft_peers),
            std::chrono::milliseconds(raft_commit_timeout_ms));
        auto s = raft_store->Start();
        if (!s.ok()) {
            std::fprintf(stderr, "Failed to start Raft store: %s\n", s.ToString().c_str());
            return 1;
        }
        bound_raft_port = raft_store->raft_port();
        store = std::move(raft_store);
    }

    ServerOptions server_opts;
    server_opts.host = host;
    server_opts.port = port;
    server_opts.max_connections = max_connections;
    server_opts.idle_timeout_ms = idle_timeout_ms;
    server_opts.requirepass = requirepass;
    server_opts.tls_cert = tls_cert;
    server_opts.tls_key = tls_key;

    Server server(*store, server_opts);
    auto s = server.Start();
    if (!s.ok()) {
        std::fprintf(stderr, "Failed to start server: %s\n", s.ToString().c_str());
        store->FlushAndSync();
        return 1;
    }

    std::printf("kernel-v2 KV server running on %s:%d (backend: %s, data: %s, auth: %s, tls: %s)\n",
                host.c_str(), port, backend_name.c_str(), data_dir.c_str(),
                requirepass.empty() ? "off" : "on",
                tls_cert.empty() ? "off" : "on");
    if (!raft_id.empty()) {
        std::printf("Raft node %s on %s:%u\n", raft_id.c_str(), raft_host.c_str(),
                    bound_raft_port);
    }
    std::printf("Press Ctrl+C to stop.\n");

    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::printf("\nShutting down...\n");
    server.Stop();
    store->FlushAndSync();
    store.reset();
    return 0;
}
