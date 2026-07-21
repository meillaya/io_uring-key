// kernel-v2: io_uring-based log-structured KV store with Redis-compatible API.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <string>

#include "engine/engine.h"
#include "io/sync_backend.h"
#include "server/server.h"

#ifdef KV_HAS_IOURING
#include "io/iouring_backend.h"
#endif

using namespace kv;

static volatile sig_atomic_t g_shutdown = 0;

static void SignalHandler(int) { g_shutdown = 1; }

int main(int argc, char* argv[]) {
    std::string data_dir = "/tmp/kv_server_data";
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    std::string backend_name = "sync";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--data-dir" && i + 1 < argc) data_dir = argv[++i];
        else if (arg == "--host" && i + 1 < argc) host = argv[++i];
        else if (arg == "--port" && i + 1 < argc) {
            const std::string port_str = argv[++i];
            try {
                int parsed = std::stoi(port_str);
                if (parsed < 0 || parsed > 65535) {
                    std::fprintf(stderr, "Invalid --port '%s' (expected 0-65535)\n",
                                 port_str.c_str());
                    return 1;
                }
                port = static_cast<uint16_t>(parsed);
            } catch (const std::exception&) {
                std::fprintf(stderr, "Invalid --port '%s' (expected 0-65535)\n",
                             port_str.c_str());
                return 1;
            }
        }
        else if (arg == "--backend" && i + 1 < argc) backend_name = argv[++i];
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
        if (!b->Init().ok()) {
            std::fprintf(stderr, "Failed to init io_uring backend\n");
            return 1;
        }
        backend = std::move(b);
    }
#endif
    else {
        std::fprintf(stderr, "Unknown backend: %s (using sync)\n", backend_name.c_str());
        backend = std::make_unique<SyncBackend>();
    }

    EngineOptions opts;
    opts.data_dir = data_dir;
    opts.sync_wal = true;

    Engine engine(std::move(backend), opts);
    auto s = engine.Open();
    if (!s.ok()) {
        std::fprintf(stderr, "Failed to open engine: %s\n", s.ToString().c_str());
        return 1;
    }

    ServerOptions server_opts;
    server_opts.host = host;
    server_opts.port = port;

    Server server(engine, server_opts);
    s = server.Start();
    if (!s.ok()) {
        std::fprintf(stderr, "Failed to start server: %s\n", s.ToString().c_str());
        return 1;
    }

    std::printf("kernel-v2 KV server running on %s:%d (backend: %s, data: %s)\n",
                host.c_str(), port, backend_name.c_str(), data_dir.c_str());
    std::printf("Press Ctrl+C to stop.\n");

    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::printf("\nShutting down...\n");
    server.Stop();
    engine.FlushAndSync();
    return 0;
}
