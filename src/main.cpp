// kernel-v2: io_uring-based log-structured KV store with Redis-compatible API.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <memory>
#include <stdexcept>
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
    std::string requirepass;
    if (const char* env_pass = std::getenv("KV_REQUIREPASS")) requirepass = env_pass;
    std::string tls_cert;
    std::string tls_key;
    if (const char* env_cert = std::getenv("KV_TLS_CERT")) tls_cert = env_cert;
    if (const char* env_key = std::getenv("KV_TLS_KEY")) tls_key = env_key;
    size_t max_connections = 1024;
    int idle_timeout_ms = 300000;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--data-dir" && i + 1 < argc) data_dir = argv[++i];
        else if (arg == "--host" && i + 1 < argc) host = argv[++i];
        else if (arg == "--requirepass" && i + 1 < argc) requirepass = argv[++i];
        else if (arg == "--tls-cert" && i + 1 < argc) tls_cert = argv[++i];
        else if (arg == "--tls-key" && i + 1 < argc) tls_key = argv[++i];
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
        else {
            std::fprintf(stderr, "Unknown or incomplete option: %s\n", arg.c_str());
            return 1;
        }
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
        std::fprintf(stderr, "Unknown backend: %s\n", backend_name.c_str());
        return 1;
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
    server_opts.max_connections = max_connections;
    server_opts.idle_timeout_ms = idle_timeout_ms;
    server_opts.requirepass = requirepass;
    server_opts.tls_cert = tls_cert;
    server_opts.tls_key = tls_key;

    Server server(engine, server_opts);
    s = server.Start();
    if (!s.ok()) {
        std::fprintf(stderr, "Failed to start server: %s\n", s.ToString().c_str());
        return 1;
    }

    std::printf("kernel-v2 KV server running on %s:%d (backend: %s, data: %s, auth: %s, tls: %s)\n",
                host.c_str(), port, backend_name.c_str(), data_dir.c_str(),
                requirepass.empty() ? "off" : "on",
                tls_cert.empty() ? "off" : "on");
    std::printf("Press Ctrl+C to stop.\n");

    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::printf("\nShutting down...\n");
    server.Stop();
    engine.FlushAndSync();
    return 0;
}
