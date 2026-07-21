// YCSB benchmark runner: runs all four backends on YCSB A-F and exports CSV.

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "bench/ycsb.h"
#include "engine/engine.h"
#include "io/mmap_backend.h"
#include "io/sync_backend.h"
#include "io/threadpool_backend.h"

#ifdef KV_HAS_IOURING
#include "io/iouring_backend.h"
#endif

using namespace kv;

static std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_bench_XXXXXX";
    char* dir = mkdtemp(tmpl);
    return dir ? std::string(dir) : "/tmp/kv_bench_fallback";
}

int main(int argc, char* argv[]) {
    std::string output_csv = "benchmark_results.csv";
    size_t num_keys = 10000;
    size_t num_ops = 50000;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--output" && i + 1 < argc) output_csv = argv[++i];
        else if (arg == "--keys" && i + 1 < argc) num_keys = std::stoul(argv[++i]);
        else if (arg == "--ops" && i + 1 < argc) num_ops = std::stoul(argv[++i]);
    }

    std::ofstream csv(output_csv);
    YCSBBenchmark::WriteCSVHeader(csv);

    std::vector<YCSBWorkload> workloads = {
        YCSBWorkload::A, YCSBWorkload::B, YCSBWorkload::C,
        YCSBWorkload::D, YCSBWorkload::E, YCSBWorkload::F,
    };

    auto run_backend = [&](const std::string& name, auto make_backend) {
        for (auto wl : workloads) {
            auto data_dir = MakeTempDir();
            auto backend = make_backend();

            EngineOptions opts;
            opts.data_dir = data_dir;
            opts.memtable_flush_threshold = 4 * 1024 * 1024;
            opts.sync_wal = false;  // Benchmark mode: no fsync per write.

            Engine engine(std::move(backend), opts);
            if (!engine.Open().ok()) {
                std::fprintf(stderr, "Failed to open engine for %s\n", name.c_str());
                std::filesystem::remove_all(data_dir);
                continue;
            }

            YCSBBenchmark bench(engine);
            YCSBConfig config;
            config.workload = wl;
            config.num_keys = num_keys;
            config.num_ops = num_ops;

            auto result = bench.Run(config);
            YCSBBenchmark::ExportCSV(result, csv);

            std::printf("%-12s workload %s: %.0f ops/sec, p50=%luus p95=%luus p99=%luus\n",
                        name.c_str(), WorkloadName(wl), result.throughput(),
                        result.overall_hist.P50(), result.overall_hist.P95(),
                        result.overall_hist.P99());

            std::filesystem::remove_all(data_dir);
        }
    };

    run_backend("sync", [] { return std::make_unique<SyncBackend>(); });
    run_backend("threadpool", [] { return std::make_unique<ThreadPoolBackend>(4); });
    run_backend("mmap", [] { return std::make_unique<MmapBackend>(); });

#ifdef KV_HAS_IOURING
    run_backend("io_uring", [] {
        auto b = std::make_unique<IOUringBackend>();
        b->Init();
        return b;
    });
#endif

    csv.close();
    std::printf("\nResults written to %s\n", output_csv.c_str());
    return 0;
}
