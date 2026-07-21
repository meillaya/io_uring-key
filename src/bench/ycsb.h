#pragma once
// YCSB A-F workload generator with latency histogram capture and CSV export.

#include <chrono>
#include <cstdint>
#include <fstream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "engine/engine.h"
#include "util/histogram.h"

namespace kv {

// YCSB workload types.
enum class YCSBWorkload { A, B, C, D, E, F };

inline const char* WorkloadName(YCSBWorkload w) {
    switch (w) {
        case YCSBWorkload::A: return "A";
        case YCSBWorkload::B: return "B";
        case YCSBWorkload::C: return "C";
        case YCSBWorkload::D: return "D";
        case YCSBWorkload::E: return "E";
        case YCSBWorkload::F: return "F";
    }
    return "?";
}

struct YCSBConfig {
    YCSBWorkload workload = YCSBWorkload::A;
    size_t num_keys = 10000;
    size_t num_ops = 100000;
    size_t value_size = 100;
    uint64_t seed = 42;
};

struct YCSBResult {
    std::string backend_name;
    std::string workload;
    LatencyHistogram read_hist;
    LatencyHistogram write_hist;
    LatencyHistogram overall_hist;
    size_t total_ops = 0;
    size_t reads = 0;
    size_t writes = 0;
    double elapsed_sec = 0;

    [[nodiscard]] double throughput() const {
        return elapsed_sec > 0 ? total_ops / elapsed_sec : 0;
    }
};

// YCSB workload mix (read% / write%):
// A: 50/50 (read-heavy with updates)
// B: 95/5 (read-mostly)
// C: 100/0 (read-only)
// D: 95/5 (read-latest)
// E: 95/5 (short ranges / scan)
// F: 50/50 (read-modify-write)
inline std::pair<int, int> WorkloadMix(YCSBWorkload w) {
    switch (w) {
        case YCSBWorkload::A: return {50, 50};
        case YCSBWorkload::B: return {95, 5};
        case YCSBWorkload::C: return {100, 0};
        case YCSBWorkload::D: return {95, 5};
        case YCSBWorkload::E: return {95, 5};
        case YCSBWorkload::F: return {50, 50};
    }
    return {50, 50};
}

class YCSBBenchmark {
public:
    explicit YCSBBenchmark(Engine& engine) : engine_(engine) {}

    YCSBResult Run(const YCSBConfig& config) {
        YCSBResult result;
        result.backend_name = std::string(engine_.backend()->Name());
        result.workload = WorkloadName(config.workload);

        auto [read_pct, write_pct] = WorkloadMix(config.workload);

        std::mt19937_64 rng(config.seed);
        std::uniform_int_distribution<int> op_dist(0, 99);
        std::uniform_int_distribution<size_t> key_dist(0, config.num_keys - 1);
        std::uniform_int_distribution<int> val_dist('a', 'z');

        // Pre-populate keys.
        for (size_t i = 0; i < config.num_keys; i++) {
            std::string key = "ycsb_key_" + std::to_string(i);
            std::string value(config.value_size, '\0');
            for (auto& c : value) c = static_cast<char>(val_dist(rng));
            engine_.Put(key, value);
        }

        auto start = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < config.num_ops; i++) {
            std::string key = "ycsb_key_" + std::to_string(key_dist(rng));
            int op = op_dist(rng);

            auto op_start = std::chrono::high_resolution_clock::now();

            if (op < read_pct) {
                // Read.
                auto val = engine_.Get(key);
                result.reads++;
                auto op_end = std::chrono::high_resolution_clock::now();
                uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_end - op_start).count();
                result.read_hist.Record(us);
                result.overall_hist.Record(us);
            } else {
                // Write (update or insert).
                std::string value(config.value_size, '\0');
                for (auto& c : value) c = static_cast<char>(val_dist(rng));
                engine_.Put(key, value);
                result.writes++;
                auto op_end = std::chrono::high_resolution_clock::now();
                uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_end - op_start).count();
                result.write_hist.Record(us);
                result.overall_hist.Record(us);
            }
            result.total_ops++;
        }

        auto end = std::chrono::high_resolution_clock::now();
        result.elapsed_sec = std::chrono::duration<double>(end - start).count();

        return result;
    }

    // Export result as CSV line.
    static void ExportCSV(const YCSBResult& r, std::ostream& os) {
        os << r.backend_name << ","
           << r.workload << ","
           << r.total_ops << ","
           << r.reads << ","
           << r.writes << ","
           << r.elapsed_sec << ","
           << r.throughput() << ","
           << r.overall_hist.P50() << ","
           << r.overall_hist.P95() << ","
           << r.overall_hist.P99() << ","
           << r.read_hist.P50() << ","
           << r.read_hist.P95() << ","
           << r.read_hist.P99() << ","
           << r.write_hist.P50() << ","
           << r.write_hist.P95() << ","
           << r.write_hist.P99() << "\n";
    }

    static void WriteCSVHeader(std::ostream& os) {
        os << "backend,workload,total_ops,reads,writes,elapsed_sec,throughput_ops,"
           << "overall_p50_us,overall_p95_us,overall_p99_us,"
           << "read_p50_us,read_p95_us,read_p99_us,"
           << "write_p50_us,write_p95_us,write_p99_us\n";
    }

private:
    Engine& engine_;
};

} // namespace kv
