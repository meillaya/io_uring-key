#pragma once
// YCSB A-F workload generator with read-latest, scan, and RMW paths.

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
    size_t scan_length = 10;
    bool sync_wal = false;
    uint64_t seed = 42;
};

struct YCSBResult {
    std::string backend_name;
    std::string workload;
    bool sync_wal = false;
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

class YCSBBenchmark {
public:
    explicit YCSBBenchmark(Engine& engine) : engine_(engine) {}

    YCSBResult Run(const YCSBConfig& config) {
        YCSBResult result;
        result.backend_name = std::string(engine_.backend()->Name());
        result.workload = WorkloadName(config.workload);
        result.sync_wal = config.sync_wal;

        std::mt19937_64 rng(config.seed);
        std::uniform_int_distribution<int> op_dist(0, 99);
        std::uniform_int_distribution<size_t> key_dist(0, config.num_keys - 1);
        std::uniform_int_distribution<int> val_dist('a', 'z');

        auto make_key = [&](size_t index) {
            return "ycsb_key_" + std::to_string(index);
        };
        auto make_value = [&]() {
            std::string value(config.value_size, '\0');
            for (auto& c : value) c = static_cast<char>(val_dist(rng));
            return value;
        };

        // Pre-populate keys.
        for (size_t i = 0; i < config.num_keys; i++) {
            engine_.Put(make_key(i), make_value());
        }
        std::string latest_key = make_key(config.num_keys - 1);

        auto start = std::chrono::high_resolution_clock::now();

        auto record_read = [&](std::chrono::high_resolution_clock::time_point op_start,
                               std::chrono::high_resolution_clock::time_point op_end) {
            uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.read_hist.Record(us);
            result.overall_hist.Record(us);
            result.reads++;
        };
        auto record_write = [&](std::chrono::high_resolution_clock::time_point op_start,
                                std::chrono::high_resolution_clock::time_point op_end) {
            uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                op_end - op_start).count();
            result.write_hist.Record(us);
            result.overall_hist.Record(us);
            result.writes++;
        };

        for (size_t i = 0; i < config.num_ops; i++) {
            std::string key = make_key(key_dist(rng));
            int op = op_dist(rng);

            switch (config.workload) {
                case YCSBWorkload::A: {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    if (op < 50) {
                        engine_.Get(key);
                        record_read(op_start, std::chrono::high_resolution_clock::now());
                    } else {
                        engine_.Put(key, make_value());
                        record_write(op_start, std::chrono::high_resolution_clock::now());
                    }
                    break;
                }
                case YCSBWorkload::B: {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    if (op < 95) {
                        engine_.Get(key);
                        record_read(op_start, std::chrono::high_resolution_clock::now());
                    } else {
                        engine_.Put(key, make_value());
                        record_write(op_start, std::chrono::high_resolution_clock::now());
                    }
                    break;
                }
                case YCSBWorkload::C: {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    engine_.Get(key);
                    record_read(op_start, std::chrono::high_resolution_clock::now());
                    break;
                }
                case YCSBWorkload::D: {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    if (op < 95) {
                        engine_.Get(latest_key);
                        record_read(op_start, std::chrono::high_resolution_clock::now());
                    } else {
                        engine_.Put(key, make_value());
                        record_write(op_start, std::chrono::high_resolution_clock::now());
                        latest_key = key;
                    }
                    break;
                }
                case YCSBWorkload::E: {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    if (op < 95) {
                        std::vector<std::pair<std::string, std::string>> rows;
                        engine_.Scan(key, config.scan_length, &rows);
                        record_read(op_start, std::chrono::high_resolution_clock::now());
                    } else {
                        engine_.Put(key, make_value());
                        record_write(op_start, std::chrono::high_resolution_clock::now());
                    }
                    break;
                }
                case YCSBWorkload::F: {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    if (op < 50) {
                        engine_.Get(key);
                        auto after_read = std::chrono::high_resolution_clock::now();
                        engine_.Put(key, make_value());
                        auto after_write = std::chrono::high_resolution_clock::now();
                        uint64_t read_us =
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                after_read - op_start).count();
                        uint64_t write_us =
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                after_write - after_read).count();
                        uint64_t total_us =
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                after_write - op_start).count();
                        result.read_hist.Record(read_us);
                        result.write_hist.Record(write_us);
                        result.overall_hist.Record(total_us);
                        result.reads++;
                        result.writes++;
                    } else {
                        engine_.Get(key);
                        record_read(op_start, std::chrono::high_resolution_clock::now());
                    }
                    break;
                }
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
           << (r.sync_wal ? "true" : "false") << ","
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
        os << "backend,workload,sync_wal,total_ops,reads,writes,elapsed_sec,"
           << "throughput_ops,"
           << "overall_p50_us,overall_p95_us,overall_p99_us,"
           << "read_p50_us,read_p95_us,read_p99_us,"
           << "write_p50_us,write_p95_us,write_p99_us\n";
    }

private:
    Engine& engine_;
};

} // namespace kv
