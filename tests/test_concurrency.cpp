#include <doctest/doctest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "engine/engine.h"
#include "io/sync_backend.h"

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_conc_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

} // namespace

// G008: Engine::Get must not race with the memtable pointer swap performed by
// flush. This soak runs concurrent readers against a writer + an explicit
// flusher so the memtable_ shared_ptr is swapped repeatedly under load.
// Invariant checked: any value returned for key "kN" must equal "kN".
TEST_CASE("concurrency: readers vs flush memtable swap (no race / no crash)") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.memtable_flush_threshold = 2048;  // flush frequently
    opts.sync_wal = false;                 // keep the soak fast
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    constexpr int kNumKeys = 200;
    std::atomic<bool> stop{false};
    std::atomic<long> reads{0};
    std::atomic<bool> invariant_violated{false};

    auto reader = [&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            for (int i = 0; i < kNumKeys; i++) {
                std::string key = "k" + std::to_string(i);
                std::optional<std::string> out;
                auto st = engine.Get(key, &out);
                if (!st.ok()) continue;  // tolerate nothing else; corruption unexpected
                if (out.has_value() && *out != key) {
                    invariant_violated.store(true);
                }
                reads.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> readers;
    for (int t = 0; t < 4; t++) readers.emplace_back(reader);

    // Writer: overwrite keys with value == key.
    std::thread writer([&]() {
        int round = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            for (int i = 0; i < kNumKeys; i++) {
                std::string key = "k" + std::to_string(i);
                engine.Put(key, key);
            }
            round++;
            if (round > 2000) break;
        }
    });

    // Flusher: force memtable swaps concurrently with readers.
    std::thread flusher([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            engine.FlushAndSync();
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    // Let it soak briefly.
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    stop.store(true);

    writer.join();
    flusher.join();
    for (auto& t : readers) t.join();

    CHECK(!invariant_violated.load());
    CHECK(reads.load() > 0);

    // Final consistency: every key present with value == key.
    for (int i = 0; i < kNumKeys; i++) {
        std::string key = "k" + std::to_string(i);
        auto v = engine.Get(key);
        REQUIRE(v.has_value());
        CHECK(*v == key);
    }

    std::filesystem::remove_all(dir);
}
