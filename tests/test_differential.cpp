#include <doctest/doctest.h>

#include <cstdlib>
#include <filesystem>
#include <map>
#include <random>
#include <string>

#include "engine/engine.h"
#include "io/sync_backend.h"

using namespace kv;

namespace {

std::string RandomString(std::mt19937& rng, size_t min_len, size_t max_len) {
    std::uniform_int_distribution<size_t> len_dist(min_len, max_len);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    size_t len = len_dist(rng);
    std::string s(len, '\0');
    for (auto& c : s) c = static_cast<char>(char_dist(rng));
    return s;
}

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

} // namespace

TEST_CASE("differential: randomized ops vs std::map") {
    auto data_dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = data_dir;
    opts.memtable_flush_threshold = 64 * 1024;  // Small threshold to force flushes.
    opts.sync_wal = true;

    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    std::map<std::string, std::string> oracle;
    std::mt19937 rng(42);  // Deterministic seed for reproducibility.

    constexpr int kNumOps = 5000;
    std::uniform_int_distribution<int> op_dist(0, 99);
    std::uniform_int_distribution<int> key_dist(0, 499);  // 500 possible keys.

    for (int i = 0; i < kNumOps; i++) {
        std::string key = "key_" + std::to_string(key_dist(rng));
        int op = op_dist(rng);

        if (op < 60) {
            // 60% puts
            std::string value = RandomString(rng, 10, 200);
            REQUIRE(engine.Put(key, value).ok());
            oracle[key] = value;
        } else if (op < 80) {
            // 20% deletes
            REQUIRE(engine.Delete(key).ok());
            oracle.erase(key);
        } else {
            // 20% gets
            auto result = engine.Get(key);
            auto expected = oracle.find(key);
            if (expected == oracle.end()) {
                CHECK(!result.has_value());
            } else {
                REQUIRE(result.has_value());
                CHECK(*result == expected->second);
            }
        }

        // Periodically verify all keys.
        if (i % 500 == 499) {
            for (auto& [k, v] : oracle) {
                auto result = engine.Get(k);
                REQUIRE_MESSAGE(result.has_value(), "key not found: ", k, " at op ", i);
                CHECK_MESSAGE(*result == v, "value mismatch for key: ", k, " at op ", i);
            }
            // Verify deleted keys are gone.
            for (int j = 0; j < 500; j++) {
                std::string k = "key_" + std::to_string(j);
                if (oracle.find(k) == oracle.end()) {
                    CHECK(!engine.Get(k).has_value());
                }
            }
        }
    }

    // Final full verification.
    for (auto& [k, v] : oracle) {
        auto result = engine.Get(k);
        REQUIRE(result.has_value());
        CHECK(*result == v);
    }

    engine.FlushAndSync();
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("differential: recovery after reopen") {
    auto data_dir = MakeTempDir();

    // Phase 1: write data.
    std::map<std::string, std::string> oracle;
    {
        auto backend = std::make_unique<SyncBackend>();
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.memtable_flush_threshold = 32 * 1024;
        opts.sync_wal = true;

        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());

        std::mt19937 rng(123);
        for (int i = 0; i < 1000; i++) {
            std::string key = "recover_key_" + std::to_string(i % 200);
            std::string value = RandomString(rng, 20, 100);
            REQUIRE(engine.Put(key, value).ok());
            oracle[key] = value;
        }
        // Delete some keys.
        for (int i = 0; i < 200; i += 3) {
            std::string key = "recover_key_" + std::to_string(i);
            REQUIRE(engine.Delete(key).ok());
            oracle.erase(key);
        }
        // Flush but don't necessarily sync everything.
        engine.FlushAndSync();
    }

    // Phase 2: reopen and verify.
    {
        auto backend = std::make_unique<SyncBackend>();
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.sync_wal = true;

        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());

        for (auto& [k, v] : oracle) {
            auto result = engine.Get(k);
            REQUIRE_MESSAGE(result.has_value(), "key not found after recovery: ", k);
            CHECK(*result == v);
        }
    }

    std::filesystem::remove_all(data_dir);
}
