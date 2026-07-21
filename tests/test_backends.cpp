#include <doctest/doctest.h>

#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <string>

#include "engine/engine.h"
#include "io/mmap_backend.h"
#include "io/sync_backend.h"
#include "io/threadpool_backend.h"

#ifdef KV_HAS_IOURING
#include "io/iouring_backend.h"
#endif

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_backend_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

std::string RandomString(std::mt19937& rng, size_t min_len, size_t max_len) {
    std::uniform_int_distribution<size_t> len_dist(min_len, max_len);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    size_t len = len_dist(rng);
    std::string s(len, '\0');
    for (auto& c : s) c = static_cast<char>(char_dist(rng));
    return s;
}

void RunDifferentialTest(std::unique_ptr<IOBackend> backend, const std::string& name) {
    auto data_dir = MakeTempDir();
    EngineOptions opts;
    opts.data_dir = data_dir;
    opts.memtable_flush_threshold = 64 * 1024;
    opts.sync_wal = true;

    Engine engine(std::move(backend), opts);
    REQUIRE_MESSAGE(engine.Open().ok(), "Failed to open engine with ", name, " backend");

    std::map<std::string, std::string> oracle;
    std::mt19937 rng(42);

    constexpr int kNumOps = 2000;
    std::uniform_int_distribution<int> op_dist(0, 99);
    std::uniform_int_distribution<int> key_dist(0, 199);

    for (int i = 0; i < kNumOps; i++) {
        std::string key = "key_" + std::to_string(key_dist(rng));
        int op = op_dist(rng);

        if (op < 60) {
            std::string value = RandomString(rng, 10, 100);
            REQUIRE_MESSAGE(engine.Put(key, value).ok(), name, " Put failed at op ", i);
            oracle[key] = value;
        } else if (op < 80) {
            REQUIRE_MESSAGE(engine.Delete(key).ok(), name, " Delete failed at op ", i);
            oracle.erase(key);
        } else {
            auto result = engine.Get(key);
            auto expected = oracle.find(key);
            if (expected == oracle.end()) {
                CHECK_MESSAGE(!result.has_value(), name, " unexpected value for ", key);
            } else {
                REQUIRE_MESSAGE(result.has_value(), name, " missing key ", key);
                CHECK_MESSAGE(*result == expected->second, name, " value mismatch for ", key);
            }
        }
    }

    // Final verification.
    for (auto& [k, v] : oracle) {
        auto result = engine.Get(k);
        REQUIRE_MESSAGE(result.has_value(), name, " final check: missing key ", k);
        CHECK(*result == v);
    }

    engine.FlushAndSync();
    std::filesystem::remove_all(data_dir);
}

} // namespace

TEST_CASE("backend differential: sync") {
    RunDifferentialTest(std::make_unique<SyncBackend>(), "sync");
}

TEST_CASE("backend differential: threadpool") {
    RunDifferentialTest(std::make_unique<ThreadPoolBackend>(4), "threadpool");
}

TEST_CASE("backend differential: mmap") {
    RunDifferentialTest(std::make_unique<MmapBackend>(), "mmap");
}

#ifdef KV_HAS_IOURING
TEST_CASE("backend differential: io_uring") {
    auto backend = std::make_unique<IOUringBackend>();
    REQUIRE(backend->Init().ok());
    RunDifferentialTest(std::move(backend), "io_uring");
}
#endif
