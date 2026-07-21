#include <doctest/doctest.h>

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "engine/engine.h"
#include "io/sync_backend.h"

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_crash_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

} // namespace

TEST_CASE("crash recovery: SIGKILL during writes") {
    auto data_dir = MakeTempDir();

    // Phase 1: Fork a child that writes data and gets SIGKILL'd.
    // We write a known set of keys, then kill the child mid-stream.
    constexpr int kNumKeys = 200;
    constexpr int kKillAfter = 100;  // Kill after this many writes.

    pid_t pid = fork();
    REQUIRE(pid >= 0);

    if (pid == 0) {
        // Child process: write keys, then get killed.
        auto backend = std::make_unique<SyncBackend>();
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.memtable_flush_threshold = 1024 * 1024;  // Large threshold, stay in WAL.
        opts.sync_wal = true;  // Fsync each write for durability.

        Engine engine(std::move(backend), opts);
        auto s = engine.Open();
        if (!s.ok()) _exit(1);

        for (int i = 0; i < kNumKeys; i++) {
            std::string key = "crash_key_" + std::to_string(i);
            std::string value = "crash_value_" + std::to_string(i);
            s = engine.Put(key, value);
            if (!s.ok()) _exit(1);

            if (i == kKillAfter) {
                // Signal parent we've written enough, then suicide.
                // Use _exit to simulate crash (no cleanup).
                raise(SIGKILL);
                _exit(0);  // Should not reach here.
            }
        }
        _exit(0);
    }

    // Parent: wait for child to die.
    int status;
    waitpid(pid, &status, 0);
    // Child should have been killed by SIGKILL.
    CHECK(WIFSIGNALED(status));
    if (WIFSIGNALED(status)) {
        CHECK(WTERMSIG(status) == SIGKILL);
    }

    // Phase 2: Recover and verify.
    {
        auto backend = std::make_unique<SyncBackend>();
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.sync_wal = true;

        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());

        // Keys 0..kKillAfter should be recoverable (they were fsync'd).
        // Keys after kKillAfter may or may not be present (depends on timing).
        for (int i = 0; i < kKillAfter; i++) {
            std::string key = "crash_key_" + std::to_string(i);
            std::string expected = "crash_value_" + std::to_string(i);
            auto result = engine.Get(key);
            REQUIRE_MESSAGE(result.has_value(),
                            "key ", key, " should be recoverable after SIGKILL");
            CHECK(*result == expected);
        }
    }

    std::filesystem::remove_all(data_dir);
}

TEST_CASE("crash recovery: SIGKILL after flush (SSTable recovery)") {
    auto data_dir = MakeTempDir();

    // Phase 1: Write enough data to trigger a flush, then SIGKILL.
    pid_t pid = fork();
    REQUIRE(pid >= 0);

    if (pid == 0) {
        auto backend = std::make_unique<SyncBackend>();
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.memtable_flush_threshold = 4096;  // Very small to force flushes.
        opts.sync_wal = true;

        Engine engine(std::move(backend), opts);
        auto s = engine.Open();
        if (!s.ok()) _exit(1);

        // Write enough to trigger multiple flushes.
        for (int i = 0; i < 500; i++) {
            std::string key = "flush_key_" + std::to_string(i);
            std::string value = "flush_value_" + std::to_string(i) +
                                "_padding_to_make_it_longer_for_flush";
            s = engine.Put(key, value);
            if (!s.ok()) _exit(1);
        }

        // Kill after writes (some data in SSTables, some in WAL).
        raise(SIGKILL);
        _exit(0);
    }

    int status;
    waitpid(pid, &status, 0);
    CHECK(WIFSIGNALED(status));

    // Phase 2: Recover.
    {
        auto backend = std::make_unique<SyncBackend>();
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.sync_wal = true;

        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());

        // All 500 keys should be recoverable (WAL + SSTables).
        int recovered = 0;
        for (int i = 0; i < 500; i++) {
            std::string key = "flush_key_" + std::to_string(i);
            auto result = engine.Get(key);
            if (result.has_value()) {
                std::string expected = "flush_value_" + std::to_string(i) +
                                       "_padding_to_make_it_longer_for_flush";
                CHECK(*result == expected);
                recovered++;
            }
        }
        // With sync_wal=true, all 500 should be recovered.
        CHECK(recovered == 500);
    }

    std::filesystem::remove_all(data_dir);
}
