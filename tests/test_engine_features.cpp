#include <doctest/doctest.h>

#include <fcntl.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "engine/engine.h"
#include "io/sync_backend.h"
#include "server/command_handler.h"

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_feat_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

std::unique_ptr<Engine> OpenEngine(const std::string& dir, size_t flush_threshold) {
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.memtable_flush_threshold = flush_threshold;
    opts.sync_wal = true;
    auto engine = std::make_unique<Engine>(std::move(backend), opts);
    REQUIRE(engine->Open().ok());
    return engine;
}

} // namespace

TEST_CASE("FLUSHDB: engine Clear() removes all data") {
    auto dir = MakeTempDir();
    auto engine = OpenEngine(dir, 4 * 1024 * 1024);

    for (int i = 0; i < 10; i++) {
        REQUIRE(engine->Put("key" + std::to_string(i), "val" + std::to_string(i)).ok());
    }
    // Force some keys into SSTables too.
    REQUIRE(engine->FlushAndSync().ok());
    for (int i = 10; i < 20; i++) {
        REQUIRE(engine->Put("key" + std::to_string(i), "val" + std::to_string(i)).ok());
    }

    CHECK(engine->DbSize() == 20);

    REQUIRE(engine->Clear().ok());

    CHECK(engine->DbSize() == 0);
    for (int i = 0; i < 20; i++) {
        CHECK(!engine->Get("key" + std::to_string(i)).has_value());
    }
    // No SSTable files should remain.
    CHECK(engine->sstable_count() == 0);

    std::filesystem::remove_all(dir);
}

TEST_CASE("FLUSHDB: recovery after Clear does not resurrect keys") {
    auto dir = MakeTempDir();
    {
        auto engine = OpenEngine(dir, 4 * 1024 * 1024);
        REQUIRE(engine->Put("a", "1").ok());
        REQUIRE(engine->Put("b", "2").ok());
        REQUIRE(engine->Clear().ok());
        REQUIRE(engine->FlushAndSync().ok());
    }
    // Reopen: WAL was truncated and SSTables removed, so nothing comes back.
    auto engine2 = OpenEngine(dir, 4 * 1024 * 1024);
    CHECK(engine2->DbSize() == 0);
    CHECK(!engine2->Get("a").has_value());
    CHECK(!engine2->Get("b").has_value());
    std::filesystem::remove_all(dir);
}

TEST_CASE("FLUSHDB/DBSIZE via command handler") {
    auto dir = MakeTempDir();
    auto engine = OpenEngine(dir, 4 * 1024 * 1024);
    CommandHandler handler(*engine);

    auto set = handler.Execute({"SET", "foo", "bar"});
    CHECK(set.str == "OK");
    handler.Execute({"SET", "baz", "qux"});

    auto dbsize = handler.Execute({"DBSIZE"});
    CHECK(dbsize.type == RespType::kInteger);
    CHECK(dbsize.integer == 2);

    auto flush = handler.Execute({"FLUSHDB"});
    CHECK(flush.type == RespType::kSimpleString);
    CHECK(flush.str == "OK");

    auto dbsize2 = handler.Execute({"DBSIZE"});
    CHECK(dbsize2.integer == 0);

    auto get = handler.Execute({"GET", "foo"});
    CHECK(get.is_null);

    std::filesystem::remove_all(dir);
}

TEST_CASE("compaction: wired into flush path, reduces SSTable count, keys survive") {
    auto dir = MakeTempDir();
    // Tiny threshold => every Put flushes its own L0 SSTable.
    auto engine = OpenEngine(dir, 1);

    constexpr int kKeys = 12;
    for (int i = 0; i < kKeys; i++) {
        REQUIRE(engine->Put("k" + std::to_string(i), "v" + std::to_string(i)).ok());
    }
    // Flush anything residual.
    REQUIRE(engine->FlushAndSync().ok());

    size_t count = engine->sstable_count();
    // Compaction must have merged runs: far fewer SSTables than keys written.
    CHECK(count >= 1);
    CHECK(count < static_cast<size_t>(kKeys));

    // Every key must still be readable after compaction.
    for (int i = 0; i < kKeys; i++) {
        auto v = engine->Get("k" + std::to_string(i));
        REQUIRE(v.has_value());
        CHECK(*v == "v" + std::to_string(i));
    }

    std::filesystem::remove_all(dir);
}

TEST_CASE("compaction: latest version wins across merged SSTables") {
    auto dir = MakeTempDir();
    auto engine = OpenEngine(dir, 1);  // flush every put

    // Overwrite the same key many times across separate SSTables.
    for (int i = 0; i < 8; i++) {
        REQUIRE(engine->Put("hot", "version" + std::to_string(i)).ok());
    }
    REQUIRE(engine->FlushAndSync().ok());

    auto v = engine->Get("hot");
    REQUIRE(v.has_value());
    CHECK(*v == "version7");  // newest write survives compaction

    std::filesystem::remove_all(dir);
}

TEST_CASE("sstable: corruption is surfaced as Corruption, not NotFound") {
    auto dir = MakeTempDir();
    auto engine = OpenEngine(dir, 4 * 1024 * 1024);

    REQUIRE(engine->Put("ckey", "cvalue").ok());
    REQUIRE(engine->FlushAndSync().ok());  // push into an SSTable
    REQUIRE(engine->sstable_count() == 1);

    // Sanity: readable before corruption.
    {
        std::optional<std::string> out;
        auto st = engine->Get("ckey", &out);
        REQUIRE(st.ok());
        REQUIRE(out.has_value());
        CHECK(*out == "cvalue");
    }

    // Locate the SSTable file and flip a byte inside the first data block
    // (offset 5 is within the entry payload, before the trailing block CRC).
    std::string sst_path;
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        auto name = e.path().filename().string();
        if (name.starts_with("sst_") && name.ends_with(".sst")) {
            sst_path = e.path().string();
            break;
        }
    }
    REQUIRE(!sst_path.empty());

    int fd = ::open(sst_path.c_str(), O_RDWR);
    REQUIRE(fd >= 0);
    unsigned char byte = 0;
    REQUIRE(::pread(fd, &byte, 1, 5) == 1);
    byte ^= 0xFF;
    REQUIRE(::pwrite(fd, &byte, 1, 5) == 1);
    ::close(fd);

    // The authoritative read path must report Corruption (not nullopt/NotFound).
    std::optional<std::string> out;
    auto st = engine->Get("ckey", &out);
    CHECK(st.IsCorruption());

    std::filesystem::remove_all(dir);
}
