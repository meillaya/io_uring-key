#include <doctest/doctest.h>

#ifdef KV_HAS_IOURING

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <random>
#include <string>

#include "engine/engine.h"
#include "io/iouring_backend.h"

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_iouring_test_XXXXXX";
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

} // namespace

TEST_CASE("io_uring: backend init and capabilities") {
    auto backend = std::make_unique<IOUringBackend>();
    REQUIRE(backend->Init().ok());

    CHECK(backend->Name() == "io_uring");
    CHECK(backend->SupportsDirectIO());
    CHECK(backend->SupportsBatchedIO());
    // Registered buffers/files depend on kernel support.
    KV_LOG_INFO("io_uring: registered_buffers=%d, fixed_files=%d",
                backend->SupportsRegisteredBuffers(), backend->SupportsFixedFiles());
}

TEST_CASE("io_uring: differential randomized ops vs std::map") {
    auto data_dir = MakeTempDir();
    auto backend = std::make_unique<IOUringBackend>();
    REQUIRE(backend->Init().ok());

    EngineOptions opts;
    opts.data_dir = data_dir;
    opts.memtable_flush_threshold = 64 * 1024;
    opts.sync_wal = true;

    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    std::map<std::string, std::string> oracle;
    std::mt19937 rng(42);

    constexpr int kNumOps = 3000;
    std::uniform_int_distribution<int> op_dist(0, 99);
    std::uniform_int_distribution<int> key_dist(0, 299);

    for (int i = 0; i < kNumOps; i++) {
        std::string key = "iouring_key_" + std::to_string(key_dist(rng));
        int op = op_dist(rng);

        if (op < 60) {
            std::string value = RandomString(rng, 10, 200);
            REQUIRE(engine.Put(key, value).ok());
            oracle[key] = value;
        } else if (op < 80) {
            REQUIRE(engine.Delete(key).ok());
            oracle.erase(key);
        } else {
            auto result = engine.Get(key);
            auto expected = oracle.find(key);
            if (expected == oracle.end()) {
                CHECK(!result.has_value());
            } else {
                REQUIRE(result.has_value());
                CHECK(*result == expected->second);
            }
        }
    }

    // Final verification.
    for (auto& [k, v] : oracle) {
        auto result = engine.Get(k);
        REQUIRE(result.has_value());
        CHECK(*result == v);
    }

    engine.FlushAndSync();
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("io_uring: recovery after reopen") {
    auto data_dir = MakeTempDir();

    // Phase 1: write data.
    std::map<std::string, std::string> oracle;
    {
        auto backend = std::make_unique<IOUringBackend>();
        REQUIRE(backend->Init().ok());
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.memtable_flush_threshold = 32 * 1024;
        opts.sync_wal = true;

        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());

        std::mt19937 rng(99);
        for (int i = 0; i < 500; i++) {
            std::string key = "iouring_recover_" + std::to_string(i % 100);
            std::string value = RandomString(rng, 20, 100);
            REQUIRE(engine.Put(key, value).ok());
            oracle[key] = value;
        }
        engine.FlushAndSync();
    }

    // Phase 2: reopen and verify.
    {
        auto backend = std::make_unique<IOUringBackend>();
        REQUIRE(backend->Init().ok());
        EngineOptions opts;
        opts.data_dir = data_dir;
        opts.sync_wal = true;

        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());

        for (auto& [k, v] : oracle) {
            auto result = engine.Get(k);
            REQUIRE_MESSAGE(result.has_value(), "key not found: ", k);
            CHECK(*result == v);
        }
    }

    std::filesystem::remove_all(data_dir);
}

TEST_CASE("io_uring: batched I/O submit") {
    auto data_dir = MakeTempDir();
    auto backend = std::make_unique<IOUringBackend>();
    REQUIRE(backend->Init().ok());

    // Create a test file.
    std::string path = data_dir + "/batch_test.dat";
    FileHandle fh;
    REQUIRE(backend->Open(path, true, false, &fh).ok());

    // Write some data.
    const char* data = "Hello, io_uring batched I/O!";
    size_t len = std::strlen(data);
    REQUIRE(backend->Write(fh, 0, data, len).ok());
    REQUIRE(backend->Sync(fh, false).ok());

    // Read back using batch.
    char buf[64] = {};
    IORequest reqs[1] = {{IOOp::kRead, fh, 0, buf, len}};
    IOResult results[1];
    REQUIRE(backend->SubmitBatch(reqs, results).ok());
    CHECK(results[0].status.ok());
    CHECK(std::string(buf, len) == data);

    backend->Close(fh);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("io_uring: alignment edge cases") {
    auto data_dir = MakeTempDir();

    // Test with O_DIRECT (requires alignment).
    IOUringOptions opts;
    opts.use_direct_io = true;
    auto backend = std::make_unique<IOUringBackend>(opts);
    REQUIRE(backend->Init().ok());

    std::string path = data_dir + "/aligned_test.dat";
    FileHandle fh;
    auto s = backend->Open(path, true, true, &fh);
    if (!s.ok()) {
        // O_DIRECT might not be supported on this filesystem (e.g., tmpfs).
        KV_LOG_WARN("O_DIRECT not supported: %s (skipping alignment tests)",
                    s.ToString().c_str());
        std::filesystem::remove_all(data_dir);
        return;
    }

    // Aligned write (4096 bytes, aligned buffer).
    AlignedBuffer wbuf(4096);
    std::memset(wbuf.data(), 0xAB, 4096);
    s = backend->Write(fh, 0, wbuf.data(), 4096);
    CHECK(s.ok());

    // Aligned read.
    AlignedBuffer rbuf(4096);
    s = backend->Read(fh, 0, rbuf.data(), 4096);
    CHECK(s.ok());
    CHECK(std::memcmp(wbuf.data(), rbuf.data(), 4096) == 0);

    // Multi-block aligned write.
    AlignedBuffer wbuf2(8192);
    std::memset(wbuf2.data(), 0xCD, 8192);
    s = backend->Write(fh, 4096, wbuf2.data(), 8192);
    CHECK(s.ok());

    // Read back both blocks.
    AlignedBuffer rbuf2(8192);
    s = backend->Read(fh, 4096, rbuf2.data(), 8192);
    CHECK(s.ok());
    CHECK(std::memcmp(wbuf2.data(), rbuf2.data(), 8192) == 0);

    backend->Sync(fh, false);
    backend->Close(fh);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("io_uring: buffered (non-O_DIRECT) mode") {
    auto data_dir = MakeTempDir();

    IOUringOptions opts;
    opts.use_direct_io = false;  // Explicitly buffered.
    auto backend = std::make_unique<IOUringBackend>(opts);
    REQUIRE(backend->Init().ok());

    std::string path = data_dir + "/buffered_test.dat";
    FileHandle fh;
    REQUIRE(backend->Open(path, true, false, &fh).ok());

    // Unaligned write (should work in buffered mode).
    const char* data = "short";
    REQUIRE(backend->Write(fh, 0, data, 5).ok());
    REQUIRE(backend->Sync(fh, false).ok());

    // Unaligned read.
    char buf[16] = {};
    REQUIRE(backend->Read(fh, 0, buf, 5).ok());
    CHECK(std::string(buf, 5) == "short");

    backend->Close(fh);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("io_uring: registered-buffer read/write fast path") {
    auto data_dir = MakeTempDir();
    auto backend = std::make_unique<IOUringBackend>();
    REQUIRE(backend->Init().ok());

    KV_LOG_INFO("io_uring: sqpoll=%d registered_buffers=%d",
                backend->sqpoll_enabled(), backend->SupportsRegisteredBuffers());

    if (!backend->SupportsRegisteredBuffers() || backend->buffer_pool() == nullptr) {
        KV_LOG_WARN("registered buffers unavailable; skipping fast-path test");
        std::filesystem::remove_all(data_dir);
        return;
    }

    auto* pool = backend->buffer_pool();
    REQUIRE(pool->count() >= 2);
    auto& wbuf = pool->at(0);
    auto& rbuf = pool->at(1);
    const size_t n = 4096;
    REQUIRE(wbuf.capacity() >= n);
    REQUIRE(rbuf.capacity() >= n);

    // Fill the write buffer (a registered pool buffer) with a pattern.
    for (size_t i = 0; i < n; i++) wbuf.data()[i] = static_cast<uint8_t>(i * 7 + 1);

    std::string path = data_dir + "/regbuf.dat";
    FileHandle fh;
    REQUIRE(backend->Open(path, true, false, &fh).ok());

    // Write from a registered buffer -> prep_write_fixed path.
    REQUIRE(backend->Write(fh, 0, wbuf.data(), n).ok());
    REQUIRE(backend->Sync(fh, false).ok());

    // Read into a different registered buffer -> prep_read_fixed path.
    std::memset(rbuf.data(), 0, n);
    REQUIRE(backend->Read(fh, 0, rbuf.data(), n).ok());

    CHECK(std::memcmp(wbuf.data(), rbuf.data(), n) == 0);

    backend->Close(fh);
    std::filesystem::remove_all(data_dir);
}

#endif // KV_HAS_IOURING
