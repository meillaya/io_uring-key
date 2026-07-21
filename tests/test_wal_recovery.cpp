#include <doctest/doctest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "io/sync_backend.h"
#include "wal/wal.h"

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_wal_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

} // namespace

TEST_CASE("WAL: basic append and replay") {
    auto data_dir = MakeTempDir();
    SyncBackend backend;

    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());

        REQUIRE(wal.Append({WALRecordType::kPut, "key1", "value1"}).ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "key2", "value2"}).ok());
        REQUIRE(wal.Append({WALRecordType::kDelete, "key1", ""}).ok());
        REQUIRE(wal.Sync().ok());
    }

    // Replay.
    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());

        std::vector<WALRecord> records;
        size_t count = 0;
        REQUIRE(wal.Replay([&](const WALRecord& rec) -> bool {
            records.push_back(rec);
            return true;
        }, &count).ok());

        CHECK(count == 3);
        REQUIRE(records.size() == 3);
        CHECK(records[0].type == WALRecordType::kPut);
        CHECK(records[0].key == "key1");
        CHECK(records[0].value == "value1");
        CHECK(records[1].type == WALRecordType::kPut);
        CHECK(records[1].key == "key2");
        CHECK(records[1].value == "value2");
        CHECK(records[2].type == WALRecordType::kDelete);
        CHECK(records[2].key == "key1");
    }

    std::filesystem::remove_all(data_dir);
}

TEST_CASE("WAL: torn write recovery (truncated last record)") {
    auto data_dir = MakeTempDir();
    SyncBackend backend;

    // Write 3 valid records.
    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "key1", "value1"}).ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "key2", "value2"}).ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "key3", "value3"}).ok());
        REQUIRE(wal.Sync().ok());
    }

    // Simulate torn write: truncate the file mid-record.
    {
        std::string wal_path = data_dir + "/wal.log";
        auto file_size = std::filesystem::file_size(wal_path);
        REQUIRE(file_size > 10);

        // Truncate last 5 bytes (simulating partial write of last record).
        std::filesystem::resize_file(wal_path, file_size - 5);
    }

    // Replay should recover the first 2 valid records.
    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());

        std::vector<WALRecord> records;
        size_t count = 0;
        REQUIRE(wal.Replay([&](const WALRecord& rec) -> bool {
            records.push_back(rec);
            return true;
        }, &count).ok());

        // Should recover 2 records (3rd was torn).
        CHECK(count == 2);
        REQUIRE(records.size() == 2);
        CHECK(records[0].key == "key1");
        CHECK(records[1].key == "key2");
    }

    std::filesystem::remove_all(data_dir);
}

TEST_CASE("WAL: CRC corruption detection") {
    auto data_dir = MakeTempDir();
    SyncBackend backend;

    // Write 3 valid records.
    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "key1", "value1"}).ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "key2", "value2"}).ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "key3", "value3"}).ok());
        REQUIRE(wal.Sync().ok());
    }

    // Corrupt a byte in the middle of the file (in the 2nd record's data).
    {
        std::string wal_path = data_dir + "/wal.log";
        std::fstream f(wal_path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());

        // Find the offset of the 2nd record.
        // First record: 4 (len) + 1 (type) + 4 (klen) + 4 (key1) + 4 (vlen) + 6 (value1) + 4 (crc) = 27
        // Corrupt a byte in the 2nd record's value area.
        auto file_size = std::filesystem::file_size(wal_path);
        // Corrupt byte at offset ~35 (in the 2nd record).
        f.seekp(35);
        char corrupt = '\xFF';
        f.write(&corrupt, 1);
        f.close();
    }

    // Replay should stop at the corrupted record.
    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());

        std::vector<WALRecord> records;
        size_t count = 0;
        REQUIRE(wal.Replay([&](const WALRecord& rec) -> bool {
            records.push_back(rec);
            return true;
        }, &count).ok());

        // Should recover only the 1st record (2nd is corrupted).
        CHECK(count == 1);
        REQUIRE(records.size() == 1);
        CHECK(records[0].key == "key1");
    }

    std::filesystem::remove_all(data_dir);
}

TEST_CASE("WAL: group commit (batch append)") {
    auto data_dir = MakeTempDir();
    SyncBackend backend;

    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());

        std::vector<WALRecord> batch;
        for (int i = 0; i < 100; i++) {
            batch.push_back({WALRecordType::kPut,
                             "batch_key_" + std::to_string(i),
                             "batch_val_" + std::to_string(i)});
        }
        REQUIRE(wal.AppendBatch(batch).ok());
        REQUIRE(wal.Sync().ok());
    }

    // Replay all.
    {
        WAL wal(&backend, data_dir);
        REQUIRE(wal.Open().ok());

        size_t count = 0;
        REQUIRE(wal.Replay([&](const WALRecord&) -> bool {
            count++;
            return true;
        }).ok());

        CHECK(count == 100);
    }

    std::filesystem::remove_all(data_dir);
}

TEST_CASE("WAL: truncate and reuse") {
    auto data_dir = MakeTempDir();
    SyncBackend backend;

    WAL wal(&backend, data_dir);
    REQUIRE(wal.Open().ok());

    REQUIRE(wal.Append({WALRecordType::kPut, "key1", "value1"}).ok());
    REQUIRE(wal.Sync().ok());
    REQUIRE(wal.Truncate().ok());

    // After truncate, replay should find nothing.
    size_t count = 0;
    REQUIRE(wal.Replay([&](const WALRecord&) -> bool {
        count++;
        return true;
    }, &count).ok());
    CHECK(count == 0);

    // Can still append after truncate.
    REQUIRE(wal.Append({WALRecordType::kPut, "key2", "value2"}).ok());
    REQUIRE(wal.Sync().ok());

    count = 0;
    REQUIRE(wal.Replay([&](const WALRecord&) -> bool {
        count++;
        return true;
    }, &count).ok());
    CHECK(count == 1);

    std::filesystem::remove_all(data_dir);
}
