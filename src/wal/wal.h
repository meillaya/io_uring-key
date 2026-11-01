#pragma once
// Write-Ahead Log: append-only, per-record CRC, group commit, replay/recovery,
// torn-write handling.

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "io/io_backend.h"
#include "util/crc32.h"
#include "util/logging.h"
#include "util/status.h"

namespace kv {

// WAL record format (on disk):
//   [4 bytes: record_len (uint32 LE, excludes this field and CRC)]
//   [1 byte:  record_type]
//   [4 bytes: key_len (uint32 LE)]
//   [key_len bytes: key]
//   [4 bytes: val_len (uint32 LE)]
//   [val_len bytes: value]
//   [4 bytes: CRC32C of everything above]
//
// Total on-disk size per record: 4 + 1 + 4 + key_len + 4 + val_len + 4

enum class WALRecordType : uint8_t {
    kPut = 1,
    kDelete = 2,
};

struct WALRecord {
    WALRecordType type;
    std::string key;
    std::string value;  // Empty for deletes.
};

// Compute the on-disk size of a WAL record.
inline size_t WALRecordSize(const WALRecord& rec) {
    return 4 + 1 + 4 + rec.key.size() + 4 + rec.value.size() + 4;
}

// Serialize a WAL record into a buffer. Returns bytes written.
inline size_t SerializeWALRecord(const WALRecord& rec, uint8_t* buf) {
    uint8_t* p = buf;
    uint32_t record_len = 1 + 4 + static_cast<uint32_t>(rec.key.size()) +
                          4 + static_cast<uint32_t>(rec.value.size());
    std::memcpy(p, &record_len, 4); p += 4;
    *p = static_cast<uint8_t>(rec.type); p += 1;
    uint32_t klen = static_cast<uint32_t>(rec.key.size());
    std::memcpy(p, &klen, 4); p += 4;
    std::memcpy(p, rec.key.data(), klen); p += klen;
    uint32_t vlen = static_cast<uint32_t>(rec.value.size());
    std::memcpy(p, &vlen, 4); p += 4;
    if (vlen > 0) { std::memcpy(p, rec.value.data(), vlen); p += vlen; }
    // CRC over everything before the CRC field.
    uint32_t crc = CRC32::Compute(buf, static_cast<size_t>(p - buf));
    std::memcpy(p, &crc, 4); p += 4;
    return static_cast<size_t>(p - buf);
}

// Write-Ahead Log.
class WAL {
public:
    WAL(IOBackend* backend, std::string dir)
        : backend_(backend), dir_(std::move(dir)) {}

    ~WAL() {
        if (fh_ != kInvalidFile) {
            backend_->Close(fh_);
        }
    }

    // Open or create the WAL file.
    Status Open() {
        std::string path = dir_ + "/wal.log";
        auto s = backend_->Open(path, true, false, &fh_);
        if (!s.ok()) return s;
        s = backend_->FileSize(fh_, &write_offset_);
        if (!s.ok()) return s;
        return Status::OK();
    }

    // Append a record to the WAL. Does NOT fsync (use Sync() for durability).
    Status Append(const WALRecord& rec) {
        size_t sz = WALRecordSize(rec);
        std::vector<uint8_t> buf(sz);
        SerializeWALRecord(rec, buf.data());

        uint64_t offset;
        auto s = backend_->Append(fh_, buf.data(), buf.size(), &offset);
        if (!s.ok()) return s;
        write_offset_ = offset + buf.size();
        pending_sync_ = true;
        return Status::OK();
    }

    // Append a batch of records (group commit).
    Status AppendBatch(const std::vector<WALRecord>& records) {
        // Serialize all records into one buffer for a single write.
        size_t total = 0;
        for (auto& r : records) total += WALRecordSize(r);

        std::vector<uint8_t> buf(total);
        size_t off = 0;
        for (auto& r : records) {
            off += SerializeWALRecord(r, buf.data() + off);
        }

        uint64_t offset;
        auto s = backend_->Append(fh_, buf.data(), buf.size(), &offset);
        if (!s.ok()) return s;
        write_offset_ = offset + buf.size();
        pending_sync_ = true;
        return Status::OK();
    }

    // Fsync the WAL to durable storage.
    Status Sync() {
        if (!pending_sync_) return Status::OK();
        auto s = backend_->Sync(fh_, true);
        if (s.ok()) pending_sync_ = false;
        return s;
    }

    // Replay the WAL, calling `cb` for each valid record.
    // Stops at the first corrupted/torn record (CRC mismatch or truncation).
    // Returns the number of valid records replayed.
    Status Replay(const std::function<bool(const WALRecord&)>& cb,
                  size_t* records_replayed = nullptr) {
        uint64_t file_size = 0;
        auto s = backend_->FileSize(fh_, &file_size);
        if (!s.ok()) return s;

        if (file_size == 0) {
            if (records_replayed) *records_replayed = 0;
            return Status::OK();
        }

        // Read the entire WAL file.
        std::vector<uint8_t> data(file_size);
        s = backend_->Read(fh_, 0, data.data(), file_size);
        if (!s.ok()) return s;

        size_t pos = 0;
        size_t count = 0;
        size_t valid_end = 0;

        while (pos + 4 <= file_size) {
            // Read record_len.
            uint32_t record_len;
            std::memcpy(&record_len, data.data() + pos, 4);

            // Sanity check: record_len must be reasonable.
            if (record_len == 0 || record_len > 256 * 1024 * 1024) {
                KV_LOG_WARN("WAL replay: invalid record_len %u at offset %zu, stopping",
                            record_len, pos);
                break;
            }

            size_t total_record_size = 4 + record_len + 4;  // len + payload + crc
            if (pos + total_record_size > file_size) {
                // Torn write: record extends past end of file.
                KV_LOG_WARN("WAL replay: torn write at offset %zu (need %zu, have %zu), stopping",
                            pos, total_record_size, file_size - pos);
                break;
            }

            // Verify CRC.
            uint32_t stored_crc;
            std::memcpy(&stored_crc, data.data() + pos + 4 + record_len, 4);
            uint32_t computed_crc = CRC32::Compute(data.data() + pos, 4 + record_len);

            if (stored_crc != computed_crc) {
                KV_LOG_WARN("WAL replay: CRC mismatch at offset %zu (stored=%08x computed=%08x), stopping",
                            pos, stored_crc, computed_crc);
                break;
            }

            // Parse the record.
            WALRecord rec;
            const uint8_t* payload = data.data() + pos + 4;
            rec.type = static_cast<WALRecordType>(payload[0]);

            uint32_t klen;
            std::memcpy(&klen, payload + 1, 4);
            if (1 + 4 + klen + 4 > record_len) {
                KV_LOG_WARN("WAL replay: corrupt key_len at offset %zu, stopping", pos);
                break;
            }
            rec.key.assign(reinterpret_cast<const char*>(payload + 5), klen);

            uint32_t vlen;
            std::memcpy(&vlen, payload + 5 + klen, 4);
            if (1 + 4 + klen + 4 + vlen != record_len) {
                KV_LOG_WARN("WAL replay: corrupt val_len at offset %zu, stopping", pos);
                break;
            }
            if (vlen > 0) {
                rec.value.assign(reinterpret_cast<const char*>(payload + 9 + klen), vlen);
            }

            if (!cb(rec)) break;
            count++;
            pos += total_record_size;
            valid_end = pos;
        }

        // A torn or corrupt tail stays in the file. Cut it before the engine
        // accepts new writes, otherwise the next replay stops at the old tail
        // and loses every record appended after it.
        if (valid_end < file_size) {
            KV_LOG_WARN("WAL replay: truncating %zu trailing bytes at offset %zu",
                        static_cast<size_t>(file_size - valid_end), valid_end);
            auto truncate_status = TruncateTo(valid_end);
            if (!truncate_status.ok()) return truncate_status;
        }

        if (records_replayed) *records_replayed = count;
        return Status::OK();
    }

    // Truncate the WAL to the last valid record after replay.
    Status TruncateTo(uint64_t size) {
        if (fh_ == kInvalidFile) {
            return Status::InvalidArgument("WAL is not open");
        }
        auto s = backend_->Truncate(fh_, size);
        if (!s.ok()) return s;
        s = backend_->Sync(fh_, false);
        if (!s.ok()) return s;
        write_offset_ = size;
        pending_sync_ = false;
        return Status::OK();
    }

    // Truncate the WAL (after successful flush to SSTable).
    Status Truncate() {
        // Close and reopen to truncate.
        if (fh_ != kInvalidFile) {
            backend_->Close(fh_);
            fh_ = kInvalidFile;
        }
        std::string path = dir_ + "/wal.log";
        auto s = backend_->Delete(path);
        if (!s.ok()) return s;
        s = backend_->Open(path, true, false, &fh_);
        if (!s.ok()) return s;
        write_offset_ = 0;
        pending_sync_ = false;
        return backend_->SyncDirectory(dir_);
    }

    [[nodiscard]] uint64_t write_offset() const { return write_offset_; }

private:
    IOBackend* backend_;
    std::string dir_;
    FileHandle fh_ = kInvalidFile;
    uint64_t write_offset_ = 0;
    bool pending_sync_ = false;
};

} // namespace kv
