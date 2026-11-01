#pragma once
// LSM Engine: Get/Put/Delete, recovery, flush, compaction.

#include <algorithm>
#include <atomic>
#include <charconv>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "compaction/compaction.h"
#include "io/io_backend.h"
#include "memtable/skiplist.h"
#include "sstable/sstable.h"
#include "util/logging.h"
#include "util/status.h"
#include "wal/wal.h"

namespace kv {

struct EngineOptions {
    std::string data_dir;
    size_t memtable_flush_threshold = 4 * 1024 * 1024;  // 4 MB
    size_t sstable_block_size = kDefaultBlockSize;
    bool sync_wal = true;  // Fsync WAL on every write.
};

class Engine {
public:
    explicit Engine(std::unique_ptr<IOBackend> backend, EngineOptions opts)
        : backend_(std::move(backend)), opts_(std::move(opts)),
          memtable_(std::make_shared<Memtable>()),
          wal_(backend_.get(), opts_.data_dir) {}

    ~Engine() {
        // Close all SSTable readers.
        sstables_.clear();
    }

    Status Open() {
        // Create data directory if needed.
        std::filesystem::create_directories(opts_.data_dir);

        // Open WAL.
        auto s = wal_.Open();
        if (!s.ok()) return s;

        // Replay WAL into memtable.
        auto mt = memtable_.load(std::memory_order_relaxed);
        size_t replayed = 0;
        s = wal_.Replay([&](const WALRecord& rec) -> bool {
            if (rec.type == WALRecordType::kPut) {
                mt->Put(rec.key, rec.value);
            } else if (rec.type == WALRecordType::kDelete) {
                mt->Delete(rec.key);
            }
            return true;
        }, &replayed);
        if (!s.ok()) return s;

        if (replayed > 0) {
            KV_LOG_INFO("WAL replay: recovered %zu records", replayed);
        }

        // Load existing SSTables.
        s = LoadSSTables();
        if (!s.ok()) return s;

        return Status::OK();
    }

    Status Put(std::string_view key, std::string_view value) {
        std::lock_guard lock(write_mu_);
        return PutLocked(key, value);
    }

    // Put only when the key is absent. The check and the write share one write
    // lock, so two clients cannot both observe "absent" and both succeed.
    Status PutIfAbsent(std::string_view key, std::string_view value, bool* inserted) {
        std::lock_guard lock(write_mu_);
        std::optional<std::string> existing;
        auto s = Get(key, &existing);
        if (!s.ok()) return s;
        if (existing.has_value()) {
            if (inserted) *inserted = false;
            return Status::OK();
        }
        auto write_status = PutLocked(key, value);
        if (inserted) *inserted = write_status.ok();
        return write_status;
    }

    // Put only when the key exists. Same lock discipline as PutIfAbsent.
    Status PutIfPresent(std::string_view key, std::string_view value, bool* inserted) {
        std::lock_guard lock(write_mu_);
        std::optional<std::string> existing;
        auto s = Get(key, &existing);
        if (!s.ok()) return s;
        if (!existing.has_value()) {
            if (inserted) *inserted = false;
            return Status::OK();
        }
        auto write_status = PutLocked(key, value);
        if (inserted) *inserted = write_status.ok();
        return write_status;
    }

    // Apply a batch of puts under one write lock and one WAL append. This is
    // the backing operation for MSET: other clients cannot observe a partial
    // batch, and one failed write rejects the whole call.
    Status PutBatch(const std::vector<std::pair<std::string, std::string>>& entries) {
        if (entries.empty()) return Status::OK();

        std::lock_guard lock(write_mu_);

        std::vector<WALRecord> records;
        records.reserve(entries.size());
        for (auto& [key, value] : entries) {
            records.push_back({WALRecordType::kPut, key, value});
        }

        auto s = wal_.AppendBatch(records);
        if (!s.ok()) return s;
        if (opts_.sync_wal) {
            s = wal_.Sync();
            if (!s.ok()) return s;
        }

        auto mt = memtable_.load(std::memory_order_acquire);
        mt->PutBatch(entries);
        MarkDirty();

        if (mt->ShouldFlush(opts_.memtable_flush_threshold)) {
            s = FlushMemtableLocked();
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    Status Delete(std::string_view key) {
        std::lock_guard lock(write_mu_);
        return DeleteLocked(key);
    }

    // Authoritative read path (G008): distinguishes "not found" from
    // "corruption". Returns Status::OK() with *out set on a clean lookup
    // (nullopt *out == not found / tombstoned); returns Status::Corruption or
    // Status::IOError if an underlying SSTable read fails a CRC/IO check.
    Status Get(std::string_view key, std::optional<std::string>* out) {
        if (out) out->reset();

        // Check memtable first (most recent data). Load a stable snapshot of the
        // memtable pointer so a concurrent flush swap cannot race with us.
        auto mt = memtable_.load(std::memory_order_acquire);
        auto mem_result = mt->Get(key);
        if (mem_result.has_value()) {
            if (mem_result->second) return Status::OK();  // Tombstone -> not found.
            if (out) *out = mem_result->first;
            return Status::OK();
        }

        // Check SSTables from newest to oldest.
        std::lock_guard lock(sst_mu_);
        for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
            bool is_tombstone = false;
            std::optional<std::string> val;
            auto s = it->reader->Get(key, &val, &is_tombstone);
            if (!s.ok()) return s;          // Surface corruption / IO error.
            if (val.has_value()) {
                if (out) *out = std::move(val);
                return Status::OK();
            }
            if (is_tombstone) return Status::OK();  // Deleted in this SSTable.
        }

        return Status::OK();  // Not found anywhere.
    }

    // Convenience overload: returns nullopt for both "not found" and any error.
    // Callers that must distinguish corruption (e.g. the RESP server) should use
    // the Status-returning Get(key, out*) above.
    std::optional<std::string> Get(std::string_view key) {
        std::optional<std::string> out;
        auto s = Get(key, &out);
        if (!s.ok()) return std::nullopt;
        return out;
    }

    // Count live (non-tombstoned) keys across the memtable and SSTables.
    // The count is cached until the next write, so repeated DBSIZE calls do
    // not rescan every table.
    Status DbSize(size_t* out) {
        if (!size_dirty_.load(std::memory_order_acquire)) {
            if (out) *out = cached_size_.load(std::memory_order_relaxed);
            return Status::OK();
        }

        std::lock_guard wlock(write_mu_);
        if (!size_dirty_.load(std::memory_order_acquire)) {
            if (out) *out = cached_size_.load(std::memory_order_relaxed);
            return Status::OK();
        }

        std::vector<std::string> keys;
        auto s = CollectLiveKeysLocked(&keys);
        if (!s.ok()) return s;

        if (out) *out = keys.size();
        cached_size_.store(keys.size(), std::memory_order_relaxed);
        size_dirty_.store(false, std::memory_order_release);
        return Status::OK();
    }

    [[nodiscard]] size_t DbSize() {
        size_t count = 0;
        auto s = DbSize(&count);
        return s.ok() ? count : 0;
    }

    // Snapshot of all live keys, newest version wins. Used by SCAN.
    Status Keys(std::vector<std::string>* out) {
        std::lock_guard wlock(write_mu_);
        return CollectLiveKeysLocked(out);
    }

    // Read up to `limit` live keys at or after `start`, in key order. Used by
    // the short-range scan workload.
    Status Scan(std::string_view start, size_t limit,
                std::vector<std::pair<std::string, std::string>>* out) {
        if (out == nullptr) return Status::InvalidArgument("scan output is null");
        std::lock_guard wlock(write_mu_);
        out->clear();
        if (limit == 0) return Status::OK();

        std::vector<std::string> keys;
        auto s = CollectLiveKeysLocked(&keys);
        if (!s.ok()) return s;

        auto it = std::lower_bound(keys.begin(), keys.end(), start,
                                   [](const std::string& key, std::string_view value) {
                                       return key < value;
                                   });
        for (; it != keys.end() && out->size() < limit; ++it) {
            std::optional<std::string> value;
            s = Get(*it, &value);
            if (!s.ok()) return s;
            if (value.has_value()) {
                out->emplace_back(*it, std::move(*value));
            }
        }
        return Status::OK();
    }

    // FLUSHDB (G008): delete ALL data. Clear the active memtable, remove every
    // SSTable file, and truncate the WAL. Scoped to this engine's data dir.
    Status Clear() {
        std::lock_guard wlock(write_mu_);

        // 1. Reset the memtable (swap in a fresh empty one; in-flight readers
        //    keep their shared_ptr to the old table until they finish).
        memtable_.store(std::make_shared<Memtable>(), std::memory_order_release);

        // 2. Remove all SSTable files + drop the readers.
        std::vector<std::string> paths;
        {
            std::lock_guard lock(sst_mu_);
            for (auto& sst : sstables_) paths.push_back(sst.path);
            sstables_.clear();
        }
        for (auto& path : paths) {
            auto s = backend_->Delete(path);
            if (!s.ok()) {
                MarkDirty();
                return s;
            }
        }

        // 3. The unlinks are only durable after the directory is synced.
        auto s = backend_->SyncDirectory(opts_.data_dir);
        if (!s.ok()) {
            MarkDirty();
            return s;
        }

        // 4. Truncate the WAL so recovery does not resurrect the cleared keys.
        s = wal_.Truncate();
        if (!s.ok()) {
            MarkDirty();
            return s;
        }

        cached_size_.store(0, std::memory_order_relaxed);
        size_dirty_.store(false, std::memory_order_release);
        KV_LOG_WARN("Clear: flushed all engine data (%zu SSTables removed)", paths.size());
        return Status::OK();
    }

    // Test/observability accessor: number of SSTables currently loaded.
    [[nodiscard]] size_t sstable_count() {
        std::lock_guard lock(sst_mu_);
        return sstables_.size();
    }

    // Force the current memtable to an SSTable.
    Status FlushMemtable() {
        std::lock_guard lock(write_mu_);
        return FlushMemtableLocked();
    }

    // Force flush + sync (for clean shutdown).
    Status FlushAndSync() {
        std::lock_guard lock(write_mu_);
        auto s = FlushMemtableLocked();
        if (!s.ok()) return s;
        return wal_.Sync();
    }

    [[nodiscard]] IOBackend* backend() { return backend_.get(); }
    [[nodiscard]] const EngineOptions& options() const { return opts_; }

private:
    struct SSTableHandle {
        std::string path;
        uint64_t file_size;
        int level;
        uint64_t sequence;
        std::unique_ptr<SSTableReader> reader;
    };

    Status PutLocked(std::string_view key, std::string_view value) {
        WALRecord rec{WALRecordType::kPut, std::string(key), std::string(value)};
        auto s = wal_.Append(rec);
        if (!s.ok()) return s;
        if (opts_.sync_wal) {
            s = wal_.Sync();
            if (!s.ok()) return s;
        }

        auto mt = memtable_.load(std::memory_order_acquire);
        mt->Put(key, value);
        MarkDirty();

        if (mt->ShouldFlush(opts_.memtable_flush_threshold)) {
            s = FlushMemtableLocked();
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    Status DeleteLocked(std::string_view key) {
        WALRecord rec{WALRecordType::kDelete, std::string(key), ""};
        auto s = wal_.Append(rec);
        if (!s.ok()) return s;
        if (opts_.sync_wal) {
            s = wal_.Sync();
            if (!s.ok()) return s;
        }

        auto mt = memtable_.load(std::memory_order_acquire);
        mt->Delete(key);
        MarkDirty();

        if (mt->ShouldFlush(opts_.memtable_flush_threshold)) {
            s = FlushMemtableLocked();
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    // Caller holds write_mu_. The SSTable file is fsynced by the writer, and
    // the directory is synced here before the WAL is truncated.
    Status FlushMemtableLocked() {
        auto mt = memtable_.load(std::memory_order_acquire);
        auto entries_vec = mt->Entries();
        if (entries_vec.empty()) return Status::OK();

        std::vector<SSTableEntry> sst_entries;
        sst_entries.reserve(entries_vec.size());
        for (auto& e : entries_vec) {
            sst_entries.push_back({e.key, e.value, e.is_tombstone});
        }

        uint64_t seq = next_sst_seq_.fetch_add(1);
        std::string sst_path = opts_.data_dir + "/sst_" + std::to_string(seq) + ".sst";
        std::string tmp_path = sst_path + ".tmp";

        // Write to a temporary name and rename after the file is fsynced. A
        // crash mid-write leaves a .tmp file, never a half-valid .sst.
        SSTableWriter writer(backend_.get(), tmp_path, opts_.sstable_block_size);
        auto s = writer.Build(sst_entries);
        if (!s.ok()) return s;

        std::error_code rename_ec;
        std::filesystem::rename(tmp_path, sst_path, rename_ec);
        if (rename_ec) {
            return Status::IOError("rename SSTable " + tmp_path + ": " +
                                   rename_ec.message());
        }

        // Make the new directory entry durable before the WAL is truncated.
        s = backend_->SyncDirectory(opts_.data_dir);
        if (!s.ok()) return s;

        auto reader = std::make_unique<SSTableReader>(backend_.get(), sst_path);
        s = reader->Open();
        if (!s.ok()) return s;

        uint64_t file_size = 0;
        std::error_code ec;
        auto fsz = std::filesystem::file_size(sst_path, ec);
        if (!ec) file_size = static_cast<uint64_t>(fsz);

        size_t table_count = 0;
        {
            std::lock_guard lock(sst_mu_);
            sstables_.push_back({
                .path = sst_path,
                .file_size = file_size,
                .level = 0,
                .sequence = seq,
                .reader = std::move(reader),
            });
            std::sort(sstables_.begin(), sstables_.end(),
                      [](const SSTableHandle& a, const SSTableHandle& b) {
                          return a.sequence < b.sequence;
                      });
            table_count = sstables_.size();
        }

        // The SSTable is durable now, so the WAL can drop these records.
        s = wal_.Truncate();
        if (!s.ok()) return s;

        memtable_.store(std::make_shared<Memtable>(), std::memory_order_release);
        KV_LOG_INFO("Flushed memtable to %s (%zu entries)", sst_path.c_str(),
                    sst_entries.size());

        if (table_count >= LevelZeroCompaction::kMinTablesForCompaction) {
            auto cs = MaybeCompactLocked();
            if (!cs.ok()) return cs;
        }
        return Status::OK();
    }

    // Caller holds write_mu_. Merge every level-0 table. Merging all tables is
    // what keeps the new output's sequence honest: no older table is left
    // underneath it to shadow a newer value.
    Status MaybeCompactLocked() {
        std::vector<SSTableMeta> inputs;
        {
            std::lock_guard lock(sst_mu_);
            if (sstables_.size() < LevelZeroCompaction::kMinTablesForCompaction) {
                return Status::OK();
            }
            inputs.reserve(sstables_.size());
            for (auto& sst : sstables_) {
                inputs.push_back({sst.path, sst.file_size, sst.level, sst.sequence});
            }
        }

        uint64_t seq = next_sst_seq_.fetch_add(1);
        std::string output_path = opts_.data_dir + "/sst_" + std::to_string(seq) + ".sst";
        std::string tmp_path = output_path + ".tmp";

        // Merge with no SSTable lock held, so readers keep serving the old
        // tables while the new one is built.
        auto s = LevelZeroCompaction::Merge(backend_.get(), inputs, tmp_path,
                                              opts_.sstable_block_size);
        if (!s.ok()) return s;

        std::error_code rename_ec;
        std::filesystem::rename(tmp_path, output_path, rename_ec);
        if (rename_ec) {
            return Status::IOError("rename SSTable " + tmp_path + ": " +
                                   rename_ec.message());
        }

        // Make the merged table durable before deleting its inputs.
        s = backend_->SyncDirectory(opts_.data_dir);
        if (!s.ok()) return s;

        auto reader = std::make_unique<SSTableReader>(backend_.get(), output_path);
        s = reader->Open();
        if (!s.ok()) return s;

        std::vector<std::string> old_paths;
        old_paths.reserve(inputs.size());
        for (auto& input : inputs) old_paths.push_back(input.path);

        uint64_t out_size = 0;
        std::error_code ec;
        auto fsz = std::filesystem::file_size(output_path, ec);
        if (!ec) out_size = static_cast<uint64_t>(fsz);

        {
            std::lock_guard lock(sst_mu_);
            std::erase_if(sstables_, [&](const SSTableHandle& sst) {
                return std::find(old_paths.begin(), old_paths.end(), sst.path) !=
                       old_paths.end();
            });
            sstables_.push_back({
                .path = output_path,
                .file_size = out_size,
                .level = 0,
                .sequence = seq,
                .reader = std::move(reader),
            });
            std::sort(sstables_.begin(), sstables_.end(),
                      [](const SSTableHandle& a, const SSTableHandle& b) {
                          return a.sequence < b.sequence;
                      });
        }

        for (auto& path : old_paths) {
            auto ds = backend_->Delete(path);
            if (!ds.ok()) return ds;
        }
        s = backend_->SyncDirectory(opts_.data_dir);
        if (!s.ok()) return s;

        KV_LOG_INFO("Compacted %zu SSTables into %s", old_paths.size(),
                    output_path.c_str());
        return Status::OK();
    }

    Status CollectLiveKeysLocked(std::vector<std::string>* out) {
        std::map<std::string, bool> live;  // key -> is_live, newest wins
        auto mt = memtable_.load(std::memory_order_acquire);
        mt->ForEach([&](const MemtableEntry& e) {
            live.emplace(e.key, !e.is_tombstone);
        });

        std::lock_guard lock(sst_mu_);
        for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
            auto s = it->reader->ForEach([&](const SSTableEntry& e) -> bool {
                live.emplace(e.key, !e.is_tombstone);
                return true;
            });
            if (!s.ok()) return s;
        }

        if (out) {
            out->clear();
            out->reserve(live.size());
            for (auto& [key, is_live] : live) {
                if (is_live) out->push_back(key);
            }
        }
        return Status::OK();
    }

    void MarkDirty() { size_dirty_.store(true, std::memory_order_release); }

    Status LoadSSTables() {
        namespace fs = std::filesystem;
        std::vector<std::pair<uint64_t, std::string>> sst_files;

        std::error_code dir_ec;
        bool removed_temp = false;
        for (fs::directory_iterator it(opts_.data_dir, dir_ec), end; it != end;
             it.increment(dir_ec)) {
            auto name = it->path().filename().string();
            if (name.starts_with("sst_") && name.ends_with(".sst.tmp")) {
                // A crash interrupted a flush or compaction. The WAL or the old
                // tables still own this data, so the temp file is garbage.
                auto removed = backend_->Delete(it->path().string());
                if (removed.ok()) removed_temp = true;
                continue;
            }
            if (!name.starts_with("sst_") || !name.ends_with(".sst")) continue;

            auto seq_str = name.substr(4, name.size() - 8);
            uint64_t seq = 0;
            auto [ptr, ec] = std::from_chars(seq_str.data(),
                                             seq_str.data() + seq_str.size(), seq);
            if (ec != std::errc() || ptr != seq_str.data() + seq_str.size()) {
                KV_LOG_WARN("Ignoring SSTable with an invalid sequence: %s",
                            it->path().c_str());
                continue;
            }
            sst_files.emplace_back(seq, it->path().string());
        }
        if (dir_ec) {
            return Status::IOError("scan data dir: " + dir_ec.message());
        }
        if (removed_temp) {
            auto sync_status = backend_->SyncDirectory(opts_.data_dir);
            if (!sync_status.ok()) return sync_status;
        }

        std::sort(sst_files.begin(), sst_files.end());

        for (auto& [seq, path] : sst_files) {
            auto reader = std::make_unique<SSTableReader>(backend_.get(), path);
            auto s = reader->Open();
            if (!s.ok()) {
                return Status::Corruption("open SSTable " + path + ": " +
                                          s.ToString());
            }

            std::error_code size_ec;
            auto file_size = fs::file_size(path, size_ec);
            if (size_ec) {
                return Status::IOError("stat SSTable " + path + ": " +
                                       size_ec.message());
            }

            sstables_.push_back({
                .path = path,
                .file_size = static_cast<uint64_t>(file_size),
                .level = 0,
                .sequence = seq,
                .reader = std::move(reader),
            });
            if (seq >= next_sst_seq_.load()) {
                next_sst_seq_.store(seq + 1);
            }
        }

        if (!sstables_.empty()) {
            KV_LOG_INFO("Loaded %zu SSTables", sstables_.size());
        }
        return Status::OK();
    }

    std::unique_ptr<IOBackend> backend_;
    EngineOptions opts_;
    std::atomic<std::shared_ptr<Memtable>> memtable_;
    WAL wal_;
    std::vector<SSTableHandle> sstables_;
    std::mutex write_mu_;
    std::mutex sst_mu_;
    std::atomic<uint64_t> next_sst_seq_{0};
    std::atomic<bool> size_dirty_{true};
    std::atomic<size_t> cached_size_{0};
};

} // namespace kv
