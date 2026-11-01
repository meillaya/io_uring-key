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
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "compaction/compaction.h"
#include "engine/kv_store.h"
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

class Engine : public KVStore {
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

    Status Put(std::string_view key, std::string_view value) override {
        std::lock_guard lock(write_mu_);
        return PutLocked(key, value);
    }

    // Put only when the key is absent. The check and the write share one write
    // lock, so two clients cannot both observe "absent" and both succeed.
    Status PutIfAbsent(std::string_view key, std::string_view value, bool* inserted) override {
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
    Status PutIfPresent(std::string_view key, std::string_view value, bool* inserted) override {
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
    Status PutBatch(const std::vector<std::pair<std::string, std::string>>& entries) override {
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

    Status Delete(std::string_view key) override {
        std::lock_guard lock(write_mu_);
        return DeleteLocked(key);
    }

    // Authoritative read path (G008): distinguishes "not found" from
    // "corruption". Returns Status::OK() with *out set on a clean lookup
    // (nullopt *out == not found / tombstoned); returns Status::Corruption or
    // Status::IOError if an underlying SSTable read fails a CRC/IO check.
    Status Get(std::string_view key, std::optional<std::string>* out) override {
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
    Status DbSize(size_t* out) override {
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

    // Merge iterator over the memtable and every SSTable. It reads only the
    // blocks it needs and never materializes the live-key set.
    Status ScanAfter(std::string_view start, bool inclusive, size_t limit,
                     std::vector<std::pair<std::string, std::string>>* out,
                     bool* done = nullptr, std::string* cursor = nullptr) override {
        if (out == nullptr) return Status::InvalidArgument("scan output is null");
        out->clear();
        if (done) *done = false;
        if (cursor) cursor->clear();
        if (limit == 0) return Status::OK();

        auto mt = memtable_.load(std::memory_order_acquire);
        auto mem_entries = mt->Entries();

        std::vector<std::shared_ptr<SSTableReader>> readers;
        {
            std::lock_guard lock(sst_mu_);
            readers.reserve(sstables_.size());
            for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
                readers.push_back(it->reader);
            }
        }

        struct Source {
            int priority = 0;
            bool is_memtable = false;
            std::vector<MemtableEntry> mem;
            size_t mem_pos = 0;
            std::shared_ptr<SSTableReader> reader;
            std::unique_ptr<SSTableReader::Iterator> sst;

            [[nodiscard]] bool valid() const {
                if (is_memtable) return mem_pos < mem.size();
                return sst != nullptr && sst->valid();
            }
            [[nodiscard]] std::string_view key() const {
                if (is_memtable) return mem[mem_pos].key;
                return sst->key();
            }
            [[nodiscard]] std::string_view value() const {
                if (is_memtable) return mem[mem_pos].value;
                return sst->value();
            }
            [[nodiscard]] bool tombstone() const {
                if (is_memtable) return mem[mem_pos].is_tombstone;
                return sst->is_tombstone();
            }
            void Next() {
                if (is_memtable) {
                    mem_pos++;
                } else {
                    sst->Next();
                }
            }
        };

        std::vector<Source> sources;
        sources.reserve(1 + readers.size());

        Source mem_source;
        mem_source.priority = 0;
        mem_source.is_memtable = true;
        mem_source.mem = std::move(mem_entries);
        while (mem_source.mem_pos < mem_source.mem.size()) {
            auto& key = mem_source.mem[mem_source.mem_pos].key;
            if (key > start || (inclusive && key == start)) break;
            mem_source.mem_pos++;
        }
        if (mem_source.valid()) sources.push_back(std::move(mem_source));

        for (size_t i = 0; i < readers.size(); i++) {
            Source source;
            source.priority = static_cast<int>(i) + 1;
            source.reader = readers[i];
            source.sst = source.reader->NewIterator(start, inclusive);
            if (!source.sst->status().ok()) return source.sst->status();
            if (source.sst->valid()) sources.push_back(std::move(source));
        }

        if (sources.empty()) {
            if (done) *done = true;
            return Status::OK();
        }

        auto compare = [&sources](size_t left, size_t right) {
            auto left_key = sources[left].key();
            auto right_key = sources[right].key();
            if (left_key != right_key) return left_key > right_key;
            return sources[left].priority > sources[right].priority;
        };
        std::priority_queue<size_t, std::vector<size_t>, decltype(compare)>
            heap(compare);
        for (size_t i = 0; i < sources.size(); i++) heap.push(i);

        std::string last_key;
        while (!heap.empty() && out->size() < limit) {
            std::string key(sources[heap.top()].key());
            std::vector<size_t> same_key;
            while (!heap.empty() && sources[heap.top()].key() == key) {
                same_key.push_back(heap.top());
                heap.pop();
            }

            // The first entry has the newest source priority.
            bool tombstone = sources[same_key.front()].tombstone();
            std::string value(sources[same_key.front()].value());
            for (size_t index : same_key) {
                sources[index].Next();
                if (sources[index].valid()) heap.push(index);
            }

            last_key = std::move(key);
            if (!tombstone) out->emplace_back(last_key, std::move(value));
        }

        if (done) *done = heap.empty();
        if (cursor) *cursor = last_key;

        for (auto& source : sources) {
            if (source.sst != nullptr && !source.sst->status().ok()) {
                return source.sst->status();
            }
        }
        return Status::OK();
    }

    // Read up to `limit` live keys at or after `start`.
    Status Scan(std::string_view start, size_t limit,
                std::vector<std::pair<std::string, std::string>>* out) {
        return ScanAfter(start, true, limit, out, nullptr, nullptr);
    }

    // FLUSHDB (G008): delete ALL data. Clear the active memtable, remove every
    // SSTable file, and truncate the WAL. Scoped to this engine's data dir.
    Status Clear() override {
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
    Status FlushAndSync() override {
        std::lock_guard lock(write_mu_);
        auto s = FlushMemtableLocked();
        if (!s.ok()) return s;
        return wal_.Sync();
    }

    [[nodiscard]] IOBackend* backend() override { return backend_.get(); }
    [[nodiscard]] const EngineOptions& options() const { return opts_; }
    [[nodiscard]] bool is_leader() const override { return true; }

private:
    struct SSTableHandle {
        std::string path;
        uint64_t file_size;
        int level;
        uint64_t sequence;
        std::shared_ptr<SSTableReader> reader;
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

        auto rename_status = backend_->Rename(tmp_path, sst_path);
        if (!rename_status.ok()) return rename_status;

        // Make the new directory entry durable before the WAL is truncated.
        s = backend_->SyncDirectory(opts_.data_dir);
        if (!s.ok()) return s;

        auto reader = std::make_shared<SSTableReader>(backend_.get(), sst_path);
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

        auto rename_status = backend_->Rename(tmp_path, output_path);
        if (!rename_status.ok()) return rename_status;

        // Make the merged table durable before deleting its inputs.
        s = backend_->SyncDirectory(opts_.data_dir);
        if (!s.ok()) return s;

        auto reader = std::make_shared<SSTableReader>(backend_.get(), output_path);
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
            auto reader = std::make_shared<SSTableReader>(backend_.get(), path);
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
