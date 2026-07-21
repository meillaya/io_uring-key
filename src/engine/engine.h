#pragma once
// LSM Engine: Get/Put/Delete, recovery, flush, compaction.

#include <algorithm>
#include <atomic>
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
          memtable_(std::make_unique<Memtable>()),
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
        size_t replayed = 0;
        s = wal_.Replay([&](const WALRecord& rec) -> bool {
            if (rec.type == WALRecordType::kPut) {
                memtable_->Put(rec.key, rec.value);
            } else if (rec.type == WALRecordType::kDelete) {
                memtable_->Delete(rec.key);
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

        // Write to WAL first.
        WALRecord rec{WALRecordType::kPut, std::string(key), std::string(value)};
        auto s = wal_.Append(rec);
        if (!s.ok()) return s;
        if (opts_.sync_wal) {
            s = wal_.Sync();
            if (!s.ok()) return s;
        }

        // Write to memtable.
        memtable_->Put(key, value);

        // Check if memtable needs flushing.
        if (memtable_->ShouldFlush(opts_.memtable_flush_threshold)) {
            s = FlushMemtable();
            if (!s.ok()) return s;
        }

        return Status::OK();
    }

    Status Delete(std::string_view key) {
        std::lock_guard lock(write_mu_);

        WALRecord rec{WALRecordType::kDelete, std::string(key), ""};
        auto s = wal_.Append(rec);
        if (!s.ok()) return s;
        if (opts_.sync_wal) {
            s = wal_.Sync();
            if (!s.ok()) return s;
        }

        memtable_->Delete(key);

        if (memtable_->ShouldFlush(opts_.memtable_flush_threshold)) {
            s = FlushMemtable();
            if (!s.ok()) return s;
        }

        return Status::OK();
    }

    // Get a value. Returns nullopt if not found.
    std::optional<std::string> Get(std::string_view key) {
        // Check memtable first (most recent data).
        auto mem_result = memtable_->Get(key);
        if (mem_result.has_value()) {
            if (mem_result->second) return std::nullopt;  // Tombstone.
            return mem_result->first;
        }

        // Check SSTables from newest to oldest.
        std::lock_guard lock(sst_mu_);
        for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
            bool is_tombstone = false;
            auto val = it->reader->Get(key, &is_tombstone);
            if (val.has_value()) return val;
            if (is_tombstone) return std::nullopt;  // Deleted in this SSTable.
        }

        return std::nullopt;
    }

    // Flush memtable to a new SSTable.
    Status FlushMemtable() {
        auto entries_vec = memtable_->Entries();
        if (entries_vec.empty()) return Status::OK();

        // Convert to SSTable entries.
        std::vector<SSTableEntry> sst_entries;
        sst_entries.reserve(entries_vec.size());
        for (auto& e : entries_vec) {
            sst_entries.push_back({e.key, e.value, e.is_tombstone});
        }

        // Generate SSTable filename.
        uint64_t seq = next_sst_seq_.fetch_add(1);
        std::string sst_path = opts_.data_dir + "/sst_" + std::to_string(seq) + ".sst";

        // Write SSTable.
        SSTableWriter writer(backend_.get(), sst_path, opts_.sstable_block_size);
        auto s = writer.Build(sst_entries);
        if (!s.ok()) return s;

        // Open the new SSTable for reading.
        auto reader = std::make_unique<SSTableReader>(backend_.get(), sst_path);
        s = reader->Open();
        if (!s.ok()) return s;

        {
            std::lock_guard lock(sst_mu_);
            uint64_t file_size = 0;
            backend_->FileSize(0, &file_size);  // Approximate.
            sstables_.push_back({
                .path = sst_path,
                .file_size = file_size,
                .level = 0,
                .sequence = seq,
                .reader = std::move(reader),
            });
        }

        // Truncate WAL and reset memtable.
        s = wal_.Truncate();
        if (!s.ok()) return s;

        memtable_ = std::make_unique<Memtable>();

        KV_LOG_INFO("Flushed memtable to %s (%zu entries)", sst_path.c_str(), sst_entries.size());
        return Status::OK();
    }

    // Run compaction if needed.
    Status MaybeCompact() {
        std::lock_guard lock(sst_mu_);

        std::vector<SSTableMeta> metas;
        metas.reserve(sstables_.size());
        for (auto& sst : sstables_) {
            metas.push_back({sst.path, sst.file_size, sst.level, sst.sequence});
        }

        auto to_compact = SizeTieredCompaction::PickForCompaction(metas);
        if (to_compact.empty()) return Status::OK();

        // Collect paths to compact.
        std::vector<SSTableMeta> inputs;
        for (size_t idx : to_compact) {
            inputs.push_back(metas[idx]);
        }

        uint64_t seq = next_sst_seq_.fetch_add(1);
        std::string output_path = opts_.data_dir + "/sst_" + std::to_string(seq) + ".sst";

        auto s = SizeTieredCompaction::Merge(backend_.get(), inputs, output_path,
                                              opts_.sstable_block_size);
        if (!s.ok()) return s;

        // Open new SSTable.
        auto reader = std::make_unique<SSTableReader>(backend_.get(), output_path);
        s = reader->Open();
        if (!s.ok()) return s;

        // Remove old SSTables and add new one.
        std::vector<std::string> old_paths;
        for (size_t idx : to_compact) {
            old_paths.push_back(metas[idx].path);
        }

        // Remove old entries.
        std::erase_if(sstables_, [&](const auto& sst) {
            return std::find(old_paths.begin(), old_paths.end(), sst.path) != old_paths.end();
        });

        sstables_.push_back({
            .path = output_path,
            .file_size = 0,
            .level = 0,
            .sequence = seq,
            .reader = std::move(reader),
        });

        // Delete old files.
        for (auto& path : old_paths) {
            backend_->Delete(path);
        }

        KV_LOG_INFO("Compacted %zu SSTables into %s", to_compact.size(), output_path.c_str());
        return Status::OK();
    }

    // Force flush + sync (for clean shutdown).
    Status FlushAndSync() {
        std::lock_guard lock(write_mu_);
        auto s = FlushMemtable();
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

    Status LoadSSTables() {
        namespace fs = std::filesystem;
        std::vector<std::pair<uint64_t, std::string>> sst_files;

        for (auto& entry : fs::directory_iterator(opts_.data_dir)) {
            auto name = entry.path().filename().string();
            if (name.starts_with("sst_") && name.ends_with(".sst")) {
                // Extract sequence number.
                auto seq_str = name.substr(4, name.size() - 8);
                uint64_t seq = std::stoull(seq_str);
                sst_files.emplace_back(seq, entry.path().string());
            }
        }

        // Sort by sequence (oldest first).
        std::sort(sst_files.begin(), sst_files.end());

        for (auto& [seq, path] : sst_files) {
            auto reader = std::make_unique<SSTableReader>(backend_.get(), path);
            auto s = reader->Open();
            if (!s.ok()) {
                KV_LOG_WARN("Failed to open SSTable %s: %s", path.c_str(), s.ToString().c_str());
                continue;
            }
            uint64_t file_size = fs::file_size(path);
            sstables_.push_back({
                .path = path,
                .file_size = file_size,
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
    std::unique_ptr<Memtable> memtable_;
    WAL wal_;
    std::vector<SSTableHandle> sstables_;
    std::mutex write_mu_;
    std::mutex sst_mu_;
    std::atomic<uint64_t> next_sst_seq_{0};
};

} // namespace kv
