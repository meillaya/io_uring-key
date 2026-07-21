#pragma once
// Size-tiered compaction: merge SSTables of similar size into larger ones.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "io/io_backend.h"
#include "sstable/sstable.h"
#include "util/logging.h"
#include "util/status.h"

namespace kv {

struct SSTableMeta {
    std::string path;
    uint64_t file_size;
    int level;  // For future leveled compaction.
    uint64_t sequence;  // Creation order (newer = higher).
};

// Size-tiered compaction policy.
class SizeTieredCompaction {
public:
    static constexpr size_t kMinTablesForCompaction = 4;
    static constexpr double kSizeRatioThreshold = 2.0;

    // Select tables for compaction: group tables of similar size.
    // Returns indices into the input vector that should be compacted together.
    static std::vector<size_t> PickForCompaction(const std::vector<SSTableMeta>& tables) {
        if (tables.size() < kMinTablesForCompaction) return {};

        // Sort by size for grouping.
        std::vector<size_t> indices(tables.size());
        for (size_t i = 0; i < tables.size(); i++) indices[i] = i;
        std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
            return tables[a].file_size < tables[b].file_size;
        });

        // Find the largest group of similar-sized tables.
        std::vector<size_t> best_group;
        size_t i = 0;
        while (i < indices.size()) {
            std::vector<size_t> group = {indices[i]};
            uint64_t base_size = tables[indices[i]].file_size;
            size_t j = i + 1;
            while (j < indices.size()) {
                uint64_t sz = tables[indices[j]].file_size;
                if (base_size > 0 && static_cast<double>(sz) / base_size <= kSizeRatioThreshold) {
                    group.push_back(indices[j]);
                    j++;
                } else {
                    break;
                }
            }
            if (group.size() > best_group.size() && group.size() >= kMinTablesForCompaction) {
                best_group = group;
            }
            i = j;
        }

        return best_group;
    }

    // Merge multiple SSTables into one output SSTable.
    // Later tables (higher sequence) take precedence for duplicate keys.
    static Status Merge(IOBackend* backend, const std::vector<SSTableMeta>& inputs,
                        const std::string& output_path, size_t block_size = kDefaultBlockSize) {
        // Read all entries from all inputs, keeping the latest version of each key.
        // Process in order of sequence (oldest first, newest overwrites).
        std::vector<SSTableMeta> sorted = inputs;
        std::sort(sorted.begin(), sorted.end(),
                  [](const auto& a, const auto& b) { return a.sequence < b.sequence; });

        // Merge: use a map to deduplicate.
        // For large datasets this would use a k-way merge; for correctness, map is fine.
        std::map<std::string, SSTableEntry> merged;

        for (auto& meta : sorted) {
            SSTableReader reader(backend, meta.path);
            auto s = reader.Open();
            if (!s.ok()) return s;

            s = reader.ForEach([&](const SSTableEntry& entry) -> bool {
                merged[entry.key] = entry;
                return true;
            });
            if (!s.ok()) return s;
        }

        // Remove tombstones that have no older version beneath them
        // (i.e., tombstones at the bottom level can be dropped).
        // For size-tiered, we keep tombstones unless this is the only level.
        // Simple policy: keep all entries including tombstones.

        // Build sorted output.
        std::vector<SSTableEntry> entries;
        entries.reserve(merged.size());
        for (auto& [key, entry] : merged) {
            entries.push_back(std::move(entry));
        }

        SSTableWriter writer(backend, output_path, block_size);
        return writer.Build(entries);
    }
};

} // namespace kv
