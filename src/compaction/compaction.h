#pragma once
// Level-0 compaction: merge all tables into one when the count hits four.

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

// Level-0 compaction policy. The trigger is a table count; the action is a
// full merge. Merging every table keeps the output's sequence honest, because
// no older table remains underneath it to shadow a newer value.
class LevelZeroCompaction {
public:
    static constexpr size_t kMinTablesForCompaction = 4;

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

        // Keep every entry, including tombstones. A tombstone is cheap compared
        // with the risk of resurrecting a key when the merge set changes.

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
