#pragma once
// Store interface consumed by the RESP command handler. Engine implements it
// directly; RaftStore implements it on top of Engine and a Raft node.

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "io/io_backend.h"
#include "util/status.h"

namespace kv {

class KVStore {
public:
    virtual ~KVStore() = default;

    virtual Status Get(std::string_view key, std::optional<std::string>* out) = 0;
    virtual Status Put(std::string_view key, std::string_view value) = 0;
    virtual Status PutIfAbsent(std::string_view key, std::string_view value,
                               bool* inserted) = 0;
    virtual Status PutIfPresent(std::string_view key, std::string_view value,
                                bool* inserted) = 0;
    virtual Status PutBatch(
        const std::vector<std::pair<std::string, std::string>>& entries) = 0;
    virtual Status Delete(std::string_view key) = 0;
    virtual Status DbSize(size_t* out) = 0;
    virtual Status ScanAfter(std::string_view start, bool inclusive, size_t limit,
                             std::vector<std::pair<std::string, std::string>>* out,
                             bool* done, std::string* cursor) = 0;
    virtual Status Clear() = 0;
    virtual Status FlushAndSync() { return Status::OK(); }
    virtual IOBackend* backend() = 0;
    virtual bool is_leader() const { return true; }
};

} // namespace kv
