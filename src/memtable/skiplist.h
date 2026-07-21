#pragma once
// Concurrent skip list memtable.
// Thread-safe for concurrent reads and writes via read-write lock.

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace kv {


struct MemtableEntry {
    std::string key;
    std::string value;
    bool is_tombstone = false;
};

// Skip list memtable with concurrent access support.
class Memtable {
public:
    static constexpr int kMaxLevel = 16;
    static constexpr double kProbability = 0.5;

    Memtable() : head_(new Node("", kMaxLevel)), level_(1), size_(0), bytes_(0) {}

    ~Memtable() {
        Node* cur = head_->next[0];
        while (cur) {
            Node* next = cur->next[0];
            delete cur;
            cur = next;
        }
        delete head_;
    }

    // Insert or update a key-value pair.
    void Put(std::string_view key, std::string_view value) {
        std::unique_lock lock(rw_);
        InsertInternal(std::string(key), std::string(value), false);
    }

    // Mark a key as deleted (tombstone).
    void Delete(std::string_view key) {
        std::unique_lock lock(rw_);
        InsertInternal(std::string(key), "", true);
    }

    // Get a value by key.
    // Returns: nullopt if not found, {value, is_tombstone} if found.
    std::optional<std::pair<std::string, bool>> Get(std::string_view key) const {
        std::shared_lock lock(rw_);
        Node* node = FindGreaterOrEqual(key);
        if (node && node->entry.key == key) {
            return std::make_pair(node->entry.value, node->entry.is_tombstone);
        }
        return std::nullopt;
    }

    // Iterate all entries in sorted order.
    void ForEach(const std::function<void(const MemtableEntry&)>& cb) const {
        std::shared_lock lock(rw_);
        Node* cur = head_->next[0];
        while (cur) {
            cb(cur->entry);
            cur = cur->next[0];
        }
    }

    // Get all entries sorted by key.
    std::vector<MemtableEntry> Entries() const {
        std::shared_lock lock(rw_);
        std::vector<MemtableEntry> result;
        result.reserve(size_);
        Node* cur = head_->next[0];
        while (cur) {
            result.push_back(cur->entry);
            cur = cur->next[0];
        }
        return result;
    }

    [[nodiscard]] size_t size() const { return size_.load(std::memory_order_relaxed); }
    [[nodiscard]] size_t bytes() const { return bytes_.load(std::memory_order_relaxed); }
    [[nodiscard]] bool empty() const { return size() == 0; }

    // Approximate memory usage threshold for flush.
    [[nodiscard]] bool ShouldFlush(size_t threshold_bytes) const {
        return bytes() >= threshold_bytes;
    }

private:
    struct Node {
        MemtableEntry entry;
        std::vector<Node*> next;

        Node(std::string_view key, int level)
            : entry{std::string(key), "", false}, next(level, nullptr) {}

        Node(MemtableEntry e, int level)
            : entry(std::move(e)), next(level, nullptr) {}
    };

    int RandomLevel() {
        int lvl = 1;
        thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        while (lvl < kMaxLevel && dist(rng) < kProbability) lvl++;
        return lvl;
    }

    Node* FindGreaterOrEqual(std::string_view key) const {
        Node* cur = head_;
        for (int i = level_ - 1; i >= 0; i--) {
            while (cur->next[i] && cur->next[i]->entry.key < key) {
                cur = cur->next[i];
            }
        }
        return cur->next[0];
    }

    void InsertInternal(std::string key, std::string value, bool tombstone) {
        // Check if key already exists.
        Node* existing = FindGreaterOrEqual(key);
        if (existing && existing->entry.key == key) {
            // Update in place.
            bytes_ -= existing->entry.value.size();
            existing->entry.value = std::move(value);
            existing->entry.is_tombstone = tombstone;
            bytes_ += existing->entry.value.size();
            return;
        }

        // Insert new node.
        int new_level = RandomLevel();
        if (new_level > level_) level_ = new_level;

        MemtableEntry entry{std::move(key), std::move(value), tombstone};
        auto* node = new Node(std::move(entry), new_level);

        // Find insertion points.
        Node* cur = head_;
        for (int i = new_level - 1; i >= 0; i--) {
            while (cur->next[i] && cur->next[i]->entry.key < node->entry.key) {
                cur = cur->next[i];
            }
            node->next[i] = cur->next[i];
            cur->next[i] = node;
        }

        size_++;
        bytes_ += node->entry.key.size() + node->entry.value.size() + 64;  // overhead estimate
    }

    Node* head_;
    int level_;
    std::atomic<size_t> size_;
    std::atomic<size_t> bytes_;
    mutable std::shared_mutex rw_;
};

} // namespace kv
