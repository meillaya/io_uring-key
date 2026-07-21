#pragma once
// SSTable: sorted string table with block format, sparse index, bloom filter, footer.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "io/io_backend.h"
#include "util/crc32.h"
#include "util/status.h"

namespace kv {

// SSTable on-disk format:
//   [Data Block 0]
//   [Data Block 1]
//   ...
//   [Data Block N]
//   [Index Block]       — sparse index: one entry per data block
//   [Bloom Filter Block]
//   [Footer]            — fixed 48 bytes
//
// Data Block format:
//   [4 bytes: num_entries (uint32 LE)]
//   For each entry:
//     [4 bytes: key_len]
//     [key_len bytes: key]
//     [1 byte: is_tombstone]
//     [4 bytes: val_len]
//     [val_len bytes: value]
//   [4 bytes: CRC32C of block data above]
//
// Index Block format:
//   [4 bytes: num_entries]
//   For each entry:
//     [4 bytes: key_len]
//     [key_len bytes: last_key_in_block]
//     [8 bytes: block_offset (uint64 LE)]
//     [4 bytes: block_size (uint32 LE)]
//   [4 bytes: CRC32C]
//
// Bloom Filter Block:
//   [4 bytes: num_bits]
//   [4 bytes: num_hashes]
//   [ceil(num_bits/8) bytes: bit array]
//   [4 bytes: CRC32C]
//
// Footer (48 bytes):
//   [8 bytes: index_offset]
//   [4 bytes: index_size]
//   [8 bytes: bloom_offset]
//   [4 bytes: bloom_size]
//   [8 bytes: data_size (total data blocks size)]
//   [8 bytes: magic (0x4B56535354000001 = "KVSST\x00\x00\x01")]
//   [4 bytes: CRC32C of footer above (44 bytes)]

inline constexpr uint64_t kSSTableMagic = 0x4B56535354000001ULL;
inline constexpr size_t kFooterSize = 48;
inline constexpr size_t kDefaultBlockSize = 4096;
inline constexpr size_t kBloomBitsPerKey = 10;

struct SSTableEntry {
    std::string key;
    std::string value;
    bool is_tombstone = false;
};

// Simple bloom filter.
class BloomFilter {
public:
    BloomFilter() = default;

    explicit BloomFilter(size_t expected_keys) {
        num_bits_ = expected_keys * kBloomBitsPerKey;
        if (num_bits_ < 64) num_bits_ = 64;
        num_hashes_ = 7;  // Optimal for 10 bits/key
        bits_.resize((num_bits_ + 7) / 8, 0);
    }

    void Add(std::string_view key) {
        uint64_t h = Hash(key);
        for (size_t i = 0; i < num_hashes_; i++) {
            uint64_t bit = (h + i * 0x9E3779B97F4A7C15ULL) % num_bits_;
            bits_[bit / 8] |= (1 << (bit % 8));
        }
    }

    [[nodiscard]] bool MayContain(std::string_view key) const {
        uint64_t h = Hash(key);
        for (size_t i = 0; i < num_hashes_; i++) {
            uint64_t bit = (h + i * 0x9E3779B97F4A7C15ULL) % num_bits_;
            if (!(bits_[bit / 8] & (1 << (bit % 8)))) return false;
        }
        return true;
    }

    // Serialize/deserialize for on-disk storage.
    std::vector<uint8_t> Serialize() const {
        std::vector<uint8_t> data(4 + 4 + bits_.size() + 4);
        uint32_t nb = static_cast<uint32_t>(num_bits_);
        uint32_t nh = static_cast<uint32_t>(num_hashes_);
        std::memcpy(data.data(), &nb, 4);
        std::memcpy(data.data() + 4, &nh, 4);
        std::memcpy(data.data() + 8, bits_.data(), bits_.size());
        uint32_t crc = CRC32::Compute(data.data(), 8 + bits_.size());
        std::memcpy(data.data() + 8 + bits_.size(), &crc, 4);
        return data;
    }

    static BloomFilter Deserialize(const uint8_t* data, size_t len) {
        BloomFilter bf;
        if (len < 12) return bf;
        std::memcpy(&bf.num_bits_, data, 4);
        std::memcpy(&bf.num_hashes_, data + 4, 4);
        size_t bytes = (bf.num_bits_ + 7) / 8;
        if (8 + bytes + 4 > len) return bf;
        bf.bits_.assign(data + 8, data + 8 + bytes);
        return bf;
    }

private:
    static uint64_t Hash(std::string_view key) {
        // FNV-1a 64-bit
        uint64_t h = 0xCBF29CE484222325ULL;
        for (uint8_t c : key) {
            h ^= c;
            h *= 0x100000001B3ULL;
        }
        return h;
    }

    size_t num_bits_ = 0;
    size_t num_hashes_ = 0;
    std::vector<uint8_t> bits_;
};

// SSTable writer: builds an SSTable file from sorted entries.
class SSTableWriter {
public:
    SSTableWriter(IOBackend* backend, std::string path, size_t block_size = kDefaultBlockSize)
        : backend_(backend), path_(std::move(path)), block_size_(block_size) {}

    // Build from sorted entries. Entries MUST be sorted by key.
    Status Build(const std::vector<SSTableEntry>& entries) {
        FileHandle fh;
        auto s = backend_->Open(path_, true, false, &fh);
        if (!s.ok()) return s;

        BloomFilter bloom(entries.size());
        std::vector<std::tuple<std::string, uint64_t, uint32_t>> index;  // last_key, offset, size

        uint64_t offset = 0;
        std::vector<uint8_t> block_buf;
        block_buf.reserve(block_size_ * 2);

        auto flush_block = [&]() -> Status {
            if (block_buf.empty()) return Status::OK();

            // Compute block CRC and append.
            uint32_t crc = CRC32::Compute(block_buf.data(), block_buf.size());
            size_t old_size = block_buf.size();
            block_buf.resize(old_size + 4);
            std::memcpy(block_buf.data() + old_size, &crc, 4);

            auto ws = backend_->Write(fh, offset, block_buf.data(), block_buf.size());
            if (!ws.ok()) { backend_->Close(fh); return ws; }

            offset += block_buf.size();
            block_buf.clear();
            return Status::OK();
        };

        for (size_t i = 0; i < entries.size(); i++) {
            bloom.Add(entries[i].key);

            // Serialize entry into block buffer.
            size_t entry_size = 4 + entries[i].key.size() + 1 + 4 + entries[i].value.size();

            // If adding this entry would exceed block size and block is non-empty, flush.
            if (!block_buf.empty() && block_buf.size() + entry_size > block_size_) {
                // Record index entry for the block about to be flushed.
                std::string last_key = entries[i - 1].key;
                uint32_t bsize = static_cast<uint32_t>(block_buf.size() + 4);  // +CRC
                index.emplace_back(std::move(last_key), offset, bsize);

                auto fs = flush_block();
                if (!fs.ok()) { backend_->Close(fh); return fs; }
            }

            // If block is empty, this is the start of a new block.
            if (block_buf.empty()) {
                // Write num_entries placeholder (will be patched).
                uint32_t num = 0;
                block_buf.resize(4);
                std::memcpy(block_buf.data(), &num, 4);
            }

            // Serialize entry.
            size_t pos = block_buf.size();
            block_buf.resize(pos + entry_size);
            uint8_t* p = block_buf.data() + pos;

            uint32_t klen = static_cast<uint32_t>(entries[i].key.size());
            std::memcpy(p, &klen, 4); p += 4;
            std::memcpy(p, entries[i].key.data(), klen); p += klen;
            *p = entries[i].is_tombstone ? 1 : 0; p += 1;
            uint32_t vlen = static_cast<uint32_t>(entries[i].value.size());
            std::memcpy(p, &vlen, 4); p += 4;
            if (vlen > 0) { std::memcpy(p, entries[i].value.data(), vlen); }

            // Increment num_entries in block header.
            uint32_t num;
            std::memcpy(&num, block_buf.data(), 4);
            num++;
            std::memcpy(block_buf.data(), &num, 4);
        }

        // Flush last block.
        if (!block_buf.empty()) {
            std::string last_key = entries.empty() ? "" : entries.back().key;
            uint32_t bsize = static_cast<uint32_t>(block_buf.size() + 4);
            index.emplace_back(std::move(last_key), offset, bsize);
            auto fs = flush_block();
            if (!fs.ok()) { backend_->Close(fh); return fs; }
        }

        uint64_t data_size = offset;

        // Write index block.
        uint64_t index_offset = offset;
        std::vector<uint8_t> index_buf;
        {
            uint32_t num = static_cast<uint32_t>(index.size());
            index_buf.resize(4);
            std::memcpy(index_buf.data(), &num, 4);
            for (auto& [key, off, sz] : index) {
                size_t pos = index_buf.size();
                index_buf.resize(pos + 4 + key.size() + 8 + 4);
                uint8_t* p = index_buf.data() + pos;
                uint32_t klen = static_cast<uint32_t>(key.size());
                std::memcpy(p, &klen, 4); p += 4;
                std::memcpy(p, key.data(), klen); p += klen;
                std::memcpy(p, &off, 8); p += 8;
                std::memcpy(p, &sz, 4);
            }
            uint32_t crc = CRC32::Compute(index_buf.data(), index_buf.size());
            size_t old = index_buf.size();
            index_buf.resize(old + 4);
            std::memcpy(index_buf.data() + old, &crc, 4);
        }
        s = backend_->Write(fh, index_offset, index_buf.data(), index_buf.size());
        if (!s.ok()) { backend_->Close(fh); return s; }
        offset += index_buf.size();

        // Write bloom filter block.
        uint64_t bloom_offset = offset;
        auto bloom_data = bloom.Serialize();
        s = backend_->Write(fh, bloom_offset, bloom_data.data(), bloom_data.size());
        if (!s.ok()) { backend_->Close(fh); return s; }
        offset += bloom_data.size();

        // Write footer.
        uint8_t footer[kFooterSize];
        std::memset(footer, 0, kFooterSize);
        std::memcpy(footer + 0, &index_offset, 8);
        uint32_t index_size = static_cast<uint32_t>(index_buf.size());
        std::memcpy(footer + 8, &index_size, 4);
        std::memcpy(footer + 12, &bloom_offset, 8);
        uint32_t bloom_size = static_cast<uint32_t>(bloom_data.size());
        std::memcpy(footer + 20, &bloom_size, 4);
        std::memcpy(footer + 24, &data_size, 8);
        std::memcpy(footer + 32, &kSSTableMagic, 8);
        uint32_t footer_crc = CRC32::Compute(footer, 40);
        std::memcpy(footer + 40, &footer_crc, 4);

        s = backend_->Write(fh, offset, footer, kFooterSize);
        if (!s.ok()) { backend_->Close(fh); return s; }

        s = backend_->Sync(fh_ = fh, false);
        backend_->Close(fh);
        fh_ = kInvalidFile;
        return s;
    }

private:
    IOBackend* backend_;
    std::string path_;
    size_t block_size_;
    FileHandle fh_ = kInvalidFile;
};

// SSTable reader: reads an SSTable file.
class SSTableReader {
public:
    SSTableReader(IOBackend* backend, std::string path)
        : backend_(backend), path_(std::move(path)) {}

    Status Open() {
        auto s = backend_->Open(path_, false, false, &fh_);
        if (!s.ok()) return s;

        uint64_t file_size;
        s = backend_->FileSize(fh_, &file_size);
        if (!s.ok()) { backend_->Close(fh_); return s; }

        if (file_size < kFooterSize) {
            backend_->Close(fh_);
            return Status::Corruption("SSTable too small");
        }

        // Read footer.
        uint8_t footer[kFooterSize];
        s = backend_->Read(fh_, file_size - kFooterSize, footer, kFooterSize);
        if (!s.ok()) { backend_->Close(fh_); return s; }

        // Verify magic.
        uint64_t magic;
        std::memcpy(&magic, footer + 32, 8);
        if (magic != kSSTableMagic) {
            backend_->Close(fh_);
            return Status::Corruption("bad SSTable magic");
        }

        // Verify footer CRC.
        uint32_t stored_crc;
        std::memcpy(&stored_crc, footer + 40, 4);
        uint32_t computed_crc = CRC32::Compute(footer, 40);
        if (stored_crc != computed_crc) {
            backend_->Close(fh_);
            return Status::Corruption("footer CRC mismatch");
        }

        std::memcpy(&index_offset_, footer + 0, 8);
        std::memcpy(&index_size_, footer + 8, 4);
        std::memcpy(&bloom_offset_, footer + 12, 8);
        std::memcpy(&bloom_size_, footer + 20, 4);
        std::memcpy(&data_size_, footer + 24, 8);

        // Load bloom filter.
        std::vector<uint8_t> bloom_data(bloom_size_);
        s = backend_->Read(fh_, bloom_offset_, bloom_data.data(), bloom_size_);
        if (!s.ok()) { backend_->Close(fh_); return s; }
        bloom_ = BloomFilter::Deserialize(bloom_data.data(), bloom_data.size());

        // Load index.
        std::vector<uint8_t> index_data(index_size_);
        s = backend_->Read(fh_, index_offset_, index_data.data(), index_size_);
        if (!s.ok()) { backend_->Close(fh_); return s; }

        // Parse index.
        if (index_data.size() < 4) {
            backend_->Close(fh_);
            return Status::Corruption("index too small");
        }
        uint32_t num_entries;
        std::memcpy(&num_entries, index_data.data(), 4);
        size_t pos = 4;
        for (uint32_t i = 0; i < num_entries; i++) {
            if (pos + 4 > index_data.size()) break;
            uint32_t klen;
            std::memcpy(&klen, index_data.data() + pos, 4); pos += 4;
            if (pos + klen + 12 > index_data.size()) break;
            std::string key(reinterpret_cast<const char*>(index_data.data() + pos), klen);
            pos += klen;
            uint64_t off;
            std::memcpy(&off, index_data.data() + pos, 8); pos += 8;
            uint32_t sz;
            std::memcpy(&sz, index_data.data() + pos, 4); pos += 4;
            index_.push_back({std::move(key), off, sz});
        }

        return Status::OK();
    }

    ~SSTableReader() {
        if (fh_ != kInvalidFile) backend_->Close(fh_);
    }

    // Point lookup. Returns nullopt if not found.
    // Sets *is_tombstone if the key was deleted.
    std::optional<std::string> Get(std::string_view key, bool* is_tombstone = nullptr) const {
        if (!bloom_.MayContain(key)) return std::nullopt;

        // Binary search in index for the block that might contain the key.
        int lo = 0, hi = static_cast<int>(index_.size()) - 1;
        int target = -1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (index_[mid].last_key >= key) {
                target = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        if (target < 0) return std::nullopt;

        // Read the block.
        auto& [last_key, block_offset, block_size] = index_[target];
        std::vector<uint8_t> block_data(block_size);
        auto s = backend_->Read(fh_, block_offset, block_data.data(), block_size);
        if (!s.ok()) return std::nullopt;

        // Verify block CRC.
        if (block_data.size() < 4) return std::nullopt;
        uint32_t stored_crc;
        std::memcpy(&stored_crc, block_data.data() + block_data.size() - 4, 4);
        uint32_t computed_crc = CRC32::Compute(block_data.data(), block_data.size() - 4);
        if (stored_crc != computed_crc) return std::nullopt;

        // Scan entries in block.
        uint32_t num_entries;
        std::memcpy(&num_entries, block_data.data(), 4);
        size_t pos = 4;
        for (uint32_t i = 0; i < num_entries; i++) {
            if (pos + 4 > block_data.size() - 4) break;
            uint32_t klen;
            std::memcpy(&klen, block_data.data() + pos, 4); pos += 4;
            if (pos + klen > block_data.size() - 4) break;
            std::string_view k(reinterpret_cast<const char*>(block_data.data() + pos), klen);
            pos += klen;
            if (pos >= block_data.size() - 4) break;
            bool tomb = block_data[pos] != 0; pos += 1;
            if (pos + 4 > block_data.size() - 4) break;
            uint32_t vlen;
            std::memcpy(&vlen, block_data.data() + pos, 4); pos += 4;
            if (pos + vlen > block_data.size() - 4) break;

            if (k == key) {
                if (is_tombstone) *is_tombstone = tomb;
                if (tomb) return std::nullopt;
                return std::string(reinterpret_cast<const char*>(block_data.data() + pos), vlen);
            }
            pos += vlen;
        }
        return std::nullopt;
    }

    // Iterate all entries in the SSTable.
    Status ForEach(const std::function<bool(const SSTableEntry&)>& cb) const {
        for (auto& [last_key, block_offset, block_size] : index_) {
            std::vector<uint8_t> block_data(block_size);
            auto s = backend_->Read(fh_, block_offset, block_data.data(), block_size);
            if (!s.ok()) return s;

            // Verify CRC.
            uint32_t stored_crc;
            std::memcpy(&stored_crc, block_data.data() + block_data.size() - 4, 4);
            uint32_t computed_crc = CRC32::Compute(block_data.data(), block_data.size() - 4);
            if (stored_crc != computed_crc) {
                return Status::Corruption("block CRC mismatch");
            }

            uint32_t num_entries;
            std::memcpy(&num_entries, block_data.data(), 4);
            size_t pos = 4;
            for (uint32_t i = 0; i < num_entries; i++) {
                SSTableEntry entry;
                uint32_t klen;
                std::memcpy(&klen, block_data.data() + pos, 4); pos += 4;
                entry.key.assign(reinterpret_cast<const char*>(block_data.data() + pos), klen);
                pos += klen;
                entry.is_tombstone = block_data[pos] != 0; pos += 1;
                uint32_t vlen;
                std::memcpy(&vlen, block_data.data() + pos, 4); pos += 4;
                entry.value.assign(reinterpret_cast<const char*>(block_data.data() + pos), vlen);
                pos += vlen;

                if (!cb(entry)) return Status::OK();
            }
        }
        return Status::OK();
    }

    [[nodiscard]] size_t num_blocks() const { return index_.size(); }

private:
    struct IndexEntry {
        std::string last_key;
        uint64_t offset;
        uint32_t size;
    };

    IOBackend* backend_;
    std::string path_;
    FileHandle fh_ = kInvalidFile;
    uint64_t index_offset_ = 0;
    uint32_t index_size_ = 0;
    uint64_t bloom_offset_ = 0;
    uint32_t bloom_size_ = 0;
    uint64_t data_size_ = 0;
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;
};

} // namespace kv
