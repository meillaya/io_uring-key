#pragma once
// 4096-aligned buffer pool for O_DIRECT I/O and io_uring registered buffers.

#include <cassert>
#include <sys/uio.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <span>
#include <vector>

namespace kv {

inline constexpr size_t kAlignment = 4096;

// Align a value up to the given alignment (must be power of 2).
inline constexpr size_t AlignUp(size_t value, size_t alignment = kAlignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

// A buffer whose data pointer is aligned to kAlignment (4096).
// Suitable for O_DIRECT I/O and io_uring registered buffers.
class AlignedBuffer {
public:
    AlignedBuffer() = default;

    explicit AlignedBuffer(size_t size)
        : size_(size), capacity_(AlignUp(size)) {
        allocate();
    }

    // Move-only
    AlignedBuffer(AlignedBuffer&& other) noexcept
        : data_(other.data_), size_(other.size_), capacity_(other.capacity_) {
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }

    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
        if (this != &other) {
            free();
            data_ = other.data_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            other.data_ = nullptr;
            other.size_ = 0;
            other.capacity_ = 0;
        }
        return *this;
    }

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    ~AlignedBuffer() { free(); }

    void Resize(size_t new_size) {
        size_t new_cap = AlignUp(new_size);
        if (new_cap > capacity_) {
            free();
            capacity_ = new_cap;
            allocate();
        }
        size_ = new_size;
    }

    void Reserve(size_t new_capacity) {
        size_t aligned = AlignUp(new_capacity);
        if (aligned > capacity_) {
            auto* old = data_;
            capacity_ = aligned;
            allocate();
            if (old) {
                std::memcpy(data_, old, size_);
                std::free(old);
            }
        }
    }

    [[nodiscard]] uint8_t* data() { return data_; }
    [[nodiscard]] const uint8_t* data() const { return data_; }
    [[nodiscard]] size_t size() const { return size_; }
    [[nodiscard]] size_t capacity() const { return capacity_; }
    [[nodiscard]] bool empty() const { return size_ == 0; }
    [[nodiscard]] bool is_aligned() const {
        return reinterpret_cast<uintptr_t>(data_) % kAlignment == 0;
    }

    std::span<uint8_t> span() { return {data_, size_}; }
    std::span<const uint8_t> span() const { return {data_, size_}; }

    uint8_t& operator[](size_t i) { return data_[i]; }
    const uint8_t& operator[](size_t i) const { return data_[i]; }

    void Append(const void* src, size_t len) {
        Reserve(size_ + len);
        std::memcpy(data_ + size_, src, len);
        size_ += len;
    }

    void Clear() { size_ = 0; }

private:
    void allocate() {
        assert(capacity_ > 0);
        assert(capacity_ % kAlignment == 0);
        data_ = static_cast<uint8_t*>(std::aligned_alloc(kAlignment, capacity_));
        assert(data_ != nullptr);
    }

    void free() {
        if (data_) {
            std::free(data_);
            data_ = nullptr;
        }
    }

    uint8_t* data_ = nullptr;
    size_t size_ = 0;
    size_t capacity_ = 0;
};

// Pool of pre-allocated aligned buffers for io_uring registered buffers.
class AlignedBufferPool {
public:
    explicit AlignedBufferPool(size_t buffer_size, size_t count = 16)
        : buffer_size_(AlignUp(buffer_size)) {
        buffers_.reserve(count);
        for (size_t i = 0; i < count; i++) {
            buffers_.emplace_back(buffer_size_);
        }
    }

    [[nodiscard]] size_t buffer_size() const { return buffer_size_; }
    [[nodiscard]] size_t count() const { return buffers_.size(); }
    [[nodiscard]] AlignedBuffer& at(size_t i) { return buffers_[i]; }
    [[nodiscard]] const AlignedBuffer& at(size_t i) const { return buffers_[i]; }

    // Get raw pointers for io_uring buffer registration.
    [[nodiscard]] std::vector<iovec> iovecs() const {
        std::vector<iovec> vecs(buffers_.size());
        for (size_t i = 0; i < buffers_.size(); i++) {
            vecs[i].iov_base = const_cast<uint8_t*>(buffers_[i].data());
            vecs[i].iov_len = buffers_[i].capacity();
        }
        return vecs;
    }

private:
    size_t buffer_size_;
    std::vector<AlignedBuffer> buffers_;
};

} // namespace kv
