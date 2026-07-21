#pragma once
// io_backend.h — FROZEN I/O backend interface contract.
//
// All four backends (sync, threadpool, mmap, io_uring) implement this interface.
// The engine, WAL, and SSTable layers interact with storage ONLY through this interface.
// This contract is FROZEN at end of G1; G3 backends implement against it unchanged.
//
// Apples-to-apples benchmark contract:
//   ONE engine, ONE memtable/SSTable/WAL/format/durability policy.
//   The four backends differ ONLY in the I/O submission/completion path.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "util/status.h"

namespace kv {

// File handle returned by Open. Backend-specific internals are hidden.
using FileHandle = uint64_t;
inline constexpr FileHandle kInvalidFile = ~0ULL;

// I/O operation type for batched submission.
enum class IOOp : uint8_t {
    kRead,
    kWrite,
    kFsync,
    kFdatasync,
};

// A single I/O request for batched submission.
struct IORequest {
    IOOp op;
    FileHandle file;
    uint64_t offset;
    void* buf;        // For read/write; nullptr for sync ops.
    size_t len;       // For read/write; 0 for sync ops.
};

// Result of a single I/O operation.
struct IOResult {
    Status status;
    ssize_t bytes_transferred = 0;  // For read/write.
};

// Completion callback for async I/O.
using IOCallback = std::function<void(const IOResult&)>;

// Abstract I/O backend interface.
//
// Thread-safety: implementations must be safe for concurrent use from
// multiple threads (the engine may issue I/O from compaction and
// foreground threads simultaneously).
class IOBackend {
public:
    virtual ~IOBackend() = default;

    // Human-readable backend name for logging/benchmarks.
    [[nodiscard]] virtual std::string_view Name() const = 0;

    // --- File lifecycle ---

    // Open or create a file. Returns a handle for subsequent operations.
    // `direct` requests O_DIRECT (alignment requirements apply).
    virtual Status Open(std::string_view path, bool create, bool direct,
                        FileHandle* out) = 0;

    // Close a file handle.
    virtual Status Close(FileHandle fh) = 0;

    // Delete a file by path.
    virtual Status Delete(std::string_view path) = 0;

    // Get file size.
    virtual Status FileSize(FileHandle fh, uint64_t* size) = 0;

    // --- Synchronous I/O ---

    // Read exactly `len` bytes at `offset` into `buf`.
    // For O_DIRECT handles, buf must be aligned and len a multiple of 4096.
    virtual Status Read(FileHandle fh, uint64_t offset, void* buf, size_t len) = 0;

    // Write `len` bytes from `buf` at `offset`.
    // For O_DIRECT handles, buf must be aligned and len a multiple of 4096.
    virtual Status Write(FileHandle fh, uint64_t offset, const void* buf, size_t len) = 0;

    // Append `len` bytes from `buf` to the file (write at current end).
    // Returns the offset at which data was written.
    virtual Status Append(FileHandle fh, const void* buf, size_t len,
                          uint64_t* offset) = 0;

    // Fsync (data + metadata) or fdatasync (data only).
    virtual Status Sync(FileHandle fh, bool data_only = false) = 0;

    // --- Batched / Async I/O (optional optimization) ---

    // Submit a batch of I/O requests. Default: execute synchronously in order.
    // io_uring backend overrides for true batched submission.
    virtual Status SubmitBatch(std::span<const IORequest> reqs,
                               std::span<IOResult> results) {
        if (reqs.size() != results.size()) {
            return Status::InvalidArgument("reqs/results size mismatch");
        }
        for (size_t i = 0; i < reqs.size(); i++) {
            results[i] = ExecuteSync(reqs[i]);
            if (!results[i].status.ok()) return results[i].status;
        }
        return Status::OK();
    }

    // --- Backend capabilities ---

    [[nodiscard]] virtual bool SupportsDirectIO() const { return false; }
    [[nodiscard]] virtual bool SupportsBatchedIO() const { return false; }
    [[nodiscard]] virtual bool SupportsRegisteredBuffers() const { return false; }
    [[nodiscard]] virtual bool SupportsFixedFiles() const { return false; }

public:
    // Synchronous execution of a single request (for default SubmitBatch).
    IOResult ExecuteSync(const IORequest& req) {
        IOResult res;
        switch (req.op) {
            case IOOp::kRead:
                res.status = Read(req.file, req.offset, req.buf, req.len);
                res.bytes_transferred = res.status.ok() ? static_cast<ssize_t>(req.len) : -1;
                break;
            case IOOp::kWrite:
                res.status = Write(req.file, req.offset, req.buf, req.len);
                res.bytes_transferred = res.status.ok() ? static_cast<ssize_t>(req.len) : -1;
                break;
            case IOOp::kFsync:
                res.status = Sync(req.file, false);
                break;
            case IOOp::kFdatasync:
                res.status = Sync(req.file, true);
                break;
        }
        return res;
    }
};

// Factory type for creating backends.
using IOBackendFactory = std::function<std::unique_ptr<IOBackend>()>;

} // namespace kv
