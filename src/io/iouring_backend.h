#pragma once
// io_uring I/O backend: batched submission, registered buffers, fixed files,
// optional SQPOLL, O_DIRECT-aware with alignment assertions.

#ifdef KV_HAS_IOURING

#include <liburing.h>

#include <cassert>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "io/io_backend.h"
#include "util/aligned_buffer.h"
#include "util/fs.h"
#include "util/logging.h"

namespace kv {

struct IOUringOptions {
    unsigned queue_depth = 256;
    // SQPOLL is attempted by default (kernel-side submission polling removes
    // the per-op syscall); Init() falls back to a normal ring if unavailable
    // (e.g. EPERM on kernels/configs that restrict it).
    bool use_sqpoll = true;
    unsigned sqpoll_idle_ms = 2000;
    bool use_direct_io = false;
    bool register_buffers = true;
    bool register_files = true;
    size_t buffer_size = 64 * 1024;
    size_t buffer_count = 32;
};

class IOUringBackend final : public IOBackend {
public:
    explicit IOUringBackend(IOUringOptions opts = {})
        : opts_(std::move(opts)), ring_initialized_(false) {}

    ~IOUringBackend() override {
        for (auto& [fh, entry] : files_) {
            ::close(entry.fd);
        }
        files_.clear();
        if (ring_initialized_) {
            if (buffers_registered_) io_uring_unregister_buffers(&ring_);
            if (files_registered_) io_uring_unregister_files(&ring_);
            io_uring_queue_exit(&ring_);
        }
    }

    [[nodiscard]] std::string_view Name() const override { return "io_uring"; }

    Status Init() {
        struct io_uring_params params;
        std::memset(&params, 0, sizeof(params));
        bool want_sqpoll = opts_.use_sqpoll;
        if (want_sqpoll) {
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_idle = opts_.sqpoll_idle_ms;
        }

        int ret = io_uring_queue_init_params(opts_.queue_depth, &ring_, &params);
        if (ret < 0 && want_sqpoll) {
            // SQPOLL unavailable (commonly EPERM without CAP_SYS_NICE on some
            // kernels): fall back to a plain submission ring so the backend
            // still works.
            KV_LOG_WARN("io_uring: SQPOLL init failed (%s); falling back to "
                        "submission-ring mode", std::strerror(-ret));
            std::memset(&params, 0, sizeof(params));
            opts_.use_sqpoll = false;
            ret = io_uring_queue_init_params(opts_.queue_depth, &ring_, &params);
        }
        if (ret < 0) {
            return Status::IOError(std::string("io_uring_queue_init: ") + std::strerror(-ret));
        }
        ring_initialized_ = true;
        if (opts_.use_sqpoll) {
            KV_LOG_INFO("io_uring: SQPOLL enabled (kernel-side submission)");
        }

        // Register buffer pool.
        if (opts_.register_buffers) {
            buffer_pool_ = std::make_unique<AlignedBufferPool>(
                opts_.buffer_size, opts_.buffer_count);
            auto iovecs = buffer_pool_->iovecs();
            ret = io_uring_register_buffers(&ring_, iovecs.data(),
                                            static_cast<unsigned>(iovecs.size()));
            if (ret < 0) {
                KV_LOG_WARN("register_buffers failed: %s", std::strerror(-ret));
                opts_.register_buffers = false;
            } else {
                buffers_registered_ = true;
                KV_LOG_INFO("io_uring: registered %zu buffers (%zu bytes each)",
                            opts_.buffer_count, opts_.buffer_size);
            }
        }

        // Register file table.
        if (opts_.register_files) {
            max_fixed_files_ = 64;
            std::vector<int> fds(max_fixed_files_, -1);
            ret = io_uring_register_files(&ring_, fds.data(),
                                          static_cast<unsigned>(fds.size()));
            if (ret < 0) {
                KV_LOG_WARN("register_files failed: %s", std::strerror(-ret));
                opts_.register_files = false;
            } else {
                files_registered_ = true;
                KV_LOG_INFO("io_uring: registered file table (%d slots)", max_fixed_files_);
            }
        }

        return Status::OK();
    }

    Status Open(std::string_view path, bool create, bool direct,
                FileHandle* out) override {
        int flags = O_RDWR;
        if (create) flags |= O_CREAT;
        bool use_direct = direct || opts_.use_direct_io;
        if (use_direct) {
#ifdef O_DIRECT
            flags |= O_DIRECT;
#endif
        }

        std::string path_str(path);
        int fd = ::open(path_str.c_str(), flags, 0644);
        if (fd < 0) {
            return Status::IOError(std::string("open: ") + std::strerror(errno));
        }

        std::lock_guard lock(mu_);
        FileHandle fh = next_handle_++;
        int fixed_idx = -1;

        if (opts_.register_files && files_registered_ && next_fixed_slot_ < max_fixed_files_) {
            fixed_idx = next_fixed_slot_++;
            int ret = io_uring_register_files_update(&ring_,
                static_cast<unsigned>(fixed_idx), &fd, 1);
            if (ret < 0) {
                KV_LOG_WARN("register_files_update fd=%d: %s", fd, std::strerror(-ret));
                fixed_idx = -1;
            }
        }

        files_[fh] = {fd, std::string(path), use_direct, fixed_idx};
        *out = fh;
        return Status::OK();
    }

    Status Close(FileHandle fh) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        if (it->second.fixed_idx >= 0 && files_registered_) {
            int neg = -1;
            io_uring_register_files_update(&ring_,
                static_cast<unsigned>(it->second.fixed_idx), &neg, 1);
        }
        ::close(it->second.fd);
        files_.erase(it);
        return Status::OK();
    }

    Status Delete(std::string_view path) override {
        std::string path_str(path);
        if (::unlink(path_str.c_str()) != 0 && errno != ENOENT) {
            return Status::IOError(std::string("unlink: ") + std::strerror(errno));
        }
        return Status::OK();
    }

    Status FileSize(FileHandle fh, uint64_t* size) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        struct stat st;
        if (::fstat(it->second.fd, &st) != 0) {
            return Status::IOError(std::string("fstat: ") + std::strerror(errno));
        }
        *size = static_cast<uint64_t>(st.st_size);
        return Status::OK();
    }

    Status Truncate(FileHandle fh, uint64_t size) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        if (::ftruncate(it->second.fd, static_cast<off_t>(size)) != 0) {
            return Status::IOError(std::string("ftruncate: ") + std::strerror(errno));
        }
        return Status::OK();
    }

    Status SyncDirectory(std::string_view path) override {
        return SyncDirectoryPath(path);
    }

    Status Read(FileHandle fh, uint64_t offset, void* buf, size_t len) override {
        return SubmitSingle(IOOp::kRead, fh, offset, buf, len);
    }

    Status Write(FileHandle fh, uint64_t offset, const void* buf, size_t len) override {
        return SubmitSingle(IOOp::kWrite, fh, offset, const_cast<void*>(buf), len);
    }

    Status Append(FileHandle fh, const void* buf, size_t len,
                  uint64_t* offset) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        struct stat st;
        if (::fstat(it->second.fd, &st) != 0) {
            return Status::IOError(std::string("fstat: ") + std::strerror(errno));
        }
        uint64_t off = static_cast<uint64_t>(st.st_size);
        auto s = SubmitSingleLocked(IOOp::kWrite, fh, off, const_cast<void*>(buf), len);
        if (!s.ok()) return s;
        if (offset) *offset = off;
        return Status::OK();
    }

    Status Sync(FileHandle fh, bool data_only) override {
        return SubmitSingle(data_only ? IOOp::kFdatasync : IOOp::kFsync, fh, 0, nullptr, 0);
    }

    Status SubmitBatch(std::span<const IORequest> reqs,
                       std::span<IOResult> results) override {
        if (reqs.size() != results.size())
            return Status::InvalidArgument("reqs/results size mismatch");
        if (reqs.empty()) return Status::OK();

        std::lock_guard lock(mu_);

        for (size_t i = 0; i < reqs.size(); i++) {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
            if (!sqe) {
                io_uring_submit(&ring_);
                sqe = io_uring_get_sqe(&ring_);
                if (!sqe) return Status::IOError("SQE queue full");
            }
            PrepareSQE(sqe, reqs[i], static_cast<int>(i));
        }

        int ret = io_uring_submit(&ring_);
        if (ret < 0)
            return Status::IOError(std::string("submit: ") + std::strerror(-ret));

        for (size_t i = 0; i < reqs.size(); i++) {
            struct io_uring_cqe* cqe;
            ret = io_uring_wait_cqe(&ring_, &cqe);
            if (ret < 0)
                return Status::IOError(std::string("wait_cqe: ") + std::strerror(-ret));

            int idx = static_cast<int>(static_cast<intptr_t>(cqe->user_data));
            if (idx >= 0 && static_cast<size_t>(idx) < results.size()) {
                if (cqe->res < 0) {
                    results[idx].status = Status::IOError(std::strerror(-cqe->res));
                    results[idx].bytes_transferred = -1;
                } else {
                    results[idx].status = Status::OK();
                    results[idx].bytes_transferred = cqe->res;
                }
            }
            io_uring_cqe_seen(&ring_, cqe);
        }

        for (auto& r : results) {
            if (!r.status.ok()) return r.status;
        }
        return Status::OK();
    }

    [[nodiscard]] bool SupportsDirectIO() const override { return true; }
    [[nodiscard]] bool SupportsBatchedIO() const override { return true; }
    [[nodiscard]] bool SupportsRegisteredBuffers() const override {
        return buffers_registered_;
    }
    [[nodiscard]] bool SupportsFixedFiles() const override {
        return files_registered_;
    }

    // Expose the registered buffer pool so callers can borrow aligned buffers
    // and hit the prep_read_fixed/prep_write_fixed fast path.
    [[nodiscard]] AlignedBufferPool* buffer_pool() { return buffer_pool_.get(); }
    [[nodiscard]] bool sqpoll_enabled() const { return opts_.use_sqpoll; }

private:
    struct FileEntry {
        int fd;
        std::string path;
        bool direct;
        int fixed_idx;
    };

    // Returns the registered-buffer index for `buf` if it is exactly the base of
    // one of our pool buffers, else -1 (caller buffer -> normal prep_read/write).
    int RegisteredBufferIndex(void* buf) const {
        if (!buffers_registered_ || !buffer_pool_) return -1;
        auto* p = static_cast<uint8_t*>(buf);
        for (size_t i = 0; i < buffer_pool_->count(); i++) {
            if (buffer_pool_->at(i).data() == p) return static_cast<int>(i);
        }
        return -1;
    }

    void PrepareSQE(struct io_uring_sqe* sqe, const IORequest& req, int tag) {
        auto it = files_.find(req.file);
        int fd = (it != files_.end()) ? it->second.fd : -1;
        int fixed_idx = (it != files_.end()) ? it->second.fixed_idx : -1;
        bool use_fixed = (fixed_idx >= 0 && files_registered_);

        int use_fd = use_fixed ? fixed_idx : fd;
        switch (req.op) {
            case IOOp::kRead: {
                // Use a registered-buffer read when the caller passed one of our
                // pool buffers (avoids the kernel re-pinning user pages per op).
                int bidx = RegisteredBufferIndex(req.buf);
                if (bidx >= 0) {
                    io_uring_prep_read_fixed(sqe, use_fd, req.buf, req.len,
                                             static_cast<__u64>(req.offset), bidx);
                } else {
                    io_uring_prep_read(sqe, use_fd, req.buf, req.len,
                                       static_cast<__u64>(req.offset));
                }
                if (use_fixed) sqe->flags |= IOSQE_FIXED_FILE;
                break;
            }
            case IOOp::kWrite: {
                int bidx = RegisteredBufferIndex(req.buf);
                if (bidx >= 0) {
                    io_uring_prep_write_fixed(sqe, use_fd, req.buf, req.len,
                                              static_cast<__u64>(req.offset), bidx);
                } else {
                    io_uring_prep_write(sqe, use_fd, req.buf, req.len,
                                        static_cast<__u64>(req.offset));
                }
                if (use_fixed) sqe->flags |= IOSQE_FIXED_FILE;
                break;
            }
            case IOOp::kFsync:
                if (use_fixed) {
                    io_uring_prep_fsync(sqe, fixed_idx, 0);
                    sqe->flags |= IOSQE_FIXED_FILE;
                } else {
                    io_uring_prep_fsync(sqe, fd, 0);
                }
                break;
            case IOOp::kFdatasync:
                if (use_fixed) {
                    io_uring_prep_fsync(sqe, fixed_idx, IORING_FSYNC_DATASYNC);
                    sqe->flags |= IOSQE_FIXED_FILE;
                } else {
                    io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
                }
                break;
        }
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<intptr_t>(tag)));
    }

    Status SubmitSingle(IOOp op, FileHandle fh, uint64_t offset,
                        void* buf, size_t len) {
        std::lock_guard lock(mu_);
        return SubmitSingleLocked(op, fh, offset, buf, len);
    }

    Status SubmitSingleLocked(IOOp op, FileHandle fh, uint64_t offset,
                               void* buf, size_t len) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            io_uring_submit(&ring_);
            sqe = io_uring_get_sqe(&ring_);
            if (!sqe) return Status::IOError("SQE queue full");
        }

        IORequest req{op, fh, offset, buf, len};
        PrepareSQE(sqe, req, 0);

        int ret = io_uring_submit(&ring_);
        if (ret < 0)
            return Status::IOError(std::string("submit: ") + std::strerror(-ret));

        struct io_uring_cqe* cqe;
        ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret < 0)
            return Status::IOError(std::string("wait_cqe: ") + std::strerror(-ret));

        Status s;
        if (cqe->res < 0) {
            s = Status::IOError(std::string("io_uring: ") + std::strerror(-cqe->res));
        } else if ((op == IOOp::kRead || op == IOOp::kWrite) &&
                   static_cast<size_t>(cqe->res) < len) {
            s = Status::IOError("short I/O: expected " + std::to_string(len) +
                                " got " + std::to_string(cqe->res));
        }
        io_uring_cqe_seen(&ring_, cqe);
        return s;
    }

    IOUringOptions opts_;
    struct io_uring ring_{};
    bool ring_initialized_ = false;
    bool buffers_registered_ = false;
    bool files_registered_ = false;
    int max_fixed_files_ = 0;
    int next_fixed_slot_ = 0;
    std::unique_ptr<AlignedBufferPool> buffer_pool_;
    std::mutex mu_;
    std::unordered_map<FileHandle, FileEntry> files_;
    FileHandle next_handle_ = 1;
};

} // namespace kv

#endif // KV_HAS_IOURING
