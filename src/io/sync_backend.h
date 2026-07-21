#pragma once
// Sync I/O backend: pread/pwrite + fsync. The baseline backend.

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "io/io_backend.h"
#include "util/logging.h"

namespace kv {

class SyncBackend final : public IOBackend {
public:
    [[nodiscard]] std::string_view Name() const override { return "sync"; }

    Status Open(std::string_view path, bool create, bool direct,
                FileHandle* out) override {
        int flags = O_RDWR;
        if (create) flags |= O_CREAT;
        if (direct) {
#ifdef O_DIRECT
            flags |= O_DIRECT;
#else
            return Status::NotSupported("O_DIRECT not available");
#endif
        }

        std::string path_str(path);
        int fd = ::open(path_str.c_str(), flags, 0644);
        if (fd < 0) {
            return Status::IOError(std::string("open failed: ") + std::strerror(errno));
        }

        std::lock_guard lock(mu_);
        FileHandle fh = next_handle_++;
        files_[fh] = {fd, std::string(path), direct};
        *out = fh;
        return Status::OK();
    }

    Status Close(FileHandle fh) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        ::close(it->second.fd);
        files_.erase(it);
        return Status::OK();
    }

    Status Delete(std::string_view path) override {
        std::string path_str(path);
        if (::unlink(path_str.c_str()) != 0 && errno != ENOENT) {
            return Status::IOError(std::string("unlink failed: ") + std::strerror(errno));
        }
        return Status::OK();
    }

    Status FileSize(FileHandle fh, uint64_t* size) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        struct stat st;
        if (::fstat(it->second.fd, &st) != 0) {
            return Status::IOError(std::string("fstat failed: ") + std::strerror(errno));
        }
        *size = static_cast<uint64_t>(st.st_size);
        return Status::OK();
    }

    Status Read(FileHandle fh, uint64_t offset, void* buf, size_t len) override {
        int fd = GetFd(fh);
        if (fd < 0) return Status::InvalidArgument("invalid file handle");

        size_t total = 0;
        auto* p = static_cast<uint8_t*>(buf);
        while (total < len) {
            ssize_t n = ::pread(fd, p + total, len - total,
                                static_cast<off_t>(offset + total));
            if (n < 0) {
                if (errno == EINTR) continue;
                return Status::IOError(std::string("pread failed: ") + std::strerror(errno));
            }
            if (n == 0) break;  // EOF
            total += static_cast<size_t>(n);
        }
        if (total < len) {
            return Status::IOError("short read: expected " + std::to_string(len) +
                                   " got " + std::to_string(total));
        }
        return Status::OK();
    }

    Status Write(FileHandle fh, uint64_t offset, const void* buf, size_t len) override {
        int fd = GetFd(fh);
        if (fd < 0) return Status::InvalidArgument("invalid file handle");

        size_t total = 0;
        auto* p = static_cast<const uint8_t*>(buf);
        while (total < len) {
            ssize_t n = ::pwrite(fd, p + total, len - total,
                                 static_cast<off_t>(offset + total));
            if (n < 0) {
                if (errno == EINTR) continue;
                return Status::IOError(std::string("pwrite failed: ") + std::strerror(errno));
            }
            total += static_cast<size_t>(n);
        }
        return Status::OK();
    }

    Status Append(FileHandle fh, const void* buf, size_t len,
                  uint64_t* offset) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");

        // Get current file size as the append offset.
        struct stat st;
        if (::fstat(it->second.fd, &st) != 0) {
            return Status::IOError(std::string("fstat failed: ") + std::strerror(errno));
        }
        uint64_t off = static_cast<uint64_t>(st.st_size);

        // Write at the end.
        size_t total = 0;
        auto* p = static_cast<const uint8_t*>(buf);
        while (total < len) {
            ssize_t n = ::pwrite(it->second.fd, p + total, len - total,
                                 static_cast<off_t>(off + total));
            if (n < 0) {
                if (errno == EINTR) continue;
                return Status::IOError(std::string("pwrite append failed: ") + std::strerror(errno));
            }
            total += static_cast<size_t>(n);
        }
        if (offset) *offset = off;
        return Status::OK();
    }

    Status Sync(FileHandle fh, bool data_only) override {
        int fd = GetFd(fh);
        if (fd < 0) return Status::InvalidArgument("invalid file handle");
        int ret = data_only ? ::fdatasync(fd) : ::fsync(fd);
        if (ret != 0) {
            return Status::IOError(std::string("sync failed: ") + std::strerror(errno));
        }
        return Status::OK();
    }

    [[nodiscard]] bool SupportsDirectIO() const override { return true; }

private:
    struct FileEntry {
        int fd;
        std::string path;
        bool direct;
    };

    int GetFd(FileHandle fh) {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        return it != files_.end() ? it->second.fd : -1;
    }

    std::mutex mu_;
    std::unordered_map<FileHandle, FileEntry> files_;
    FileHandle next_handle_ = 1;
};

} // namespace kv
