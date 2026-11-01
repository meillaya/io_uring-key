#pragma once
// mmap I/O backend: memory-mapped segment files.

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

#include "io/io_backend.h"
#include "util/fs.h"

namespace kv {

class MmapBackend final : public IOBackend {
public:
    [[nodiscard]] std::string_view Name() const override { return "mmap"; }

    Status Open(std::string_view path, bool create, bool direct,
                FileHandle* out) override {
        int flags = O_RDWR;
        if (create) flags |= O_CREAT;

        std::string path_str(path);
        int fd = ::open(path_str.c_str(), flags, 0644);
        if (fd < 0) {
            return Status::IOError(std::string("open: ") + std::strerror(errno));
        }

        // Get file size for mmap.
        struct stat st;
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            return Status::IOError(std::string("fstat: ") + std::strerror(errno));
        }

        std::lock_guard lock(mu_);
        FileHandle fh = next_handle_++;
        files_[fh] = {fd, std::string(path), nullptr, 0,
                      static_cast<uint64_t>(st.st_size)};
        *out = fh;
        return Status::OK();
    }

    Status Close(FileHandle fh) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        UnmapLocked(it->second);
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
        it->second.file_size = *size;
        return Status::OK();
    }

    Status Truncate(FileHandle fh, uint64_t size) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        if (::ftruncate(it->second.fd, static_cast<off_t>(size)) != 0) {
            return Status::IOError(std::string("ftruncate: ") + std::strerror(errno));
        }
        UnmapLocked(it->second);
        it->second.file_size = size;
        return Status::OK();
    }

    Status SyncDirectory(std::string_view path) override {
        return SyncDirectoryPath(path);
    }

    Status Read(FileHandle fh, uint64_t offset, void* buf, size_t len) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");

        auto s = EnsureMappedLocked(it->second);
        if (!s.ok()) return s;

        if (offset + len > it->second.file_size) {
            return Status::IOError("read past end of file");
        }
        std::memcpy(buf, static_cast<uint8_t*>(it->second.map_base) + offset, len);
        return Status::OK();
    }

    Status Write(FileHandle fh, uint64_t offset, const void* buf, size_t len) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");

        // Extend file if needed.
        uint64_t needed = offset + len;
        if (needed > it->second.file_size) {
            if (::ftruncate(it->second.fd, static_cast<off_t>(needed)) != 0) {
                return Status::IOError(std::string("ftruncate: ") + std::strerror(errno));
            }
            // Remap.
            UnmapLocked(it->second);
            it->second.file_size = needed;
        }

        auto s = EnsureMappedLocked(it->second);
        if (!s.ok()) return s;

        std::memcpy(static_cast<uint8_t*>(it->second.map_base) + offset, buf, len);
        return Status::OK();
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

        // Extend and remap.
        uint64_t needed = off + len;
        if (::ftruncate(it->second.fd, static_cast<off_t>(needed)) != 0) {
            return Status::IOError(std::string("ftruncate: ") + std::strerror(errno));
        }
        UnmapLocked(it->second);
        it->second.file_size = needed;

        auto s = EnsureMappedLocked(it->second);
        if (!s.ok()) return s;

        std::memcpy(static_cast<uint8_t*>(it->second.map_base) + off, buf, len);
        if (offset) *offset = off;
        return Status::OK();
    }

    Status Sync(FileHandle fh, bool data_only) override {
        std::lock_guard lock(mu_);
        auto it = files_.find(fh);
        if (it == files_.end()) return Status::InvalidArgument("invalid file handle");
        if (it->second.map_base) {
            int flags = data_only ? MS_SYNC : (MS_SYNC | MS_INVALIDATE);
            if (::msync(it->second.map_base, it->second.file_size, flags) != 0) {
                return Status::IOError(std::string("msync: ") + std::strerror(errno));
            }
        }
        // Also fsync the fd for metadata.
        if (!data_only) {
            if (::fsync(it->second.fd) != 0) {
                return Status::IOError(std::string("fsync: ") + std::strerror(errno));
            }
        }
        return Status::OK();
    }

    [[nodiscard]] bool SupportsDirectIO() const override { return false; }

private:
    struct FileEntry {
        int fd;
        std::string path;
        void* map_base;
        size_t map_len;
        uint64_t file_size;
    };

    Status EnsureMappedLocked(FileEntry& entry) {
        if (entry.map_base && entry.map_len >= entry.file_size) return Status::OK();
        if (entry.file_size == 0) return Status::OK();

        UnmapLocked(entry);
        void* base = ::mmap(nullptr, entry.file_size, PROT_READ | PROT_WRITE,
                            MAP_SHARED, entry.fd, 0);
        if (base == MAP_FAILED) {
            return Status::IOError(std::string("mmap: ") + std::strerror(errno));
        }
        entry.map_base = base;
        entry.map_len = entry.file_size;
        return Status::OK();
    }

    void UnmapLocked(FileEntry& entry) {
        if (entry.map_base) {
            ::munmap(entry.map_base, entry.map_len);
            entry.map_base = nullptr;
            entry.map_len = 0;
        }
    }

    std::mutex mu_;
    std::unordered_map<FileHandle, FileEntry> files_;
    FileHandle next_handle_ = 1;
};

} // namespace kv
