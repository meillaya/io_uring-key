#pragma once
// POSIX directory durability helpers shared by the I/O backends.

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>

#include "util/status.h"

namespace kv {

// fsync a directory so creates, renames, and unlinks inside it survive a
// power loss. Linux requires O_DIRECTORY for a directory fd.
inline Status SyncDirectoryPath(std::string_view path) {
    std::string owned(path);
    int fd = ::open(owned.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return Status::IOError("open dir for fsync: " + owned + ": " +
                               std::strerror(errno));
    }

    int rc = ::fsync(fd);
    int error = errno;
    ::close(fd);
    if (rc != 0) {
        return Status::IOError("fsync dir: " + owned + ": " +
                               std::strerror(error));
    }
    return Status::OK();
}

} // namespace kv
