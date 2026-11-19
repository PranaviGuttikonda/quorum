#pragma once

// Given: outside the simulator, FileStorage runs on real files.
// Approach: FileSystem over one existing directory, using plain POSIX calls.
//           Each call opens, acts and closes, so no descriptor outlives a
//           call. That is simple, and fast enough for a node that syncs once
//           per batch.
//   - fsync uses fcntl(F_FULLFSYNC) on Apple platforms, because fsync() there
//     only reaches the drive's cache. If the file system doesn't support it,
//     it falls back to fsync().
//   - Failures throw std::system_error naming the call and the path.

#include <string>

#include "quorum/storage/filesystem.h"

namespace quorum::storage {

class PosixFileSystem final : public FileSystem {
public:
    // `dir` must already exist.
    explicit PosixFileSystem(std::string dir);

    std::optional<std::string> read(const std::string& name) override;
    void create(const std::string& name) override;
    void append(const std::string& name, std::string_view data) override;
    void truncate(const std::string& name, std::uint64_t size) override;
    void fsync(const std::string& name) override;
    void rename(const std::string& from, const std::string& to) override;
    void remove(const std::string& name) override;
    void fsync_dir() override;

    const std::string& dir() const { return dir_; }

private:
    std::string path(const std::string& name) const;

    std::string dir_;
};

}  // namespace quorum::storage
