#pragma once

// Given: durable storage has to run on real files, and also on a simulated
//        disk whose crash behavior is driven by the seed.
// Approach: the smallest file interface FileStorage needs: one flat
//           directory of named files, whole-file reads, appends and
//           truncation, and explicit durability.
//   - Nothing is durable until fsync(name) for a file's contents, and
//     fsync_dir() for which names exist (create, rename, remove). That is
//     the weakest guarantee POSIX gives, and SimDisk enforces exactly it.
//   - Errors throw. A FileSystem that loses power (SimDisk) throws on every
//     call until it is powered on again.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace quorum::storage {

class FileSystem {
public:
    virtual ~FileSystem() = default;

    // The whole file, or nullopt if `name` doesn't exist.
    virtual std::optional<std::string> read(const std::string& name) = 0;
    // Creates `name` empty, or truncates it to empty if it exists.
    virtual void create(const std::string& name) = 0;
    // Appends to an existing file.
    virtual void append(const std::string& name, std::string_view data) = 0;
    // Shrinks an existing file to `size` bytes. Throws if it is shorter.
    virtual void truncate(const std::string& name, std::uint64_t size) = 0;
    // Makes the contents of `name` durable.
    virtual void fsync(const std::string& name) = 0;
    // Atomically replaces `to` (if any) with `from`.
    virtual void rename(const std::string& from, const std::string& to) = 0;
    // Removes `name` if it exists.
    virtual void remove(const std::string& name) = 0;
    // Makes the directory's names durable.
    virtual void fsync_dir() = 0;
};

}  // namespace quorum::storage
