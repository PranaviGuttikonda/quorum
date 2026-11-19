#pragma once

// Given: Raft's persistent state (Figure 2) must survive power loss, and a
//        crash may leave the last write torn.
// Approach: two files in one directory.
//   - "log": append-only records, one LogEntry each. A record is
//         u32 length | u32 crc32(length) | u32 crc32(payload) | payload
//     (little-endian). The length has its own checksum, so a flipped bit in a
//     length reads as corruption instead of a record running past the end.
//     Conflict truncation cuts the file at a record boundary. Appends are
//     buffered until sync(), so one step's entries share one fsync.
//   - "meta": one record holding currentTerm and votedFor, replaced
//     atomically: write "meta.tmp", fsync it, rename it over "meta", fsync
//     the directory. A crash leaves either the old or the new file.
//   - Recovery (the constructor) reads records until the first bad one. A
//     bad record is the torn tail of the last write if nothing follows it:
//     its header or payload runs past the end of the file, or it is the
//     final record and its payload checksum fails. That tail is truncated
//     and synced. Anything else (a bad length check anywhere, a bad record
//     with bytes after it, an undecodable payload, a damaged meta file) is
//     corruption of data that was synced, and throws StorageCorruption.
//     Skipping it could silently drop committed entries.
//   - After any exception from a write, the object must be discarded and
//     the files recovered by a new FileStorage.
//   - No compaction or snapshots (§7): the log grows without bound.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "quorum/raft/storage.h"
#include "quorum/storage/filesystem.h"

namespace quorum::raft {

struct StorageCorruption : std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct RecoveryReport {
    bool created = false;          // there was no log yet
    std::uint64_t records = 0;     // entries recovered
    std::uint64_t torn_bytes = 0;  // tail bytes truncated
};

class FileStorage final : public Storage {
public:
    static constexpr const char* kLog = "log";
    static constexpr const char* kMeta = "meta";
    static constexpr const char* kMetaTmp = "meta.tmp";

    // Recovers from `fs`, creating an empty log if there is none. Throws
    // StorageCorruption as described above.
    explicit FileStorage(storage::FileSystem& fs);

    PersistentState load() const override { return state_; }
    void save_term_and_vote(Term term, std::optional<NodeId> voted_for) override;
    void write_log(LogIndex keep, std::span<const LogEntry> entries) override;
    void sync() override;

    const RecoveryReport& recovery() const { return recovery_; }
    bool has_unsynced_writes() const { return dirty_; }

private:
    storage::FileSystem& fs_;
    PersistentState state_;
    std::vector<std::uint64_t> offsets_;  // offsets_[i]: where entry i + 1 starts
    std::uint64_t end_ = 0;               // log file size
    bool dirty_ = false;
    RecoveryReport recovery_;
};

// Record framing, exposed for tests.
std::string encode_record(std::string_view payload);

}  // namespace quorum::raft
