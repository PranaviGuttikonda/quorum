#pragma once

// Given: crash recovery is only tested if crashes can lose exactly what a
//        real disk may lose, reproducibly from a seed.
// Approach: an in-memory FileSystem that tracks, for every file, its durable
//           contents and the unsynced operations since its last fsync, and
//           separately the durable set of names.
//   - fsync(name) makes the file's contents durable. fsync_dir() makes the
//     current names durable (create, rename and remove are otherwise
//     undone by a crash).
//   - power_loss(): names revert to the last fsync_dir(). Each surviving
//     file keeps its durable contents plus a seed-chosen prefix of its
//     unsynced operations, in order: k of n survive whole, k uniform in
//     [0, n]. The first one that doesn't survive whole, if it is an append,
//     survives torn (a strict, non-empty prefix of its bytes) with
//     probability torn_write. Everything else is lost.
//   - crash_after(n): n more mutating calls succeed, and the next loses
//     power partway: an append, truncate, create, rename or remove is
//     issued (so it may survive, or be torn), and an fsync or fsync_dir
//     doesn't complete. That call throws DiskCrash.
//   - While powered off every call throws DiskCrash. power_on() restores
//     service.
//   - Draws come from the disk's own Rng, and only at power loss.

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "quorum/sim/rng.h"
#include "quorum/storage/filesystem.h"

namespace quorum::sim {

// Thrown by a SimDisk that loses power (or has lost it).
struct DiskCrash : std::runtime_error {
    DiskCrash() : std::runtime_error("simulated power loss") {}
};

struct SimDiskConfig {
    double torn_write = 0.5;
};

struct SimDiskStats {
    std::uint64_t power_losses = 0;
    std::uint64_t mid_call_crashes = 0;  // power losses from crash_after()
    std::uint64_t unsynced_ops_kept = 0;
    std::uint64_t unsynced_ops_lost = 0;  // including torn ones
    std::uint64_t torn_writes = 0;
    std::uint64_t fsyncs = 0;
    std::uint64_t dir_fsyncs = 0;
};

class SimDisk final : public storage::FileSystem {
public:
    explicit SimDisk(std::uint64_t seed, SimDiskConfig config = {});

    std::optional<std::string> read(const std::string& name) override;
    void create(const std::string& name) override;
    void append(const std::string& name, std::string_view data) override;
    void truncate(const std::string& name, std::uint64_t size) override;
    void fsync(const std::string& name) override;
    void rename(const std::string& from, const std::string& to) override;
    void remove(const std::string& name) override;
    void fsync_dir() override;

    // Simulated power loss, as described above. Does nothing while off.
    void power_loss();
    void power_on();
    bool powered() const { return powered_; }

    // The next mutating call after `n` more loses power partway through.
    void crash_after(std::uint64_t n) { crash_countdown_ = n; }
    void disarm() { crash_countdown_.reset(); }
    bool armed() const { return crash_countdown_.has_value(); }

    // Media corruption, for tests: flips one bit of `name` in both its current
    // and durable contents. Throws if the byte doesn't exist in both.
    void flip_bit(const std::string& name, std::uint64_t byte, unsigned bit);
    // Unsynced operations pending on `name` (0 if it doesn't exist).
    std::size_t unsynced_ops(const std::string& name) const;

    const SimDiskStats& stats() const { return stats_; }

private:
    struct Op {
        bool append = true;
        std::string data;        // append
        std::uint64_t size = 0;  // truncate
    };
    struct Inode {
        std::string durable;
        std::string current;
        std::vector<Op> pending;
    };

    void check_powered() const;
    // Counts one mutating call. Returns true if this call is the one that
    // loses power partway.
    bool crash_now();
    [[noreturn]] void crash_mid_call();
    Inode& inode(const std::string& name);
    void issue(Inode& f, Op op);

    Rng rng_;
    SimDiskConfig config_;
    bool powered_ = true;
    std::optional<std::uint64_t> crash_countdown_;
    std::uint64_t next_inode_ = 1;
    std::map<std::uint64_t, Inode> inodes_;
    std::map<std::string, std::uint64_t> names_;
    std::map<std::string, std::uint64_t> durable_names_;
    SimDiskStats stats_;
};

}  // namespace quorum::sim
