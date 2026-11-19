#pragma once

// Given: the fuzz suites should also run every node on FileStorage over a
//        SimDisk, where a crash loses whatever wasn't synced.
// Approach: a StorageProvider with one SimDisk per node, created on first
//           boot from a stream forked off the seed (nodes first boot in id
//           order, so this is reproducible). Every boot runs recovery on a
//           fresh FileStorage. Every crash is a power loss.
//           arm() schedules a power loss partway through one of a node's next
//           few disk calls, which is how a crash lands between a write and
//           its fsync, or inside the atomic term/vote update.

#include <cstdint>
#include <map>
#include <memory>

#include "quorum/raft/cluster.h"
#include "quorum/raft/file_storage.h"
#include "quorum/sim/disk.h"
#include "quorum/sim/rng.h"

namespace quorum::testing {

struct DiskTotals {
    std::uint64_t power_losses = 0;
    std::uint64_t mid_call_crashes = 0;
    std::uint64_t unsynced_ops_lost = 0;
    std::uint64_t unsynced_ops_kept = 0;
    std::uint64_t torn_writes = 0;
    std::uint64_t recoveries = 0;
    std::uint64_t torn_tails = 0;  // recoveries that truncated a torn tail
    std::uint64_t fsyncs = 0;

    DiskTotals& operator+=(const DiskTotals& o) {
        power_losses += o.power_losses;
        mid_call_crashes += o.mid_call_crashes;
        unsynced_ops_lost += o.unsynced_ops_lost;
        unsynced_ops_kept += o.unsynced_ops_kept;
        torn_writes += o.torn_writes;
        recoveries += o.recoveries;
        torn_tails += o.torn_tails;
        fsyncs += o.fsyncs;
        return *this;
    }
};

class SimDisks final : public raft::StorageProvider {
public:
    explicit SimDisks(std::uint64_t seed, sim::SimDiskConfig config = {})
        : rng_(seed ^ 0xd15cd15cd15cd15cULL), config_(config) {}

    raft::Storage& boot(raft::NodeId id) override {
        sim::SimDisk& d = disk(id);
        d.power_on();
        storage_.erase(id);
        auto s = std::make_unique<raft::FileStorage>(d);  // may throw DiskCrash
        ++recoveries_;
        if (s->recovery().torn_bytes > 0) ++torn_tails_;
        return *(storage_[id] = std::move(s));
    }

    void crash(raft::NodeId id) override {
        storage_.erase(id);
        disk(id).power_loss();
    }

    sim::SimDisk& disk(raft::NodeId id) {
        auto& d = disks_[id];
        if (!d) d = std::make_unique<sim::SimDisk>(rng_.fork().next_u64(), config_);
        return *d;
    }

    // Power loss partway through one of `id`'s next `window` disk calls.
    void arm(raft::NodeId id, sim::Rng& rng, std::uint64_t window = 8) {
        disk(id).crash_after(rng.uniform(0, window - 1));
    }

    void disarm_all() {
        for (auto& [id, d] : disks_) d->disarm();
    }

    DiskTotals totals() const {
        DiskTotals t;
        for (const auto& [id, d] : disks_) {
            const auto& s = d->stats();
            t.power_losses += s.power_losses;
            t.mid_call_crashes += s.mid_call_crashes;
            t.unsynced_ops_lost += s.unsynced_ops_lost;
            t.unsynced_ops_kept += s.unsynced_ops_kept;
            t.torn_writes += s.torn_writes;
            t.fsyncs += s.fsyncs;
        }
        t.recoveries = recoveries_;
        t.torn_tails = torn_tails_;
        return t;
    }

private:
    sim::Rng rng_;
    sim::SimDiskConfig config_;
    std::map<raft::NodeId, std::unique_ptr<sim::SimDisk>> disks_;
    std::map<raft::NodeId, std::unique_ptr<raft::FileStorage>> storage_;
    std::uint64_t recoveries_ = 0;
    std::uint64_t torn_tails_ = 0;
};

}  // namespace quorum::testing
