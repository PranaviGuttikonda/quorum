// Given: Election Safety must hold under any mix of faults, not just scripted
//        ones.
// Approach: for each of 1,000 seeds, run a 5-node cluster with 10% message
//           loss and a seed-driven schedule of crashes, restarts, partitions
//           and heals. ElectionSafetyChecker runs after every tick. Then clear
//           all faults and require the cluster to settle on one leader
//           (liveness). Any failure reports its seed, so it can be replayed.

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/raft/cluster.h"
#include "quorum/raft/invariants.h"
#include "quorum/sim/rng.h"

using namespace quorum::raft;
using quorum::sim::Rng;

namespace {

constexpr std::uint64_t kSeeds = 1000;
constexpr Tick kFaultTicks = 4000;
constexpr Tick kSettleBound = 5000;
constexpr std::size_t kNodes = 5;
constexpr std::size_t kMaxDown = 2;

// Given: std::shuffle is implementation-defined, so its output differs across
//        standard libraries.
// Approach: Fisher-Yates driven by our portable Rng.
void shuffle(std::vector<NodeId>& v, Rng& rng) {
    for (std::size_t i = v.size(); i > 1; --i) {
        const auto j = static_cast<std::size_t>(rng.uniform(0, i - 1));
        std::swap(v[i - 1], v[j]);
    }
}

struct FuzzResult {
    bool safe = true;
    bool settled = false;
    Tick settle_ticks = 0;
    std::string violation;
    std::size_t terms_with_leader = 0;
    std::uint64_t crashes = 0;
    std::uint64_t partitions = 0;
};

FuzzResult run_seed(std::uint64_t seed) {
    RaftCluster c(seed, kNodes, {}, {.min_delay = 1, .max_delay = 10, .drop_probability = 0.1});
    ElectionSafetyChecker chk;
    c.add_observer([&chk](const RaftCluster& cl) { chk.check(cl); });

    // A separate stream for faults, so the fault schedule doesn't depend on
    // how many draws the cluster made.
    Rng faults(seed ^ 0xfa017fa017fa017fULL);
    bool partitioned = false;
    FuzzResult r;

    for (Tick t = 0; t < kFaultTicks; ++t) {
        std::vector<NodeId> up, down;
        for (NodeId id : c.ids()) (c.is_up(id) ? up : down).push_back(id);

        if (down.size() < kMaxDown && faults.bernoulli(0.004)) {
            c.crash(up[faults.uniform(0, up.size() - 1)]);
            ++r.crashes;
        }
        for (NodeId id : down)
            if (faults.bernoulli(0.01)) c.restart(id);

        if (!partitioned && faults.bernoulli(0.002)) {
            std::vector<NodeId> ids = c.ids();
            shuffle(ids, faults);
            const auto cut = static_cast<std::ptrdiff_t>(faults.uniform(1, kNodes - 1));
            c.partition({{ids.begin(), ids.begin() + cut}, {ids.begin() + cut, ids.end()}});
            partitioned = true;
            ++r.partitions;
        } else if (partitioned && faults.bernoulli(0.004)) {
            c.heal();
            partitioned = false;
        }
        c.tick();
    }

    // Liveness: with every fault cleared (loss stays at 10%), one leader must
    // emerge that every node follows.
    c.heal();
    for (NodeId id : c.ids()) c.restart(id);
    const Tick settle_start = c.now();
    r.settled = c.run_until([](const RaftCluster& cl) { return cl.stable_leader().has_value(); },
                            kSettleBound);
    r.settle_ticks = c.now() - settle_start;

    r.safe = chk.ok();
    if (!r.safe) r.violation = *chk.violation();
    r.terms_with_leader = chk.leaders_by_term().size();
    return r;
}

}  // namespace

TEST(election_fuzz_safety_across_1000_seeds) {
    std::uint64_t total_crashes = 0, total_partitions = 0, total_terms = 0;
    Tick worst_settle = 0;
    for (std::uint64_t seed = 1; seed <= kSeeds; ++seed) {
        const FuzzResult r = run_seed(seed);
        if (!r.safe)
            quorum::test::fail(__FILE__, __LINE__, "seed " + std::to_string(seed) + ": " + r.violation);
        if (!r.settled)
            quorum::test::fail(__FILE__, __LINE__,
                               "seed " + std::to_string(seed) + ": no stable leader within " +
                                   std::to_string(kSettleBound) + " ticks after faults cleared");
        total_crashes += r.crashes;
        total_partitions += r.partitions;
        total_terms += r.terms_with_leader;
        worst_settle = std::max(worst_settle, r.settle_ticks);
    }
    // The fault schedule really ran. Observed per seed: ~15 crashes, ~5
    // partitions and ~5 terms with a leader. The floors are about half that.
    CHECK(total_crashes > 7 * kSeeds);
    CHECK(total_partitions > 2 * kSeeds);
    CHECK(total_terms > 3 * kSeeds);
    CHECK(worst_settle < kSettleBound);
}

TEST(election_fuzz_is_reproducible) {
    for (std::uint64_t seed : {3ULL, 333ULL, 999ULL}) {
        const FuzzResult a = run_seed(seed);
        const FuzzResult b = run_seed(seed);
        CHECK_EQ(a.crashes, b.crashes);
        CHECK_EQ(a.partitions, b.partitions);
        CHECK_EQ(a.terms_with_leader, b.terms_with_leader);
    }
}
