// Given: replication safety must hold under any mix of faults while clients
//        keep writing.
// Approach: run many seeds of a cluster with 10% loss, 10% duplicate delivery
//           and a seed-driven fault schedule. Every checker runs after every
//           tick. Then all faults are cleared, and every node must end up
//           applying the same sequence. Any failure reports its seed, so it
//           can be replayed. The fault schedules live in fault_injector.h:
//   - general: clients write to random nodes and follow leader hints.
//   - nemesis: writes go to every node that believes it leads, so stale
//     leaders build up divergent suffixes.

#include <algorithm>
#include <cstdint>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "fault_injector.h"
#include "fuzz_seeds.h"
#include "harness.h"
#include "quorum/raft/cluster.h"
#include "quorum/raft/invariants.h"
#include "quorum/sim/rng.h"

using namespace quorum::raft;
using quorum::sim::Rng;
using quorum::testing::FaultInjector;
using quorum::testing::FaultProfile;

namespace {

// QUORUM_FUZZ_SEEDS, default 1000 (tests/fuzz_seeds.h).
std::uint64_t seeds() { return quorum::testing::fuzz_seeds(); }
constexpr Tick kFaultTicks = 4000;
constexpr Tick kSettleBound = 5000;

// A fault schedule plus the client workload on top of it.
struct Profile {
    FaultProfile faults = FaultProfile::general();
    double propose = 0.1;  // per tick
    // Instead of one client write to a random node, hand each write to every
    // live node that believes it leads, stale leaders included.
    bool propose_to_all_leaders = false;
};

struct FuzzResult {
    std::string failure;  // empty when the seed passed
    std::uint64_t proposed = 0;
    std::uint64_t committed = 0;
    std::uint64_t truncated = 0;
    std::uint64_t duplicated = 0;
    std::uint64_t crashes = 0;
    std::uint64_t partitions = 0;
    Tick settle_ticks = 0;
};

// A client: try a random live node, and follow one leader hint.
void client_propose(RaftCluster& c, Rng& rng, const std::string& cmd, std::uint64_t& accepted) {
    std::vector<NodeId> up;
    for (NodeId id : c.ids())
        if (c.is_up(id)) up.push_back(id);
    if (up.empty()) return;
    auto r = c.propose(up[rng.uniform(0, up.size() - 1)], cmd);
    if (const auto* nl = std::get_if<NotLeader>(&r); nl && nl->leader_hint && c.is_up(*nl->leader_hint))
        r = c.propose(*nl->leader_hint, cmd);
    if (std::holds_alternative<Proposed>(r)) ++accepted;
}

bool converged(const RaftCluster& c) {
    const auto l = c.stable_leader();
    if (!l) return false;
    const RaftNode& leader = *c.node(*l);
    if (leader.commit_index() != leader.last_log_index()) return false;
    for (NodeId id : c.ids())
        if (c.node(id)->log() != leader.log() || c.applied(id) != leader.log()) return false;
    return true;
}

FuzzResult run_seed(std::uint64_t seed, const Profile& p = {}) {
    RaftCluster c(seed, p.faults.nodes, {}, p.faults.net);
    ElectionSafetyChecker election;
    LogSafetyChecker logs;
    c.add_observer([&](const RaftCluster& cl) {
        election.check(cl);
        logs.check(cl);
    });

    FaultInjector faults(p.faults, seed);
    Rng clients(seed ^ 0xc11e47c11e47c11eULL);
    FuzzResult r;
    std::uint64_t next_cmd = 0;
    auto fresh = [&] { return "s" + std::to_string(seed) + "-" + std::to_string(next_cmd++); };
    auto failed = [&] { return !election.ok() || !logs.ok(); };

    try {
        for (Tick t = 0; t < kFaultTicks && !failed(); ++t) {
            faults.step(c, t);
            if (clients.bernoulli(p.propose)) {
                if (p.propose_to_all_leaders) {
                    for (NodeId l : c.leaders())
                        if (std::holds_alternative<Proposed>(c.propose(l, fresh()))) ++r.proposed;
                } else {
                    client_propose(c, clients, fresh(), r.proposed);
                }
            }
            c.tick();
        }

        // Heal everything. Loss and duplicates stay on. The final leader's
        // no-op (§8) lets its whole log commit.
        faults.clear(c);
        const Tick settle_start = c.now();
        while (!failed() && !converged(c) && c.now() - settle_start < kSettleBound) c.tick();
        r.settle_ticks = c.now() - settle_start;

        if (!election.ok()) {
            r.failure = *election.violation();
        } else if (!logs.ok()) {
            r.failure = *logs.violation();
        } else if (!converged(c)) {
            r.failure = "did not converge within " + std::to_string(kSettleBound) + " ticks";
        } else {
            // Every node applied the same sequence: the leader's whole log.
            // Each command appears once (duplicate delivery never duplicates an
            // entry), and every command came from a client.
            const auto& log = c.node(*c.stable_leader())->log();
            std::set<std::string> seen;
            for (const auto& e : log) {
                if (e.is_noop()) continue;
                if (!seen.insert(e.command).second) r.failure = "command applied twice: " + e.command;
                if (e.command.rfind("s" + std::to_string(seed) + "-", 0) != 0)
                    r.failure = "unknown command: " + e.command;
                ++r.committed;
            }
        }
    } catch (const std::exception& e) {
        r.failure = std::string("exception: ") + e.what();
    }
    r.crashes = faults.crashes;
    r.partitions = faults.partitions;
    for (NodeId id : c.ids()) r.truncated += c.truncated_entries(id);
    r.duplicated = c.network().stats().duplicated;
    return r;
}

}  // namespace

TEST(replication_fuzz_safety_and_convergence_across_seeds) {
    FuzzResult total;
    Tick worst_settle = 0;
    for (std::uint64_t seed = 1; seed <= seeds(); ++seed) {
        const FuzzResult r = run_seed(seed);
        if (!r.failure.empty())
            quorum::test::fail(__FILE__, __LINE__, "seed " + std::to_string(seed) + ": " + r.failure);
        total.proposed += r.proposed;
        total.committed += r.committed;
        total.truncated += r.truncated;
        total.duplicated += r.duplicated;
        total.crashes += r.crashes;
        total.partitions += r.partitions;
        worst_settle = std::max(worst_settle, r.settle_ticks);
    }
    // The workload really ran. Observed per seed: ~274 accepted proposals,
    // ~263 committed entries, ~18 entries removed by conflict truncation,
    // ~343 duplicate deliveries, ~15 crashes and ~5 partitions. The floors are
    // about half that.
    CHECK(total.committed > 130 * seeds());
    CHECK(total.truncated > 9 * seeds());
    CHECK(total.duplicated > 170 * seeds());
    CHECK(total.crashes > 7 * seeds());
    CHECK(total.partitions > 2 * seeds());
    CHECK(worst_settle < kSettleBound);
}

TEST(replication_fuzz_is_reproducible) {
    for (std::uint64_t seed : {7ULL, 70ULL, 700ULL}) {
        const FuzzResult a = run_seed(seed);
        const FuzzResult b = run_seed(seed);
        CHECK(a.failure.empty());
        CHECK_EQ(a.committed, b.committed);
        CHECK_EQ(a.truncated, b.truncated);
        CHECK_EQ(a.duplicated, b.duplicated);
        CHECK_EQ(a.settle_ticks, b.settle_ticks);
    }
}

Profile nemesis() {
    Profile p;
    p.faults = FaultProfile::nemesis();
    p.propose = 0.03;
    p.propose_to_all_leaders = true;
    return p;
}

TEST(replication_fuzz_nemesis_across_seeds) {
    const Profile p = nemesis();
    FuzzResult total;
    for (std::uint64_t seed = 1; seed <= seeds(); ++seed) {
        const FuzzResult r = run_seed(seed, p);
        if (!r.failure.empty())
            quorum::test::fail(__FILE__, __LINE__,
                               "nemesis seed " + std::to_string(seed) + ": " + r.failure);
        total.committed += r.committed;
        total.truncated += r.truncated;
        total.crashes += r.crashes;
        total.partitions += r.partitions;
    }
    // Observed per seed: ~65 committed entries, ~5.6 entries removed by
    // conflict truncation, ~16 crashes and ~52 partitions. The floors are
    // about half that.
    CHECK(total.committed > 30 * seeds());
    CHECK(total.truncated > 2 * seeds());
    CHECK(total.crashes > 8 * seeds());
    CHECK(total.partitions > 25 * seeds());
}
