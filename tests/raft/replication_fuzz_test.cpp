// Given: replication safety must hold under any mix of faults while clients
//        keep writing.
// Approach: run many seeds of a cluster with 10% loss, 10% duplicate delivery
//           and a seed-driven fault schedule. Every checker runs after every
//           tick. Then all faults are cleared, and every node must end up
//           applying the same sequence. Any failure reports its seed, so it
//           can be replayed. There are two schedules:
//   - general: 5 nodes, random crashes, restarts and partitions, and clients
//     that write to random nodes and follow leader hints.
//   - nemesis: 3 nodes, aimed at the Figure 8 window. It crashes leaders soon
//     after they win, cuts the newest leader off alone (replacing any current
//     partition), and hands writes to every node that believes it leads, so
//     stale leaders build up divergent suffixes.

#include <algorithm>
#include <cstdint>
#include <optional>
#include <set>
#include <stdexcept>
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

// Per-tick fault and workload probabilities.
struct Profile {
    double crash = 0.004;           // crash a random live node
    double crash_new_leader = 0.0;  // per new leader: crash it 1..40 ticks after it wins
    double restart = 0.01;          // per crashed node
    double partition = 0.002;       // random two-way split
    double isolate_leader = 0.0;    // cut the newest leader off alone
    double heal = 0.004;
    double propose = 0.1;
    // Instead of one client write to a random node, hand each write to every
    // live node that believes it leads, stale leaders included.
    bool propose_to_all_leaders = false;
    std::size_t nodes = 5;
    std::size_t max_down = 2;
    quorum::sim::NetworkConfig net{
        .min_delay = 1, .max_delay = 10, .drop_probability = 0.1, .duplicate_probability = 0.1};
};

// Fisher-Yates on the portable Rng (std::shuffle is implementation-defined).
void shuffle(std::vector<NodeId>& v, Rng& rng) {
    for (std::size_t i = v.size(); i > 1; --i) {
        const auto j = static_cast<std::size_t>(rng.uniform(0, i - 1));
        std::swap(v[i - 1], v[j]);
    }
}

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

// The leader with the highest term. When a partition leaves a stale leader
// behind, this is the one the majority follows.
std::optional<NodeId> newest_leader(const RaftCluster& c) {
    std::optional<NodeId> best;
    for (NodeId id : c.leaders())
        if (!best || c.node(id)->current_term() > c.node(*best)->current_term()) best = id;
    return best;
}

FuzzResult run_seed(std::uint64_t seed, const Profile& p = {}) {
    RaftCluster c(seed, p.nodes, {}, p.net);
    ElectionSafetyChecker election;
    LogSafetyChecker logs;
    c.add_observer([&](const RaftCluster& cl) {
        election.check(cl);
        logs.check(cl);
    });

    Rng faults(seed ^ 0xfa017fa017fa017fULL);
    Rng clients(seed ^ 0xc11e47c11e47c11eULL);
    bool partitioned = false;
    FuzzResult r;
    std::uint64_t next_cmd = 0;
    auto fresh = [&] { return "s" + std::to_string(seed) + "-" + std::to_string(next_cmd++); };
    auto failed = [&] { return !election.ok() || !logs.ok(); };

    std::set<std::pair<NodeId, Term>> seen_leaders;
    std::vector<std::pair<Tick, std::pair<NodeId, Term>>> doomed;  // (when, (leader, term))

    try {
        for (Tick t = 0; t < kFaultTicks && !failed(); ++t) {
            // A nemesis that targets the window right after an election: the new
            // leader crashes while it is still bringing followers up to date.
            for (NodeId l : c.leaders()) {
                const Term term = c.node(l)->current_term();
                if (seen_leaders.insert({l, term}).second && faults.bernoulli(p.crash_new_leader))
                    doomed.push_back({t + faults.uniform(1, 40), {l, term}});
            }
            for (auto it = doomed.begin(); it != doomed.end();) {
                if (it->first > t) {
                    ++it;
                    continue;
                }
                const auto [l, term] = it->second;
                std::size_t down_now = 0;
                for (NodeId id : c.ids()) down_now += c.is_up(id) ? 0 : 1;
                if (c.is_up(l) && c.node(l)->role() == Role::Leader &&
                    c.node(l)->current_term() == term && down_now < p.max_down) {
                    c.crash(l);
                    ++r.crashes;
                }
                it = doomed.erase(it);
            }

            std::vector<NodeId> up, down;
            for (NodeId id : c.ids()) (c.is_up(id) ? up : down).push_back(id);
            if (down.size() < p.max_down && faults.bernoulli(p.crash)) {
                c.crash(up[faults.uniform(0, up.size() - 1)]);
                ++r.crashes;
            }
            for (NodeId id : down)
                if (faults.bernoulli(p.restart)) c.restart(id);
            // Cutting off the newest leader may replace an existing partition,
            // for example isolating a new majority leader while the previous
            // one is still cut off. Figure 8 needs exactly that. Whatever the
            // isolated leader accepts now stays on that one node.
            if (faults.bernoulli(p.isolate_leader)) {
                if (const auto l = newest_leader(c)) {
                    std::vector<NodeId> others;
                    for (NodeId id : c.ids())
                        if (id != *l) others.push_back(id);
                    c.partition({{*l}, others});
                    partitioned = true;
                    ++r.partitions;
                }
            } else if (!partitioned && faults.bernoulli(p.partition)) {
                std::vector<NodeId> ids = c.ids();
                shuffle(ids, faults);
                const auto cut = static_cast<std::ptrdiff_t>(faults.uniform(1, p.nodes - 1));
                c.partition({{ids.begin(), ids.begin() + cut}, {ids.begin() + cut, ids.end()}});
                partitioned = true;
                ++r.partitions;
            } else if (partitioned && faults.bernoulli(p.heal)) {
                c.heal();
                partitioned = false;
            }
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

        // Heal everything. Loss and duplicates stay on. Once a stable leader
        // exists, give it an entry of its own term so its whole log can
        // commit (§5.4.2).
        c.heal();
        for (NodeId id : c.ids()) c.restart(id);
        const Tick settle_start = c.now();
        while (!failed() && !converged(c) && c.now() - settle_start < kSettleBound) {
            if (const auto l = c.stable_leader()) {
                const RaftNode& n = *c.node(*l);
                if (n.last_log_term() != n.current_term()) c.propose(*l, fresh());
            }
            c.tick();
        }
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
                if (!seen.insert(e.command).second) r.failure = "command applied twice: " + e.command;
                if (e.command.rfind("s" + std::to_string(seed) + "-", 0) != 0)
                    r.failure = "unknown command: " + e.command;
            }
            r.committed = log.size();
        }
    } catch (const std::exception& e) {
        r.failure = std::string("exception: ") + e.what();
    }
    for (NodeId id : c.ids()) r.truncated += c.storage(id).truncated_entries();
    r.duplicated = c.network().stats().duplicated;
    return r;
}

}  // namespace

TEST(replication_fuzz_safety_and_convergence_across_1000_seeds) {
    FuzzResult total;
    Tick worst_settle = 0;
    for (std::uint64_t seed = 1; seed <= kSeeds; ++seed) {
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
    CHECK(total.committed > 130 * kSeeds);
    CHECK(total.truncated > 9 * kSeeds);
    CHECK(total.duplicated > 170 * kSeeds);
    CHECK(total.crashes > 7 * kSeeds);
    CHECK(total.partitions > 2 * kSeeds);
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
    p.nodes = 3;
    p.max_down = 1;
    p.crash_new_leader = 0.5;
    p.isolate_leader = 0.02;
    p.heal = 0.03;
    p.restart = 0.02;
    p.propose = 0.03;
    p.propose_to_all_leaders = true;
    return p;
}

TEST(replication_fuzz_nemesis_across_1000_seeds) {
    const Profile p = nemesis();
    FuzzResult total;
    for (std::uint64_t seed = 1; seed <= kSeeds; ++seed) {
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
    CHECK(total.committed > 30 * kSeeds);
    CHECK(total.truncated > 2 * kSeeds);
    CHECK(total.crashes > 8 * kSeeds);
    CHECK(total.partitions > 25 * kSeeds);
}
