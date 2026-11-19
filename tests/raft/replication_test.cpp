// Given: log replication must work end to end over the simulated network.
// Approach: scripted scenarios on a 5-node RaftCluster, each run over many
//           seeds, with every safety checker observing every tick.

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

using namespace quorum::raft;

namespace {

constexpr Tick kElectionBound = 1500;
constexpr Tick kConvergeBound = 3000;

#define CHECK_SEED(seed, expr)                                                         \
    do {                                                                               \
        if (!(expr))                                                                   \
            quorum::test::fail(__FILE__, __LINE__,                                     \
                               "seed " + std::to_string(seed) + ": CHECK(" #expr ")"); \
    } while (0)

struct Checked {
    RaftCluster c;
    ElectionSafetyChecker election;
    LogSafetyChecker logs;

    explicit Checked(std::uint64_t seed, quorum::sim::NetworkConfig net = {}) : c(seed, 5, {}, net) {
        c.add_observer([this](const RaftCluster& cl) {
            election.check(cl);
            logs.check(cl);
        });
    }
    bool ok() const { return election.ok() && logs.ok(); }
    std::string why() const {
        return election.violation().value_or("") + logs.violation().value_or("");
    }
};

std::optional<NodeId> elect(RaftCluster& c) {
    if (!c.run_until([](const RaftCluster& x) { return x.stable_leader().has_value(); },
                     kElectionBound))
        return std::nullopt;
    return c.stable_leader();
}

// True once there is one stable leader whose whole log is committed, and every
// live node holds and has applied exactly that log.
bool converged(const RaftCluster& c) {
    const auto l = c.stable_leader();
    if (!l) return false;
    const RaftNode& leader = *c.node(*l);
    if (leader.commit_index() != leader.last_log_index()) return false;
    for (NodeId id : c.ids()) {
        const RaftNode* n = c.node(id);
        if (!n) continue;
        if (n->log() != leader.log() || c.applied(id) != leader.log()) return false;
    }
    return true;
}

// Client commands only: leaders also append no-ops (§8).
std::vector<std::string> commands(const std::vector<LogEntry>& log) {
    std::vector<std::string> out;
    for (const auto& e : log)
        if (!e.is_noop()) out.push_back(e.command);
    return out;
}

}  // namespace

TEST(replication_stable_cluster_replicates_and_applies_in_order) {
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        Checked k(seed);
        const auto leader = elect(k.c);
        CHECK_SEED(seed, leader.has_value());
        const Term term = k.c.node(*leader)->current_term();

        // Followers redirect clients to the leader.
        for (NodeId id : k.c.ids())
            if (id != *leader)
                CHECK_SEED(seed, (k.c.propose(id, "x") == ProposeResult{NotLeader{*leader}}));

        // The leader's log so far holds only its no-op, so the first command
        // lands right after it, and each later one takes the next index.
        CHECK_SEED(seed, k.c.node(*leader)->log().size() == 1 && k.c.node(*leader)->log()[0].is_noop());
        std::vector<std::string> want;
        for (int tick = 0; tick < 20; ++tick) {
            for (int j = 0; j < 5; ++j) {
                const std::string cmd = "cmd-" + std::to_string(want.size());
                const auto r = k.c.propose(*leader, cmd);
                CHECK_SEED(seed, (r == ProposeResult{Proposed{want.size() + 2, term}}));
                want.push_back(cmd);
            }
            k.c.tick();
        }
        CHECK_SEED(seed, k.c.run_until(converged, kConvergeBound));
        for (NodeId id : k.c.ids()) {
            CHECK_SEED(seed, commands(k.c.applied(id)) == want);
            CHECK_SEED(seed, k.c.node(id)->commit_index() == want.size() + 1);
        }
        CHECK_SEED(seed, k.c.stable_leader() == leader);
        if (!k.ok()) quorum::test::fail(__FILE__, __LINE__, k.why());
    }
}

TEST(replication_crashed_follower_catches_up) {
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        Checked k(seed);
        const auto leader = elect(k.c);
        CHECK_SEED(seed, leader.has_value());
        const NodeId lagger = *leader == 1 ? 2 : 1;
        k.c.crash(lagger);

        // More entries than one batch, so catch-up needs several rounds.
        for (int i = 0; i < 150; ++i) k.c.propose(*leader, "e" + std::to_string(i));
        CHECK_SEED(seed, k.c.run_until(
                             [&](const RaftCluster& c) {
                                 const RaftNode& l = *c.node(*leader);
                                 return l.commit_index() == l.last_log_index();
                             },
                             kConvergeBound));

        k.c.restart(lagger);
        CHECK_SEED(seed, commands(k.c.node(lagger)->log()).empty());  // it saw no command before crashing
        CHECK_SEED(seed, k.c.run_until(converged, kConvergeBound));
        CHECK_SEED(seed, commands(k.c.applied(lagger)).size() == 150);
        if (!k.ok()) quorum::test::fail(__FILE__, __LINE__, k.why());
    }
}

TEST(replication_partitioned_follower_catches_up_and_cannot_win) {
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        Checked k(seed);
        const auto leader = elect(k.c);
        CHECK_SEED(seed, leader.has_value());
        const NodeId lagger = *leader == 5 ? 4 : 5;
        std::vector<NodeId> rest;
        for (NodeId id : k.c.ids())
            if (id != lagger) rest.push_back(id);
        k.c.partition({{lagger}, rest});

        for (int i = 0; i < 200; ++i) {
            k.c.propose(*leader, "p" + std::to_string(i));
            if (i % 10 == 0) k.c.tick();
        }
        CHECK_SEED(seed, k.c.run_until(
                             [&](const RaftCluster& c) {
                                 const RaftNode& l = *c.node(*leader);
                                 return l.commit_index() == l.last_log_index();
                             },
                             kConvergeBound));
        // The isolated node kept timing out, so its term is now the highest.
        k.c.run_for(1000);
        CHECK_SEED(seed, k.c.node(lagger)->current_term() > k.c.node(*leader)->current_term());

        k.c.heal();
        CHECK_SEED(seed, k.c.run_until(converged, kConvergeBound));
        // Its higher term forced an election, but its stale log can't win one
        // (§5.4.1). Every committed entry survives.
        CHECK_SEED(seed, k.c.stable_leader() != lagger);
        CHECK_SEED(seed, commands(k.c.applied(lagger)).size() == 200);
        if (!k.ok()) quorum::test::fail(__FILE__, __LINE__, k.why());
    }
}

TEST(replication_conflicting_uncommitted_entries_are_overwritten) {
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        Checked k(seed);
        const auto old_leader = elect(k.c);
        CHECK_SEED(seed, old_leader.has_value());
        k.c.propose(*old_leader, "base");
        CHECK_SEED(seed, k.c.run_until(converged, kConvergeBound));

        // Isolate the leader. It still accepts proposals, but can't commit them.
        std::vector<NodeId> rest;
        for (NodeId id : k.c.ids())
            if (id != *old_leader) rest.push_back(id);
        k.c.partition({{*old_leader}, rest});
        for (const char* cmd : {"lost-1", "lost-2", "lost-3"})
            CHECK_SEED(seed, std::holds_alternative<Proposed>(k.c.propose(*old_leader, cmd)));

        // The majority elects a new leader and commits different entries at
        // the same indices.
        NodeId new_leader = 0;
        CHECK_SEED(seed, k.c.run_until(
                             [&](const RaftCluster& c) {
                                 for (NodeId id : c.leaders())
                                     if (id != *old_leader) {
                                         new_leader = id;
                                         return true;
                                     }
                                 return false;
                             },
                             kElectionBound));
        k.c.propose(new_leader, "kept-1");
        k.c.propose(new_leader, "kept-2");
        CHECK_SEED(seed, k.c.run_until(
                             [&](const RaftCluster& c) {
                                 const RaftNode& l = *c.node(new_leader);
                                 return l.commit_index() == l.last_log_index();
                             },
                             kConvergeBound));
        CHECK_SEED(seed, commands(k.c.node(*old_leader)->log()).size() == 4);  // base + 3 uncommitted

        k.c.heal();
        CHECK_SEED(seed, k.c.run_until(converged, kConvergeBound));
        const std::vector<std::string> want{"base", "kept-1", "kept-2"};
        for (NodeId id : k.c.ids()) {
            CHECK_SEED(seed, commands(k.c.node(id)->log()) == want);
            CHECK_SEED(seed, commands(k.c.applied(id)) == want);
        }
        CHECK_SEED(seed, k.c.storage(*old_leader).truncated_entries() >= 3);
        for (const auto& e : k.logs.applied_history())
            CHECK_SEED(seed, e.command.rfind("lost-", 0) != 0);  // never applied anywhere
        if (!k.ok()) quorum::test::fail(__FILE__, __LINE__, k.why());
    }
}

TEST(replication_survives_loss_and_duplicates) {
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        Checked k(seed, {.min_delay = 1, .max_delay = 10, .drop_probability = 0.2,
                         .duplicate_probability = 0.3});
        const auto leader = elect(k.c);
        CHECK_SEED(seed, leader.has_value());
        std::set<std::string> proposed;
        for (int i = 0; i < 100; ++i) {
            const auto l = k.c.stable_leader();
            if (l && std::holds_alternative<Proposed>(k.c.propose(*l, "d" + std::to_string(i))))
                proposed.insert("d" + std::to_string(i));
            k.c.run_for(3);
        }
        // The final leader's no-op lets its whole log commit.
        CHECK_SEED(seed, k.c.run_until(converged, kConvergeBound));
        // Duplicate delivery never duplicates log entries.
        const auto& log = k.c.node(*k.c.stable_leader())->log();
        std::set<std::string> seen;
        for (const auto& cmd : commands(log)) CHECK_SEED(seed, seen.insert(cmd).second);
        CHECK_SEED(seed, k.c.network().stats().duplicated > 0);
        if (!k.ok()) quorum::test::fail(__FILE__, __LINE__, k.why());
    }
}

TEST(replication_figure8_scenario_over_the_network) {
    // Given: Raft Figure 8 on a 3-node cluster, driven by partitions:
    //   1. Leader A, isolated alone, accepts x (term a). x exists only on A.
    //   2. The other two elect B, which (isolated alone) accepts y (term b > a).
    //   3. A and the third node D elect A in term c > b. A brings D up to x,
    //      so x is on a majority, but x is from term a < c.
    //   4. A crashes before any term-c entry spreads. B rejoins, and its last
    //      term b > a wins D's vote.
    // Approach: §5.4.2 says A must not have committed x in step 3, so B
    //           overwriting x is legal. A leader that commits x by counting
    //           replicas makes step 4 lose a committed entry, which the
    //           checkers report. Every step is checked, so the test can't pass
    //           vacuously.
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        // No §8 no-op here: this test exercises the raw §5.4.2 rule, which a
        // no-op would satisfy on the leader's behalf.
        RaftConfig raw;
        raw.append_noop_on_election = false;
        RaftCluster c(seed, 3, raw);
        ElectionSafetyChecker election;
        LogSafetyChecker logs;
        c.add_observer([&](const RaftCluster& cl) {
            election.check(cl);
            logs.check(cl);
        });

        const auto a = elect(c);
        CHECK_SEED(seed, a.has_value());
        c.propose(*a, "base");
        CHECK_SEED(seed, c.run_until(converged, kConvergeBound));

        // 1. Isolate A; x stays on A only.
        std::vector<NodeId> others;
        for (NodeId id : c.ids())
            if (id != *a) others.push_back(id);
        c.partition({{*a}, others});
        CHECK_SEED(seed, std::holds_alternative<Proposed>(c.propose(*a, "x")));
        const Term term_a = c.node(*a)->current_term();

        // 2. The other two elect B. Isolate B too and give it y.
        NodeId b = 0;
        CHECK_SEED(seed, c.run_until(
                             [&](const RaftCluster& cl) {
                                 for (NodeId id : others)
                                     if (cl.node(id)->role() == Role::Leader) {
                                         b = id;
                                         return true;
                                     }
                                 return false;
                             },
                             kElectionBound));
        const NodeId d = others[0] == b ? others[1] : others[0];
        c.partition({{*a, d}, {b}});
        CHECK_SEED(seed, std::holds_alternative<Proposed>(c.propose(b, "y")));
        const Term term_b = c.node(b)->current_term();
        CHECK_SEED(seed, term_b > term_a);

        // 3. A and D: A's log (ending in x, term a) beats D's, so A wins and
        //    replicates x to D. No client writes reach A in its new term.
        CHECK_SEED(seed, c.run_until(
                             [&](const RaftCluster& cl) {
                                 const RaftNode* na = cl.node(*a);
                                 return na->role() == Role::Leader && na->current_term() > term_b &&
                                        cl.node(d)->last_log_index() == na->last_log_index();
                             },
                             kElectionBound));
        CHECK_SEED(seed, c.node(d)->log().back().command == "x");
        c.run_for(30);  // let the leader see D's acknowledgement
        // §5.4.2: x is from an older term, so counting replicas can't commit it.
        if (c.node(*a)->commit_index() == c.node(*a)->last_log_index())
            quorum::test::fail(__FILE__, __LINE__,
                               "seed " + std::to_string(seed) +
                                   ": leader committed an older-term entry by counting replicas");

        // 4. Crash A and heal. B's last term beats D's, so B can win and
        //    overwrite x.
        c.crash(*a);
        c.heal();
        try {
            c.run_until(
                [&](const RaftCluster& cl) {
                    return cl.node(b)->role() == Role::Leader && cl.node(d)->log() == cl.node(b)->log();
                },
                3 * kElectionBound);
        } catch (const std::logic_error& e) {
            // D would have truncated an entry it had seen committed.
            quorum::test::fail(__FILE__, __LINE__, "seed " + std::to_string(seed) + ": " + e.what());
        }
        if (!election.ok() || !logs.ok())
            quorum::test::fail(__FILE__, __LINE__,
                               "seed " + std::to_string(seed) + ": " +
                                   election.violation().value_or("") + logs.violation().value_or(""));
        CHECK_SEED(seed, c.node(b)->role() == Role::Leader);
        CHECK_SEED(seed, c.node(d)->log().back().command == "y");  // x was legally overwritten
    }
}
