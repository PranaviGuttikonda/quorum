// Given: leader election must work end to end over the simulated network.
// Approach: scripted scenarios on a 5-node RaftCluster, each run over many
//           seeds. An ElectionSafetyChecker observes every tick.

#include <algorithm>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/raft/cluster.h"
#include "quorum/raft/invariants.h"

using namespace quorum::raft;

namespace {

// Upper bound for electing a leader in a healthy cluster. One timeout is at
// most 300 ticks. The bound leaves room for a few split votes, and the
// measured worst case over all seeds used here is well below it.
constexpr Tick kElectionBound = 1500;

std::string seed_msg(std::uint64_t seed, const std::string& what) {
    return "seed " + std::to_string(seed) + ": " + what;
}

#define CHECK_SEED(seed, expr)                                                          \
    do {                                                                                \
        if (!(expr)) quorum::test::fail(__FILE__, __LINE__, seed_msg(seed, "CHECK(" #expr ")")); \
    } while (0)

void attach(RaftCluster& c, ElectionSafetyChecker& chk) {
    c.add_observer([&chk](const RaftCluster& cl) { chk.check(cl); });
}

bool has_stable_leader(const RaftCluster& c) {
    return c.stable_leader().has_value();
}

}  // namespace

TEST(election_stable_cluster_elects_exactly_one_leader) {
    Tick worst = 0;
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
        RaftCluster c(seed, 5);
        ElectionSafetyChecker chk;
        attach(c, chk);

        CHECK_SEED(seed, c.run_until(has_stable_leader, kElectionBound));
        worst = std::max(worst, c.now());
        const NodeId leader = *c.stable_leader();
        const Term term = c.node(leader)->current_term();

        // With no faults the leader must hold: one leader every tick, no new terms.
        for (int i = 0; i < 3000; ++i) {
            c.tick();
            CHECK_SEED(seed, c.leaders() == std::vector<NodeId>{leader});
        }
        CHECK_SEED(seed, c.stable_leader() == leader);
        for (NodeId id : c.ids()) CHECK_SEED(seed, c.node(id)->current_term() == term);
        CHECK_SEED(seed, chk.ok());
    }
    CHECK(worst < kElectionBound);
}

TEST(election_is_deterministic_per_seed) {
    auto run = [](std::uint64_t seed) {
        RaftCluster c(seed, 5, {}, {.min_delay = 1, .max_delay = 10, .drop_probability = 0.1});
        ElectionSafetyChecker chk;
        attach(c, chk);
        c.run_for(500);
        c.crash(c.leaders().empty() ? 1 : c.leaders()[0]);
        c.run_for(1000);
        return std::make_pair(c.network().trace(), chk.leaders_by_term());
    };
    const auto a = run(77);
    const auto b = run(77);
    CHECK(a.first == b.first);
    CHECK(a.second == b.second);
    CHECK(!a.second.empty());
    CHECK(!(run(78).first == a.first));
}

TEST(election_leader_crash_elects_new_leader_with_higher_term) {
    for (std::uint64_t seed = 1; seed <= 50; ++seed) {
        RaftCluster c(seed, 5);
        ElectionSafetyChecker chk;
        attach(c, chk);

        CHECK_SEED(seed, c.run_until(has_stable_leader, kElectionBound));
        const NodeId old_leader = *c.stable_leader();
        const Term old_term = c.node(old_leader)->current_term();

        c.crash(old_leader);
        CHECK_SEED(seed, c.run_until(has_stable_leader, kElectionBound));
        const NodeId new_leader = *c.stable_leader();
        const Term new_term = c.node(new_leader)->current_term();
        CHECK_SEED(seed, new_leader != old_leader);
        CHECK_SEED(seed, new_term > old_term);

        // The old leader comes back with its persisted (stale) term and
        // follows the new leader.
        c.restart(old_leader);
        CHECK_SEED(seed, c.node(old_leader)->current_term() == old_term);
        CHECK_SEED(seed, c.node(old_leader)->role() == Role::Follower);
        c.run_for(1000);
        CHECK_SEED(seed, c.stable_leader().has_value());
        const NodeId final_leader = *c.stable_leader();
        CHECK_SEED(seed, c.node(old_leader)->current_term() ==
                             c.node(final_leader)->current_term());
        CHECK_SEED(seed, chk.ok());
    }
}

TEST(election_minority_partition_without_leader_never_elects) {
    for (std::uint64_t seed = 1; seed <= 50; ++seed) {
        RaftCluster c(seed, 5);
        ElectionSafetyChecker chk;
        attach(c, chk);
        CHECK_SEED(seed, c.run_until(has_stable_leader, kElectionBound));
        const NodeId leader = *c.stable_leader();

        std::vector<NodeId> minority, majority;
        for (NodeId id : c.ids()) {
            if (id != leader && minority.size() < 2) minority.push_back(id);
            else majority.push_back(id);
        }
        c.partition({minority, majority});

        for (int i = 0; i < 5000; ++i) {
            c.tick();
            for (NodeId id : minority) CHECK_SEED(seed, c.node(id)->role() != Role::Leader);
        }
        // The majority keeps its leader, while the minority keeps timing out
        // and inflating its terms.
        CHECK_SEED(seed, c.stable_leader() == leader);
        for (NodeId id : minority)
            CHECK_SEED(seed, c.node(id)->current_term() > c.node(leader)->current_term());

        c.heal();
        CHECK_SEED(seed, c.run_until(has_stable_leader, kElectionBound));
        CHECK_SEED(seed, c.leaders().size() == 1);
        const Term t = c.node(*c.stable_leader())->current_term();
        for (NodeId id : c.ids()) CHECK_SEED(seed, c.node(id)->current_term() == t);
        CHECK_SEED(seed, chk.ok());
    }
}

TEST(election_isolated_leader_is_replaced_and_steps_down_after_heal) {
    for (std::uint64_t seed = 1; seed <= 50; ++seed) {
        RaftCluster c(seed, 5);
        ElectionSafetyChecker chk;
        attach(c, chk);
        CHECK_SEED(seed, c.run_until(has_stable_leader, kElectionBound));
        const NodeId old_leader = *c.stable_leader();
        const Term old_term = c.node(old_leader)->current_term();

        std::vector<NodeId> minority{old_leader}, majority;
        for (NodeId id : c.ids()) {
            if (id == old_leader) continue;
            if (minority.size() < 2) minority.push_back(id);
            else majority.push_back(id);
        }
        c.partition({minority, majority});

        // No minority node may win an election after the partition. The old
        // leader may keep believing it leads old_term (no check-quorum in §5),
        // but it can't commit anything and no one else is in that term.
        for (int i = 0; i < 5000; ++i) {
            c.tick();
            for (NodeId id : minority) {
                const RaftNode* n = c.node(id);
                if (n->role() == Role::Leader)
                    CHECK_SEED(seed, id == old_leader && n->current_term() == old_term);
            }
        }
        // The majority elected its own leader in a newer term.
        const std::set<NodeId> maj(majority.begin(), majority.end());
        bool replaced = false;
        for (NodeId id : c.leaders())
            if (maj.count(id) && c.node(id)->current_term() > old_term) replaced = true;
        CHECK_SEED(seed, replaced);

        c.heal();
        CHECK_SEED(seed, c.run_until(has_stable_leader, kElectionBound));
        CHECK_SEED(seed, c.leaders().size() == 1);
        CHECK_SEED(seed, c.node(old_leader)->current_term() > old_term);
        CHECK_SEED(seed, chk.ok());
    }
}

TEST(election_checker_detects_two_leaders_in_one_term) {
    // Given: the checker is the oracle for the fuzz test, so it must actually
    //        fire.
    // Approach: feed it a split-brain history directly.
    ElectionSafetyChecker chk;
    chk.observe(10, 1, 3);
    chk.observe(11, 1, 3);  // same leader again: fine
    chk.observe(12, 2, 4);  // new term, new leader: fine
    CHECK(chk.ok());
    chk.observe(500, 3, 3);  // a second leader for term 3, much later
    CHECK(!chk.ok());
    CHECK_EQ(*chk.violation(),
             std::string("election safety violated at t=500: term 3 led by node 1 and node 3"));
    chk.observe(501, 4, 4);  // only the first violation is kept
    CHECK(chk.violation()->find("t=500") != std::string::npos);
}
