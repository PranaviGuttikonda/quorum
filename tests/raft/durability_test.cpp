// Given: with FileStorage on SimDisk, a crash is a power loss, so anything a
//        node acknowledged must already be synced.
// Approach: power off the whole cluster at once, right after something
//           committed or a KV write was acknowledged, and check it comes back.
//           Without the sync-before-send barrier these fail on early seeds.
//           Also: a disk losing power mid-call crashes just that node, and a
//           power loss during recovery leaves it down until the next restart.

#include <memory>
#include <optional>
#include <string>

#include "harness.h"
#include "quorum/kv/cluster.h"
#include "quorum/raft/cluster.h"
#include "sim_disks.h"

using namespace quorum::raft;
using quorum::testing::SimDisks;

#define CHECK_SEED(seed, expr)                                                                 \
    do {                                                                                       \
        if (!(expr))                                                                           \
            ::quorum::test::fail(__FILE__, __LINE__, "seed " + std::to_string(seed) + ": " #expr); \
    } while (0)

namespace {

void power_cycle_all(RaftCluster& c) {
    for (NodeId id : c.ids()) c.crash(id);
    for (NodeId id : c.ids()) c.restart(id);
}

}  // namespace

TEST(durability_committed_entries_survive_a_cluster_power_loss) {
    for (std::uint64_t seed = 1; seed <= 60; ++seed) {
        auto disks = std::make_shared<SimDisks>(seed);
        RaftCluster c(seed, 3, {}, {}, {}, disks);
        CHECK_SEED(seed, c.run_until([](const RaftCluster& x) { return x.stable_leader().has_value(); }, 3000));
        const NodeId leader = *c.stable_leader();
        LogIndex last = 0;
        for (int i = 0; i < 5; ++i) last = std::get<Proposed>(c.propose(leader, "cmd" + std::to_string(i))).index;
        // Stop the moment the leader has committed them: replies are in, so the
        // entries are on a majority, but nothing has been synced since.
        CHECK_SEED(seed, c.run_until([&](const RaftCluster& x) { return x.node(leader)->commit_index() >= last; }, 1000));
        const std::vector<LogEntry> committed(c.node(leader)->log().begin(),
                                              c.node(leader)->log().begin() + static_cast<std::ptrdiff_t>(last));

        power_cycle_all(c);
        std::size_t holders = 0;
        for (NodeId id : c.ids()) {
            const auto& log = c.node(id)->log();
            holders += log.size() >= last && std::equal(committed.begin(), committed.end(), log.begin());
        }
        CHECK_SEED(seed, holders >= 2);
        CHECK_SEED(seed, c.run_until(
                             [&](const RaftCluster& x) {
                                 for (NodeId id : x.ids())
                                     if (x.applied(id).size() < last) return false;
                                 return true;
                             },
                             3000));
        for (NodeId id : c.ids())
            CHECK_SEED(seed, std::equal(committed.begin(), committed.end(), c.applied(id).begin()));
    }
}

TEST(durability_acknowledged_kv_writes_survive_a_cluster_power_loss) {
    using namespace quorum::kv;
    for (std::uint64_t seed = 1; seed <= 30; ++seed) {
        KvClusterConfig cfg;
        cfg.servers = 3;
        cfg.clients = 2;
        cfg.storage = std::make_shared<SimDisks>(seed);
        KvCluster k(seed, cfg);
        CHECK_SEED(seed, (k.execute(0, Op::Put, "a", "1") == std::optional<Result>{Result{true, std::nullopt}}));
        CHECK_SEED(seed, (k.execute(0, Op::Put, "a", "2") == std::optional<Result>{Result{true, std::nullopt}}));
        power_cycle_all(k.raft());
        CHECK_SEED(seed, (k.execute(1, Op::Get, "a") == std::optional<Result>{Result{true, std::string("2")}}));
    }
}

TEST(durability_a_mid_call_power_loss_crashes_only_that_node) {
    auto disks = std::make_shared<SimDisks>(7);
    RaftCluster c(7, 3, {}, {}, {}, disks);
    CHECK(c.run_until([](const RaftCluster& x) { return x.stable_leader().has_value(); }, 3000));
    const NodeId leader = *c.stable_leader();
    disks->disk(leader).crash_after(0);  // the append in propose()
    CHECK(std::holds_alternative<NotLeader>(c.propose(leader, "x")));
    CHECK(!c.is_up(leader));
    CHECK_EQ(c.disk_crashes(), std::uint64_t{1});
    for (NodeId id : c.ids())
        if (id != leader) CHECK(c.is_up(id));

    // A power loss during recovery leaves the node down.
    disks->disk(leader).crash_after(0);
    c.restart(leader);
    CHECK(!c.is_up(leader));
    CHECK_EQ(c.disk_crashes(), std::uint64_t{2});
    c.restart(leader);
    CHECK(c.is_up(leader));
    CHECK(c.run_until([](const RaftCluster& x) { return x.stable_leader().has_value(); }, 3000));
}

TEST(durability_a_power_loss_while_handling_a_message_crashes_after_delivery) {
    auto disks = std::make_shared<SimDisks>(8);
    RaftCluster c(8, 3, {}, {}, {}, disks);
    CHECK(c.run_until([](const RaftCluster& x) { return x.stable_leader().has_value(); }, 3000));
    const NodeId leader = *c.stable_leader();
    const NodeId follower = leader == 1 ? 2 : 1;
    disks->disk(follower).crash_after(0);  // its next disk call: appending the entry
    (void)c.propose(leader, "y");
    CHECK(c.run_until([&](const RaftCluster& x) { return !x.is_up(follower); }, 200));
    CHECK_EQ(c.disk_crashes(), std::uint64_t{1});
    c.restart(follower);
    CHECK(c.run_until([&](const RaftCluster& x) { return x.is_up(follower) && x.applied(follower).size() >= 2; },
                      3000));
}
