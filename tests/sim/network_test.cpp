// Given: the simulated network is the base for every later fault test.
// Approach: drive small clusters through scripted and randomized workloads, and
//           check the trace, the stats and what each handler received.

#include "quorum/sim/network.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "harness.h"

using namespace quorum::sim;

namespace {

// Records every message each node receives.
struct Inbox {
    std::map<NodeId, std::vector<Message>> got;

    SimNetwork::Handler handler_for(NodeId id) {
        return [this, id](const Message& m) { got[id].push_back(m); };
    }
    std::size_t count(NodeId id) const {
        auto it = got.find(id);
        return it == got.end() ? 0 : it->second.size();
    }
};

SimNetwork make_cluster(std::uint64_t seed, NetworkConfig cfg, Inbox& inbox, NodeId n) {
    SimNetwork net(seed, cfg);
    for (NodeId id = 1; id <= n; ++id) net.add_node(id, inbox.handler_for(id));
    return net;
}

std::size_t count_drops(const SimNetwork& net, DropReason reason) {
    std::size_t c = 0;
    for (const auto& e : net.trace())
        if (e.kind == EventKind::Drop && e.reason == reason) ++c;
    return c;
}

constexpr NodeId kScenarioNodes = 5;

// Given: a busy cluster with loss, a partition and a crash/restart.
// Approach: nodes reply to a random peer (drawn from a workload Rng forked from
//           the seed), and nodes 1, 2 and 3 also send on timers. Node 3
//           keeps sending while crashed, and node 2 keeps probing it. All inputs are seed-driven, so the whole
//           trace is a function of the seed.

std::vector<TraceEvent> run_scenario(std::uint64_t seed) {
    NetworkConfig cfg{.min_delay = 1, .max_delay = 8, .drop_probability = 0.1};
    SimNetwork net(seed, cfg);
    Rng workload = Rng(seed).fork();

    for (NodeId id = 1; id <= kScenarioNodes; ++id) {
        net.add_node(id, [&net, &workload, id](const Message& m) {
            if (m.payload.size() > 6) return;  // bound the reply chain
            const auto to = static_cast<NodeId>(workload.uniform(1, kScenarioNodes));
            net.send(id, to, m.payload + "+");
        });
    }

    for (Tick t = 0; t < 300; ++t) {
        if (t % 3 == 0) net.send(1, static_cast<NodeId>(workload.uniform(2, kScenarioNodes)), "ping");
        if (t % 7 == 0) net.send(3, 1, "tick");  // keeps sending while crashed
        if (t % 5 == 0) net.send(2, 3, "probe");  // always traffic to the crashed node
        if (t == 60) net.partition({{1, 2}, {3, 4, 5}});
        if (t == 90) net.crash(3);
        if (t == 140) net.heal();
        if (t == 170) net.restart(3);
        net.tick();
    }
    return net.trace();
}

}  // namespace

TEST(network_config_is_validated) {
    CHECK_THROWS(SimNetwork(1, NetworkConfig{.min_delay = 0, .max_delay = 5}));
    CHECK_THROWS(SimNetwork(1, NetworkConfig{.min_delay = 6, .max_delay = 5}));
    CHECK_THROWS(SimNetwork(1, NetworkConfig{.drop_probability = -0.1}));
    CHECK_THROWS(SimNetwork(1, NetworkConfig{.drop_probability = 1.1}));
}

TEST(network_unknown_and_duplicate_nodes_throw) {
    Inbox inbox;
    SimNetwork net = make_cluster(1, {}, inbox, 2);
    CHECK_THROWS(net.add_node(1, nullptr));
    CHECK_THROWS(net.send(1, 9, "x"));
    CHECK_THROWS(net.send(9, 1, "x"));
    CHECK_THROWS(net.crash(9));
    CHECK_THROWS(net.partition({{1}, {9}}));
}

TEST(network_delivers_by_node_id_within_delay_range) {
    Inbox inbox;
    NetworkConfig cfg{.min_delay = 3, .max_delay = 7};
    SimNetwork net = make_cluster(11, cfg, inbox, 3);

    std::map<Tick, int> seen_delays;
    for (int i = 0; i < 2000; ++i) net.send(1, 2, "m" + std::to_string(i));
    net.send(1, 3, "only-for-3");
    net.run_for(cfg.max_delay);

    CHECK_EQ(inbox.count(2), std::size_t{2000});
    CHECK_EQ(inbox.count(3), std::size_t{1});
    CHECK_EQ(inbox.count(1), std::size_t{0});
    CHECK_EQ(inbox.got[3][0].payload, std::string("only-for-3"));
    CHECK_EQ(net.in_flight(), std::size_t{0});

    for (const Message& m : inbox.got[2]) {
        const Tick d = m.deliver_at - m.sent_at;
        CHECK(d >= cfg.min_delay && d <= cfg.max_delay);
        ++seen_delays[d];
    }
    CHECK_EQ(seen_delays.size(), std::size_t{5});  // every delay 3..7 occurs
}

TEST(network_delivery_order_is_by_time_then_id) {
    Inbox inbox;
    SimNetwork net = make_cluster(4, {.min_delay = 1, .max_delay = 20}, inbox, 2);
    for (int i = 0; i < 500; ++i) net.send(1, 2, "");
    net.run_for(20);

    const auto& got = inbox.got[2];
    CHECK_EQ(got.size(), std::size_t{500});
    for (std::size_t i = 1; i < got.size(); ++i) {
        const bool ordered = got[i - 1].deliver_at < got[i].deliver_at ||
                             (got[i - 1].deliver_at == got[i].deliver_at && got[i - 1].id < got[i].id);
        CHECK(ordered);
    }
}

TEST(network_same_seed_gives_identical_trace) {
    const auto a = run_scenario(2024);
    const auto b = run_scenario(2024);
    CHECK(a.size() > 500);
    CHECK_EQ(a.size(), b.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (!(a[i] == b[i])) {
            quorum::test::fail(__FILE__, __LINE__,
                               "trace diverges at " + std::to_string(i) + ": " + to_string(a[i]) +
                                   " vs " + to_string(b[i]));
        }
    }

    // The scenario must exercise every fault path, or it proves little.
    std::map<DropReason, int> reasons;
    for (const auto& e : a)
        if (e.kind == EventKind::Drop) ++reasons[e.reason];
    CHECK(reasons[DropReason::RandomLoss] > 0);
    CHECK(reasons[DropReason::Partitioned] > 0);
    CHECK(reasons[DropReason::ReceiverDown] > 0);
    CHECK(reasons[DropReason::SenderDown] > 0);
}

TEST(network_different_seed_gives_different_trace) {
    CHECK(!(run_scenario(2024) == run_scenario(2025)));
}

TEST(network_drops_at_rate_p) {
    for (double p : {0.05, 0.25, 0.5}) {
        Inbox inbox;
        SimNetwork net = make_cluster(99, {.drop_probability = p}, inbox, 2);
        const int n = 20000;
        for (int i = 0; i < n; ++i) net.send(1, 2, "");
        net.run_for(10);

        const auto& s = net.stats();
        const double rate = static_cast<double>(s.dropped_random) / n;
        CHECK(rate > p - 0.02 && rate < p + 0.02);
        CHECK_EQ(s.delivered + s.dropped_random, static_cast<std::uint64_t>(n));
        CHECK_EQ(inbox.count(2), static_cast<std::size_t>(s.delivered));
    }
}

TEST(network_drop_probability_extremes) {
    Inbox none_box, all_box;
    SimNetwork none = make_cluster(5, {.drop_probability = 0.0}, none_box, 2);
    SimNetwork all = make_cluster(5, {.drop_probability = 1.0}, all_box, 2);
    for (int i = 0; i < 1000; ++i) {
        none.send(1, 2, "");
        all.send(1, 2, "");
    }
    none.run_for(10);
    all.run_for(10);
    CHECK_EQ(none_box.count(2), std::size_t{1000});
    CHECK_EQ(all_box.count(2), std::size_t{0});
}

TEST(network_partition_blocks_then_heals) {
    Inbox inbox;
    SimNetwork net = make_cluster(8, {}, inbox, 4);
    net.partition({{1, 2}, {3, 4}});

    CHECK(net.can_reach(1, 2));
    CHECK(net.can_reach(3, 4));
    CHECK(!net.can_reach(1, 3));
    CHECK(!net.can_reach(4, 2));

    net.send(1, 2, "same-side");
    net.send(3, 4, "same-side");
    net.send(1, 3, "cross");
    net.send(4, 2, "cross");
    net.run_for(10);

    CHECK_EQ(inbox.count(2), std::size_t{1});
    CHECK_EQ(inbox.count(4), std::size_t{1});
    CHECK_EQ(inbox.count(3), std::size_t{0});
    CHECK_EQ(net.stats().dropped_partition, std::uint64_t{2});

    net.heal();
    CHECK(net.can_reach(1, 3));
    net.send(1, 3, "after-heal");
    net.send(4, 2, "after-heal");
    net.run_for(10);
    CHECK_EQ(inbox.count(3), std::size_t{1});
    CHECK_EQ(inbox.count(2), std::size_t{2});
    CHECK_EQ(inbox.got[3][0].payload, std::string("after-heal"));
}

TEST(network_partition_drops_in_flight_messages) {
    Inbox inbox;
    SimNetwork net = make_cluster(8, {.min_delay = 5, .max_delay = 5}, inbox, 2);
    net.send(1, 2, "in-flight");
    net.tick();
    net.partition({{1}, {2}});
    net.run_for(10);
    CHECK_EQ(inbox.count(2), std::size_t{0});
    CHECK_EQ(count_drops(net, DropReason::Partitioned), std::size_t{1});
}

TEST(network_partition_unlisted_nodes_share_a_group) {
    Inbox inbox;
    SimNetwork net = make_cluster(1, {}, inbox, 3);
    net.partition({{1}});
    CHECK(!net.can_reach(1, 2));
    CHECK(!net.can_reach(3, 1));
    CHECK(net.can_reach(2, 3));
    CHECK(net.can_reach(1, 1));
}

TEST(network_invalid_partition_keeps_previous) {
    Inbox inbox;
    SimNetwork net = make_cluster(1, {}, inbox, 3);
    net.partition({{1}, {2, 3}});
    CHECK_THROWS(net.partition({{1, 2}, {2, 3}}));
    CHECK(!net.can_reach(1, 2));
    CHECK(net.can_reach(2, 3));
}

TEST(network_crashed_node_receives_nothing) {
    Inbox inbox;
    SimNetwork net = make_cluster(3, {.min_delay = 1, .max_delay = 6}, inbox, 3);

    // Some messages are already in flight when node 2 crashes.
    for (int i = 0; i < 50; ++i) net.send(1, 2, "before");
    net.crash(2);
    CHECK(!net.is_up(2));
    for (int i = 0; i < 50; ++i) net.send(3, 2, "during");
    net.run_for(20);

    CHECK_EQ(inbox.count(2), std::size_t{0});
    CHECK_EQ(net.stats().dropped_receiver_down, std::uint64_t{100});

    net.restart(2);
    CHECK(net.is_up(2));
    net.send(1, 2, "after");
    net.run_for(10);
    CHECK_EQ(inbox.count(2), std::size_t{1});
    CHECK_EQ(inbox.got[2][0].payload, std::string("after"));
}

TEST(network_restart_does_not_deliver_messages_for_old_incarnation) {
    Inbox inbox;
    SimNetwork net = make_cluster(3, {.min_delay = 5, .max_delay = 5}, inbox, 2);
    net.send(1, 2, "to-old-incarnation");
    net.crash(2);
    net.restart(2);  // back up before the message is due
    net.run_for(10);
    CHECK_EQ(inbox.count(2), std::size_t{0});
    CHECK_EQ(count_drops(net, DropReason::ReceiverDown), std::size_t{1});
}

TEST(network_crashed_node_cannot_send) {
    Inbox inbox;
    SimNetwork net = make_cluster(3, {}, inbox, 2);
    net.crash(1);
    net.send(1, 2, "ghost");
    net.run_for(20);
    CHECK_EQ(inbox.count(2), std::size_t{0});
    CHECK_EQ(net.stats().dropped_sender_down, std::uint64_t{1});
}

TEST(network_messages_sent_before_crash_still_arrive) {
    Inbox inbox;
    SimNetwork net = make_cluster(3, {.min_delay = 5, .max_delay = 5}, inbox, 2);
    net.send(1, 2, "on-the-wire");
    net.crash(1);
    net.run_for(10);
    CHECK_EQ(inbox.count(2), std::size_t{1});
}

TEST(network_faults_do_not_shift_randomness_of_later_messages) {
    // Given: two runs with the same seed, where one has an extra partition.
    // Approach: the per-send RNG draws are fixed, so messages sent after the
    //           heal get identical delays in both runs.
    auto delays_after = [](bool with_partition) {
        Inbox inbox;
        SimNetwork net = make_cluster(42, {.min_delay = 1, .max_delay = 9}, inbox, 2);
        if (with_partition) net.partition({{1}, {2}});
        for (int i = 0; i < 20; ++i) net.send(1, 2, "");
        net.heal();
        net.run_for(10);
        for (int i = 0; i < 20; ++i) net.send(1, 2, "late");
        net.run_for(10);
        std::vector<Tick> out;
        for (const Message& m : inbox.got[2])
            if (m.payload == "late") out.push_back(m.deliver_at - m.sent_at);
        return out;
    };
    CHECK(delays_after(false) == delays_after(true));
    CHECK_EQ(delays_after(true).size(), std::size_t{20});
}

TEST(network_duplicate_probability_is_validated) {
    CHECK_THROWS(SimNetwork(1, NetworkConfig{.duplicate_probability = -0.1}));
    CHECK_THROWS(SimNetwork(1, NetworkConfig{.duplicate_probability = 1.5}));
}

TEST(network_duplicates_arrive_twice_at_rate_q) {
    Inbox inbox;
    SimNetwork net = make_cluster(21, {.min_delay = 1, .max_delay = 10, .duplicate_probability = 0.2},
                                  inbox, 2);
    const int n = 20000;
    for (int i = 0; i < n; ++i) net.send(1, 2, "m");
    net.run_for(10);

    const auto& s = net.stats();
    const double rate = static_cast<double>(s.duplicated) / n;
    CHECK(rate > 0.18 && rate < 0.22);
    CHECK_EQ(s.delivered, static_cast<std::uint64_t>(n) + s.duplicated);
    CHECK_EQ(inbox.count(2), static_cast<std::size_t>(s.delivered));

    // Every id arrives once or twice, and twice exactly when duplicated.
    std::map<MessageId, int> per_id;
    for (const Message& m : inbox.got[2]) ++per_id[m.id];
    CHECK_EQ(per_id.size(), static_cast<std::size_t>(n));
    std::uint64_t twice = 0;
    for (const auto& [id, k] : per_id) {
        CHECK(k == 1 || k == 2);
        twice += k == 2 ? 1 : 0;
    }
    CHECK_EQ(twice, s.duplicated);

    // The two copies have independent delays, so they often differ.
    std::map<MessageId, std::vector<Tick>> times;
    for (const Message& m : inbox.got[2]) times[m.id].push_back(m.deliver_at);
    int different = 0;
    for (const auto& [id, ts] : times)
        if (ts.size() == 2 && ts[0] != ts[1]) ++different;
    CHECK(different > 0);
}

TEST(network_lost_messages_are_never_duplicated) {
    Inbox inbox;
    SimNetwork net = make_cluster(
        22, {.drop_probability = 1.0, .duplicate_probability = 1.0}, inbox, 2);
    for (int i = 0; i < 100; ++i) net.send(1, 2, "");
    net.run_for(20);
    CHECK_EQ(inbox.count(2), std::size_t{0});
    CHECK_EQ(net.stats().duplicated, std::uint64_t{0});
}

TEST(network_duplicates_respect_crash_and_partition) {
    Inbox inbox;
    SimNetwork net = make_cluster(23, {.min_delay = 5, .max_delay = 5, .duplicate_probability = 1.0},
                                  inbox, 3);
    net.send(1, 2, "to-crashed");
    net.send(1, 3, "to-partitioned");
    net.crash(2);
    net.partition({{1}, {2, 3}});
    net.run_for(10);
    CHECK_EQ(inbox.count(2), std::size_t{0});
    CHECK_EQ(inbox.count(3), std::size_t{0});
    CHECK_EQ(net.stats().dropped_receiver_down, std::uint64_t{2});  // both copies
    CHECK_EQ(net.stats().dropped_partition, std::uint64_t{2});
}

TEST(network_trace_with_duplicates_is_reproducible) {
    auto run = [](std::uint64_t seed) {
        Inbox inbox;
        SimNetwork net = make_cluster(
            seed, {.min_delay = 1, .max_delay = 8, .drop_probability = 0.1, .duplicate_probability = 0.3},
            inbox, 3);
        for (int i = 0; i < 300; ++i) net.send(1 + i % 3, 1 + (i + 1) % 3, "x");
        net.run_for(10);
        return net.trace();
    };
    const auto a = run(5);
    CHECK(a == run(5));
    CHECK(!(a == run(6)));
    bool saw_dup = false;
    for (const auto& e : a) saw_dup |= e.kind == EventKind::Duplicate;
    CHECK(saw_dup);
}
