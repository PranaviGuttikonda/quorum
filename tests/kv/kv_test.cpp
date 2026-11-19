// Given: the KV store must behave correctly end to end: clients, servers, Raft
//        and the simulated network together.
// Approach: scripted scenarios on a 5-server KvCluster, each over many seeds,
//           with the Raft safety checkers observing every tick.

#include <optional>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/kv/cluster.h"
#include "quorum/raft/invariants.h"

using namespace quorum::kv;
using quorum::raft::ElectionSafetyChecker;
using quorum::raft::LogSafetyChecker;
using quorum::raft::RaftNode;

namespace {

constexpr Tick kBound = 5000;

#define CHECK_SEED(seed, expr)                                                         \
    do {                                                                               \
        if (!(expr))                                                                   \
            quorum::test::fail(__FILE__, __LINE__,                                     \
                               "seed " + std::to_string(seed) + ": CHECK(" #expr ")"); \
    } while (0)

const std::optional<std::string> absent;
std::optional<std::string> some(std::string s) { return s; }

struct Checked {
    KvCluster k;
    ElectionSafetyChecker election;
    LogSafetyChecker logs;

    explicit Checked(std::uint64_t seed, KvClusterConfig cfg = {}) : k(seed, cfg) {
        k.raft().add_observer([this](const quorum::raft::RaftCluster& c) {
            election.check(c);
            logs.check(c);
        });
    }
    void require_safe(std::uint64_t seed) const {
        if (!election.ok() || !logs.ok())
            quorum::test::fail(__FILE__, __LINE__,
                               "seed " + std::to_string(seed) + ": " + election.violation().value_or("") +
                                   logs.violation().value_or(""));
    }
};

NodeId elect(KvCluster& k) {
    k.raft().run_until([](const quorum::raft::RaftCluster& c) { return c.stable_leader().has_value(); },
                       kBound);
    return *k.raft().stable_leader();
}

// Every live server has applied the leader's whole committed log, and they all
// hold the same KV data and sessions.
bool kv_converged(const KvCluster& k) {
    const auto l = k.raft().stable_leader();
    if (!l) return false;
    const RaftNode& leader = *k.raft().node(*l);
    if (leader.commit_index() != leader.last_log_index()) return false;
    const KvService* ref = k.service(*l);
    for (NodeId id : k.raft().ids()) {
        const KvService* s = k.service(id);
        if (!s) continue;
        if (s->state().last_applied() != leader.last_log_index()) return false;
        if (s->state().data() != ref->state().data() || s->state().sessions() != ref->state().sessions())
            return false;
    }
    return true;
}

// How many times (client, seq) actually executed on server `id`.
std::size_t executions_of(const KvCluster& k, NodeId id, ClientId c, Seq s) {
    std::size_t n = 0;
    for (const Execution& e : k.service(id)->state().executions())
        if (e.command.client == c && e.command.seq == s) ++n;
    return n;
}

}  // namespace

TEST(kv_basic_operations_on_a_stable_cluster) {
    for (std::uint64_t seed = 1; seed <= 10; ++seed) {
        Checked t(seed);
        elect(t.k);
        auto run = [&](Op op, std::string key, std::string value = {},
                       std::optional<std::string> expected = std::nullopt) {
            const auto r = t.k.execute(0, op, std::move(key), std::move(value), std::move(expected));
            CHECK_SEED(seed, r.has_value());
            return *r;
        };
        CHECK_SEED(seed, (run(Op::Get, "a") == Result{true, absent}));
        CHECK_SEED(seed, (run(Op::Put, "a", "1") == Result{true, absent}));
        CHECK_SEED(seed, (run(Op::Get, "a") == Result{true, some("1")}));
        CHECK_SEED(seed, (run(Op::Cas, "a", "2", some("1")) == Result{true, some("1")}));
        CHECK_SEED(seed, (run(Op::Cas, "a", "3", some("1")) == Result{false, some("2")}));
        CHECK_SEED(seed, (run(Op::Delete, "a") == Result{true, absent}));
        CHECK_SEED(seed, (run(Op::Delete, "a") == Result{false, absent}));
        CHECK_SEED(seed, (run(Op::Cas, "a", "x", absent) == Result{true, absent}));
        CHECK_SEED(seed, (run(Op::Get, "a") == Result{true, some("x")}));

        // A client pointed at a follower is redirected and still completes.
        const NodeId leader = *t.k.raft().stable_leader();
        t.k.client(1).set_leader_hint(leader == 1 ? 2 : 1);
        CHECK_SEED(seed, (t.k.execute(1, Op::Get, "a") == std::optional<Result>{Result{true, some("x")}}));
        CHECK_SEED(seed, t.k.client(1).stats().redirects >= 1);

        CHECK_SEED(seed, t.k.run_until(kv_converged, kBound));
        for (NodeId id : t.k.raft().ids())
            CHECK_SEED(seed, (t.k.service(id)->state().data() ==
                              std::map<std::string, std::string>{{"a", "x"}}));
        t.require_safe(seed);
    }
}

TEST(kv_retried_write_is_applied_exactly_once) {
    // Given: a client's create-if-absent CAS commits, but the reply is lost
    //        and the leader then crashes.
    // Approach: the client times out and retries the same (client, seq) on
    //           other servers. Applying it twice would answer {false, "once"}.
    //           Exactly-once means the original answer {true, absent}, and a
    //           single execution on every replica.
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        Checked t(seed);
        const NodeId leader = elect(t.k);
        KvClient& client = t.k.client(0);
        client.set_leader_hint(leader);
        const std::size_t op = client.submit(Op::Cas, "k", "once", absent);

        // Wait until the leader has proposed it, then cut the client off so
        // the reply can't reach it.
        CHECK_SEED(seed, t.k.run_until(
                             [&](const KvCluster& k) { return k.service(leader)->stats().proposed == 1; },
                             kBound));
        t.k.raft().set_global(client.node_id(), false);
        t.k.raft().partition({t.k.raft().ids(), {client.node_id()}});
        CHECK_SEED(seed, t.k.run_until(
                             [&](const KvCluster& k) {
                                 return k.service(leader)->state().cached(1, 1).has_value();
                             },
                             kBound));
        CHECK_SEED(seed, t.k.service(leader)->stats().replied == 1);  // sent into the void
        t.k.run_for(20);  // let the commit index reach the followers
        CHECK_SEED(seed, !client.idle());

        const Tick crashed_at = t.k.now();
        t.k.raft().crash(leader);
        t.k.raft().heal();
        t.k.raft().set_global(client.node_id(), true);
        CHECK_SEED(seed, t.k.run_until([&](const KvCluster& k) { return k.client(0).idle(); }, kBound));

        const OpRecord& rec = t.k.history().ops().at(op);
        CHECK_SEED(seed, (rec.result == std::optional<Result>{Result{true, absent}}));
        CHECK_SEED(seed, *rec.completed > crashed_at);
        CHECK_SEED(seed, client.stats().timeouts >= 1);

        t.k.raft().restart(leader);
        CHECK_SEED(seed, t.k.run_until(kv_converged, kBound));
        for (NodeId id : t.k.raft().ids()) {
            CHECK_SEED(seed, executions_of(t.k, id, 1, 1) == 1);
            CHECK_SEED(seed, t.k.service(id)->state().data().at("k") == "once");
        }
        t.require_safe(seed);
    }
}

TEST(kv_reads_after_leader_change_see_completed_writes) {
    // Given: writes that completed under one leader, and then a leader change.
    //   - By crash: the old leader is gone.
    //   - By isolation: the old leader is still up, still thinks it leads, and
    //     a second client still points at it.
    // Approach: reads go through the log, so the stale leader can't answer from
    //           its own state. The second client times out there and retries
    //           until a real leader serves it.
    for (int mode = 0; mode < 2; ++mode) {
        for (std::uint64_t seed = 1; seed <= 20; ++seed) {
            Checked t(seed);
            const NodeId old_leader = elect(t.k);
            for (int i = 0; i < 10; ++i)
                CHECK_SEED(seed, t.k.execute(0, Op::Put, "k" + std::to_string(i), "v" + std::to_string(i)));

            if (mode == 0) {
                t.k.raft().crash(old_leader);
            } else {
                std::vector<NodeId> rest;
                for (NodeId id : t.k.raft().ids())
                    if (id != old_leader) rest.push_back(id);
                t.k.raft().partition({{old_leader}, rest});
            }
            // More completed writes, which can only commit under a new leader.
            for (int i = 0; i < 5; ++i)
                CHECK_SEED(seed, t.k.execute(0, Op::Put, "k" + std::to_string(i), "w" + std::to_string(i)));
            CHECK_SEED(seed, t.k.raft().node(*t.k.client(0).leader_hint())->current_term() >
                                 (mode == 0 ? 0 : t.k.raft().node(old_leader)->current_term()));

            t.k.client(1).set_leader_hint(old_leader);
            for (int i = 0; i < 10; ++i) {
                const std::string want = (i < 5 ? "w" : "v") + std::to_string(i);
                const auto r = t.k.execute(1, Op::Get, "k" + std::to_string(i));
                CHECK_SEED(seed, (r == std::optional<Result>{Result{true, some(want)}}));
            }
            if (mode == 1) {
                // The stale leader was really asked first and couldn't answer.
                CHECK_SEED(seed, t.k.client(1).stats().timeouts >= 1);
                CHECK_SEED(seed, t.k.service(old_leader)->stats().replied == 10);  // only the first 10 puts
            }
            t.require_safe(seed);
        }
    }
}

TEST(kv_history_records_intervals_and_results) {
    Checked t(5);
    elect(t.k);
    t.k.execute(0, Op::Put, "a", "1");
    t.k.execute(1, Op::Get, "a");
    // With every server down, an operation stays pending: no completion.
    for (NodeId id : t.k.raft().ids()) t.k.raft().crash(id);
    t.k.client(2).submit(Op::Put, "a", "lost?");
    t.k.run_for(500);

    const auto& ops = t.k.history().ops();
    CHECK_EQ(ops.size(), std::size_t{3});
    CHECK((ops[0].command == Command::put(1, 1, "a", "1")));
    CHECK((ops[1].command == Command::get(2, 1, "a")));
    for (int i = 0; i < 2; ++i) CHECK(ops[i].completed && *ops[i].completed > ops[i].invoked);
    CHECK(ops[1].invoked >= *ops[0].completed);  // ran one after the other
    CHECK((ops[1].result == std::optional<Result>{Result{true, some("1")}}));
    CHECK(!ops[2].completed.has_value() && !ops[2].result.has_value());
    CHECK(t.k.client(2).stats().timeouts >= 4);  // kept retrying
}
