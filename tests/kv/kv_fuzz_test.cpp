// Given: the KV store must stay correct under the step 3 fault schedules
//        while clients keep issuing operations and retrying.
// Approach: for each of 1,000 seeds per schedule, three simulated clients run
//           random put/get/delete/cas operations on a few hot keys, with the
//           Raft safety checkers on every tick. After the faults stop, every
//           client finishes its outstanding operation. Then:
//   - every replica holds the same KV map, session table and execution
//     record;
//   - every operation in the history executed exactly once: none lost,
//     none applied twice;
//   - each client's reply equals the result of its single execution in log
//     order;
//   - replaying the leader's log into a fresh state machine rebuilds the
//     same state (sessions included).

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fault_injector.h"
#include "harness.h"
#include "quorum/kv/cluster.h"
#include "quorum/raft/invariants.h"

using namespace quorum::kv;
using quorum::raft::ElectionSafetyChecker;
using quorum::raft::LogSafetyChecker;
using quorum::raft::RaftNode;
using quorum::sim::Rng;
using quorum::testing::FaultInjector;
using quorum::testing::FaultProfile;

namespace {

constexpr std::uint64_t kSeeds = 1000;
constexpr Tick kFaultTicks = 4000;
constexpr Tick kSettleBound = 5000;
constexpr double kOpRate = 0.3;  // per idle client per tick

struct FuzzResult {
    std::string failure;
    std::uint64_t completed = 0;
    std::uint64_t timeouts = 0;
    std::uint64_t redirects = 0;
    std::uint64_t deduplicated = 0;  // retries absorbed by the session table
    std::uint64_t crashes = 0;
    std::uint64_t partitions = 0;
    Tick settle_ticks = 0;
};

void submit_random(KvClient& c, Rng& rng) {
    const std::string key = "k" + std::to_string(rng.uniform(0, 3));
    const std::string value = "c" + std::to_string(c.client_id()) + "-" + std::to_string(c.stats().sends);
    const auto roll = rng.uniform(0, 99);
    if (roll < 35) {
        c.submit(Op::Put, key, value);
    } else if (roll < 65) {
        c.submit(Op::Get, key);
    } else if (roll < 75) {
        c.submit(Op::Delete, key);
    } else {
        // Expect the last value this client saw (if any) or absence, so CAS
        // succeeds sometimes and fails sometimes.
        std::optional<std::string> expected;
        if (c.last_result() && c.last_result()->value && rng.bernoulli(0.7)) expected = c.last_result()->value;
        c.submit(Op::Cas, key, value, expected);
    }
}

bool settled(const KvCluster& k) {
    if (!k.clients_idle()) return false;
    const auto l = k.raft().stable_leader();
    if (!l) return false;
    const RaftNode& leader = *k.raft().node(*l);
    if (leader.commit_index() != leader.last_log_index()) return false;
    for (NodeId id : k.raft().ids())
        if (k.service(id)->state().last_applied() != leader.last_log_index()) return false;
    return true;
}

std::string check_final_state(const KvCluster& k) {
    const NodeId lid = *k.raft().stable_leader();
    const KvStateMachine& ref = k.service(lid)->state();
    for (NodeId id : k.raft().ids()) {
        const KvStateMachine& sm = k.service(id)->state();
        if (sm.data() != ref.data()) return "node " + std::to_string(id) + " has a different KV map";
        if (sm.sessions() != ref.sessions()) return "node " + std::to_string(id) + " has different sessions";
        if (sm.executions() != ref.executions())
            return "node " + std::to_string(id) + " executed a different sequence";
    }

    // Exactly once: index every execution by (client, seq).
    std::map<std::pair<ClientId, Seq>, const Execution*> executed;
    for (const Execution& e : ref.executions())
        if (!executed.emplace(std::make_pair(e.command.client, e.command.seq), &e).second)
            return "client " + std::to_string(e.command.client) + " seq " + std::to_string(e.command.seq) +
                   " executed twice";
    for (const OpRecord& op : k.history().ops()) {
        const auto key = std::make_pair(op.command.client, op.command.seq);
        if (!op.completed) return "an operation never completed";
        const auto it = executed.find(key);
        if (it == executed.end())
            return "completed " + std::string(to_string(op.command.op)) + " by client " +
                   std::to_string(key.first) + " seq " + std::to_string(key.second) + " was lost";
        if (!(it->second->command == op.command)) return "a different command ran under the same seq";
        if (!(it->second->result == *op.result))
            return "client " + std::to_string(key.first) + " seq " + std::to_string(key.second) +
                   " got a reply that differs from its execution";
    }
    if (executed.size() != k.history().ops().size()) return "an execution with no client operation";

    // Deterministic rebuild from the log alone.
    KvStateMachine replay;
    const auto& log = k.raft().node(lid)->log();
    for (std::size_t i = 0; i < log.size(); ++i) replay.apply(i + 1, log[i]);
    if (replay.data() != ref.data() || replay.sessions() != ref.sessions() ||
        replay.executions() != ref.executions())
        return "replaying the log rebuilt a different state";
    return {};
}

FuzzResult run_seed(std::uint64_t seed, const FaultProfile& p) {
    KvClusterConfig cfg;
    cfg.servers = p.nodes;
    cfg.clients = 3;
    cfg.net = p.net;
    KvCluster k(seed, cfg);
    ElectionSafetyChecker election;
    LogSafetyChecker logs;
    k.raft().add_observer([&](const quorum::raft::RaftCluster& c) {
        election.check(c);
        logs.check(c);
    });
    auto failed = [&] { return !election.ok() || !logs.ok(); };

    FaultInjector faults(p, seed);
    Rng work(seed ^ 0x3c3c3c3c5a5a5a5aULL);
    FuzzResult r;
    try {
        for (Tick t = 0; t < kFaultTicks && !failed(); ++t) {
            faults.step(k.raft(), t);
            for (std::size_t i = 0; i < k.client_count(); ++i)
                if (k.client(i).idle() && work.bernoulli(kOpRate)) submit_random(k.client(i), work);
            k.tick();
        }
        faults.clear(k.raft());
        const Tick settle_start = k.now();
        while (!failed() && !settled(k) && k.now() - settle_start < kSettleBound) k.tick();
        r.settle_ticks = k.now() - settle_start;

        if (!election.ok()) r.failure = *election.violation();
        else if (!logs.ok()) r.failure = *logs.violation();
        else if (!settled(k)) r.failure = "did not settle within " + std::to_string(kSettleBound) + " ticks";
        else r.failure = check_final_state(k);
    } catch (const std::exception& e) {
        r.failure = std::string("exception: ") + e.what();
    }
    for (std::size_t i = 0; i < k.client_count(); ++i) {
        r.completed += k.client(i).stats().completed;
        r.timeouts += k.client(i).stats().timeouts;
        r.redirects += k.client(i).stats().redirects;
    }
    for (NodeId id : k.raft().ids())
        if (const KvService* s = k.service(id))
            r.deduplicated += s->stats().replied_cached + s->state().stats().duplicates;
    r.crashes = faults.crashes;
    r.partitions = faults.partitions;
    return r;
}

FuzzResult run_all(const FaultProfile& p, const char* name) {
    FuzzResult total;
    for (std::uint64_t seed = 1; seed <= kSeeds; ++seed) {
        const FuzzResult r = run_seed(seed, p);
        if (!r.failure.empty())
            quorum::test::fail(__FILE__, __LINE__,
                               std::string(name) + " seed " + std::to_string(seed) + ": " + r.failure);
        total.completed += r.completed;
        total.timeouts += r.timeouts;
        total.redirects += r.redirects;
        total.deduplicated += r.deduplicated;
        total.crashes += r.crashes;
        total.partitions += r.partitions;
        total.settle_ticks = std::max(total.settle_ticks, r.settle_ticks);
    }
    return total;
}

}  // namespace

TEST(kv_fuzz_general_across_1000_seeds) {
    const FuzzResult t = run_all(FaultProfile::general(), "general");
    // Observed per seed: ~149 completed operations, ~73 client timeouts,
    // ~122 redirects and ~89 retries absorbed by the session table. The
    // floors are about half that.
    CHECK(t.completed > 75 * kSeeds);
    CHECK(t.timeouts > 35 * kSeeds);
    CHECK(t.redirects > 60 * kSeeds);
    CHECK(t.deduplicated > 40 * kSeeds);
    CHECK(t.settle_ticks < kSettleBound);
}

TEST(kv_fuzz_nemesis_across_1000_seeds) {
    const FuzzResult t = run_all(FaultProfile::nemesis(), "nemesis");
    // Observed per seed: ~79 completed, ~79 timeouts, ~163 redirects, ~51
    // absorbed retries.
    CHECK(t.completed > 40 * kSeeds);
    CHECK(t.timeouts > 40 * kSeeds);
    CHECK(t.redirects > 80 * kSeeds);
    CHECK(t.deduplicated > 25 * kSeeds);
    CHECK(t.settle_ticks < kSettleBound);
}

TEST(kv_fuzz_is_reproducible) {
    for (std::uint64_t seed : {5ULL, 50ULL, 500ULL}) {
        const FuzzResult a = run_seed(seed, FaultProfile::general());
        const FuzzResult b = run_seed(seed, FaultProfile::general());
        CHECK(a.failure.empty());
        CHECK_EQ(a.completed, b.completed);
        CHECK_EQ(a.timeouts, b.timeouts);
        CHECK_EQ(a.deduplicated, b.deduplicated);
        CHECK_EQ(a.settle_ticks, b.settle_ticks);
    }
}
