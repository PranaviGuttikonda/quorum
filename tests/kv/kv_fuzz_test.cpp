// Given: the KV store must stay correct under the step 3 fault schedules
//        while clients keep issuing operations and retrying.
// Approach: for each seed (QUORUM_FUZZ_SEEDS, default 1,000) per schedule, three simulated clients run
//           random put/get/delete/cas operations on a few hot keys, with the
//           Raft safety checkers on every tick. After the faults stop, every
//           client finishes its outstanding operation. Then:
//   - the whole history is linearizable (checked per key), and any
//     "unknown" verdict is counted and must be zero;
//   - every replica holds the same KV map, session table and execution
//     record;
//   - every write in the history executed exactly once: none lost, none
//     applied twice. Each writer's reply equals the result of that single
//     execution. Reads aren't required to touch the log: their results are
//     judged by the linearizability checker alone;
//   - replaying the leader's log into a fresh state machine rebuilds the
//     same state (sessions included).
//   Faults include clients being pulled into partitions, so a client can be
//   stuck with a minority leader.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fault_injector.h"
#include "fuzz_seeds.h"
#include "harness.h"
#include "quorum/kv/cluster.h"
#include "quorum/kv/linearizability.h"
#include "quorum/raft/invariants.h"

using namespace quorum::kv;
using quorum::raft::ElectionSafetyChecker;
using quorum::raft::LogSafetyChecker;
using quorum::raft::RaftNode;
using quorum::sim::Rng;
using quorum::testing::FaultInjector;
using quorum::testing::FaultProfile;

namespace {

// QUORUM_FUZZ_SEEDS, default 1000 (tests/fuzz_seeds.h).
std::uint64_t seeds() { return quorum::testing::fuzz_seeds(); }
constexpr Tick kFaultTicks = 4000;
constexpr Tick kSettleBound = 5000;
constexpr double kOpRate = 0.3;  // per idle client per tick

struct FuzzResult {
    std::string failure;
    std::uint64_t checked_ops = 0;     // operations given to the linearizability checker
    std::uint64_t unknown = 0;         // histories the checker couldn't decide within its bound
    std::uint64_t max_states = 0;      // largest per-key search, against a 1M bound
    std::uint64_t clients_partitioned = 0;
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

    // Exactly once, for writes: index every execution by (client, seq).
    std::map<std::pair<ClientId, Seq>, const Execution*> executed;
    for (const Execution& e : ref.executions())
        if (!executed.emplace(std::make_pair(e.command.client, e.command.seq), &e).second)
            return "client " + std::to_string(e.command.client) + " seq " + std::to_string(e.command.seq) +
                   " executed twice";
    std::map<std::pair<ClientId, Seq>, const OpRecord*> by_seq;
    for (const OpRecord& op : k.history().ops()) {
        const auto key = std::make_pair(op.command.client, op.command.seq);
        by_seq.emplace(key, &op);
        if (!op.completed) return "an operation never completed";
        if (!op.command.is_write()) continue;  // reads: judged by the linearizability checker
        const auto it = executed.find(key);
        if (it == executed.end())
            return "completed " + std::string(to_string(op.command.op)) + " by client " +
                   std::to_string(key.first) + " seq " + std::to_string(key.second) + " was lost";
        if (!(it->second->command == op.command)) return "a different command ran under the same seq";
        if (!(it->second->result == *op.result))
            return "client " + std::to_string(key.first) + " seq " + std::to_string(key.second) +
                   " got a reply that differs from its execution";
    }
    for (const auto& [key, e] : executed) {
        const auto it = by_seq.find(key);
        if (it == by_seq.end() || !(it->second->command == e->command))
            return "an execution with no matching client operation";
    }

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
        if (r.failure.empty()) {
            const CheckResult lin = check_linearizable(k.history().ops());
            r.checked_ops = lin.ops;
            for (const KeyReport& kr : lin.keys) r.max_states = std::max(r.max_states, kr.states);
            if (lin.verdict == Verdict::NotLinearizable) r.failure = "not linearizable\n" + lin.explanation;
            else if (lin.verdict == Verdict::Unknown) ++r.unknown;
        }
        if (r.failure.empty()) r.failure = check_final_state(k);
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
    r.clients_partitioned = faults.clients_partitioned;
    return r;
}

FuzzResult run_all(const FaultProfile& p, const char* name) {
    FuzzResult total;
    for (std::uint64_t seed = 1; seed <= seeds(); ++seed) {
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
        total.checked_ops += r.checked_ops;
        total.unknown += r.unknown;
        total.max_states = std::max(total.max_states, r.max_states);
        total.clients_partitioned += r.clients_partitioned;
    }
    std::printf("kv fuzz %s: %llu seeds, %llu ops checked for linearizability, %llu unknown, "
                "largest per-key search %llu states, %llu client partitionings\n",
                name, static_cast<unsigned long long>(seeds()),
                static_cast<unsigned long long>(total.checked_ops), static_cast<unsigned long long>(total.unknown),
                static_cast<unsigned long long>(total.max_states),
                static_cast<unsigned long long>(total.clients_partitioned));
    return total;
}

// Both schedules also pull clients into partitions.
FaultProfile with_client_partitions(FaultProfile p) {
    p.client_partition = 0.5;
    return p;
}

}  // namespace

TEST(kv_fuzz_general_across_seeds) {
    const FuzzResult t = run_all(with_client_partitions(FaultProfile::general()), "general");
    // Observed per seed: ~149 completed operations, ~73 client timeouts,
    // ~122 redirects and ~89 retries absorbed by the session table. The
    // floors are about half that.
    CHECK(t.completed > 75 * seeds());
    CHECK(t.timeouts > 35 * seeds());
    CHECK(t.redirects > 60 * seeds());
    CHECK(t.deduplicated > 40 * seeds());
    CHECK(t.settle_ticks < kSettleBound);
    CHECK_EQ(t.unknown, std::uint64_t{0});  // every history decided within the bound
}

TEST(kv_fuzz_nemesis_across_seeds) {
    const FuzzResult t = run_all(with_client_partitions(FaultProfile::nemesis()), "nemesis");
    // Observed per seed: ~79 completed, ~79 timeouts, ~163 redirects, ~51
    // absorbed retries.
    CHECK(t.completed > 40 * seeds());
    CHECK(t.timeouts > 40 * seeds());
    CHECK(t.redirects > 80 * seeds());
    CHECK(t.deduplicated > 25 * seeds());
    CHECK(t.settle_ticks < kSettleBound);
    CHECK_EQ(t.unknown, std::uint64_t{0});  // every history decided within the bound
}

TEST(kv_fuzz_is_reproducible) {
    for (std::uint64_t seed : {5ULL, 50ULL, 500ULL}) {
        const FuzzResult a = run_seed(seed, with_client_partitions(FaultProfile::general()));
        const FuzzResult b = run_seed(seed, with_client_partitions(FaultProfile::general()));
        CHECK(a.failure.empty());
        CHECK_EQ(a.completed, b.completed);
        CHECK_EQ(a.timeouts, b.timeouts);
        CHECK_EQ(a.deduplicated, b.deduplicated);
        CHECK_EQ(a.settle_ticks, b.settle_ticks);
    }
}
