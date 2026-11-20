#pragma once

// Given: the browser visualizer must show the real engine, not a model of it.
// Approach: a thin host around KvCluster (memory storage) that the
//           WebAssembly build exposes through embind, and that also builds
//           natively for tests.
//   - Calls: tick, crash, restart, partition, heal, submit. Each one only
//     forwards to the engine. There is no Raft logic here, and none in the
//     page.
//   - snapshot() reports everything the page draws as JSON: each server's
//     role, term, vote, timer, commit index and recent log (entries
//     committed in that server's own view are marked), the partition, the
//     messages on the wire (classified by decoding their payloads), recent
//     drops, and the client operations with reply, server, latency and
//     retries.
//   - Timing is slowed for watching: network delay 8..20 ticks against
//     heartbeats every 50 and election timeouts of 150..300. The client
//     retries after 300.
//   - Clients run one operation at a time. Further submits queue here in
//     order and start when the client is idle.
//   - The network trace is read and cleared every tick, so memory stays
//     bounded however long the page runs.
//   - Time: 1 tick = 10 ms of simulated time (kTicksPerSecond).
//   - Events: elections won, crashes, restarts, splits, heals, operations
//     completing and chaos milestones, numbered from 0, for the timeline.
//     The oldest are dropped beyond kEventCap.
//   - Metrics: writes per second over the last 10 s, latency p50/p99 over
//     the last kLatencyWindow operations, elections (terms that got a
//     leader), and ticks without a working leader (a live leader that can
//     reach a live majority).
//   - Load generator: while on, each idle client sends a random put or get
//     on one of a few keys, with probability kLoadRate per tick. Its draws
//     come from its own stream, and only while it is on.
//   - Chaos: for kChaosTicks (60 simulated seconds) it crashes and restarts
//     servers (at most 2 down) and splits and heals the network, from a
//     stream seeded by the seed and the start tick, with the load generator
//     on. Then it heals everything, restarts every server, pauses the load,
//     waits for outstanding operations (up to kChaosSettle ticks), and runs
//     kv::check_linearizable on the whole history.
//   Everything is a function of the seed and the calls made at each tick, so
//   the page re-simulates any earlier moment exactly.

#include <cstdint>
#include <deque>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "quorum/kv/cluster.h"
#include "quorum/kv/linearizability.h"
#include "quorum/sim/rng.h"

namespace quorum::web {

using raft::NodeId;
using sim::Tick;

class Simulation {
public:
    static constexpr std::size_t kClients = 2;
    static constexpr std::size_t kLogTail = 24;  // entries per server in a snapshot
    static constexpr std::size_t kOpTail = 30;   // submitted operations in a snapshot
    static constexpr Tick kDropMemory = 12;      // ticks a drop stays in snapshots
    static constexpr Tick kTicksPerSecond = 100;
    static constexpr std::size_t kEventCap = 20000;
    static constexpr std::size_t kLatencyWindow = 200;
    static constexpr double kLoadRate = 0.15;
    static constexpr Tick kChaosTicks = 60 * kTicksPerSecond;
    static constexpr Tick kChaosSettle = 3000;
    static constexpr std::size_t kMaxDown = 2;

    // 1 <= servers <= 9.
    Simulation(std::uint64_t seed, std::size_t servers);

    void tick(std::uint32_t n = 1);
    // False if `id` isn't a server or is already in that state.
    bool crash(NodeId id);
    bool restart(NodeId id);
    // Groups of server ids, e.g. "1,2|3,4,5". Unlisted servers form one more
    // group. Clients stay reachable from every side. False (and no change)
    // if the text is malformed or names an unknown or repeated server.
    bool partition(const std::string& groups);
    void heal();
    // Queues an operation for client 1..kClients: op is put, get or delete.
    // Returns its id, or -1 if the arguments are invalid.
    std::int64_t submit(std::uint32_t client, const std::string& op, const std::string& key,
                        const std::string& value);

    // Load generator on or off. Off also drops nothing already queued.
    void set_load(bool on);
    bool load() const { return load_; }
    // Starts chaos at the current tick. False if chaos is already running.
    bool start_chaos();

    std::string snapshot() const;
    // Events from index `from` on: {"first": i, "total": n, "events": [...]}.
    std::string events_json(std::size_t from) const;
    std::uint64_t seed() const { return seed_; }
    Tick now() const { return kv_.now(); }
    std::optional<NodeId> leader() const { return kv_.raft().stable_leader(); }
    const kv::KvCluster& cluster() const { return kv_; }

    struct WebOp {
        std::int64_t id = 0;
        std::uint32_t client = 0;
        kv::Op op = kv::Op::Get;
        std::string key;
        std::string value;
        std::optional<std::size_t> history;  // set once started
        std::optional<NodeId> server;        // who answered
        kv::ClientStats stats;               // sends, timeouts, redirects
        bool generated = false;              // by the load generator
    };
    struct Event {
        Tick tick = 0;
        std::string kind;  // elect, crash, restart, split, heal, op, chaos, check
        NodeId node = 0;   // the server concerned, if any
        bool chaos = false;  // caused by chaos mode
        std::string text;
    };
    struct Completion {
        Tick at = 0;
        Tick latency = 0;
        bool write = false;
    };
    struct Chaos {
        enum class Phase { Running, Settling, Done };
        Phase phase = Phase::Running;
        Tick start = 0;
        Tick end = 0;
        Tick settle_until = 0;
        bool load_before = false;
        sim::Rng rng{0};
        std::optional<kv::CheckResult> result;
    };
    struct Drop {
        Tick at = 0;
        NodeId from = 0;
        NodeId to = 0;
        sim::DropReason reason = sim::DropReason::None;
    };
    const std::vector<WebOp>& ops() const { return ops_; }
    const std::deque<Drop>& drops() const { return drops_; }
    const std::deque<Event>& events() const { return events_; }
    std::size_t first_event() const { return first_event_; }  // index of events().front()
    const std::optional<Chaos>& chaos() const { return chaos_; }
    std::uint64_t elections() const { return elections_; }
    Tick leaderless_ticks() const { return leaderless_; }
    bool has_working_leader() const;

private:
    void start_queued();
    void generate_load();
    void chaos_step();
    void collect();
    void observe();
    void add_event(std::string kind, NodeId node, std::string text);
    void enqueue(WebOp w);
    bool do_crash(NodeId id);
    bool do_restart(NodeId id);

    std::uint64_t seed_;
    kv::KvCluster kv_;
    std::vector<WebOp> ops_;
    std::vector<std::deque<std::size_t>> queued_;  // per client: indices into ops_
    std::vector<std::optional<std::size_t>> running_;
    std::deque<Drop> drops_;
    bool load_ = false;
    sim::Rng load_rng_;
    std::uint64_t load_ops_ = 0;
    std::optional<Chaos> chaos_;
    bool in_chaos_ = false;  // events added now are caused by chaos
    std::deque<Event> events_;
    std::size_t first_event_ = 0;
    std::deque<Completion> completions_;
    std::set<raft::Term> led_terms_;
    std::uint64_t elections_ = 0;
    Tick leaderless_ = 0;
};

// Given: WASM and native builds must behave identically for a seed.
// Approach: a fixed script (crash the leader, writes and reads, a split, a
//           heal, a restart, the load generator, a full chaos run with its
//           linearizability check), one line per step with a hash of the
//           snapshot, then every event and the final snapshot. Both builds
//           print it, and CI compares.
std::string scenario_trace(std::uint64_t seed);

}  // namespace quorum::web
