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

#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "quorum/kv/cluster.h"

namespace quorum::web {

using raft::NodeId;
using sim::Tick;

class Simulation {
public:
    static constexpr std::size_t kClients = 2;
    static constexpr std::size_t kLogTail = 24;  // entries per server in a snapshot
    static constexpr std::size_t kOpTail = 30;   // operations in a snapshot
    static constexpr Tick kDropMemory = 12;      // ticks a drop stays in snapshots

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

    std::string snapshot() const;
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
    };
    struct Drop {
        Tick at = 0;
        NodeId from = 0;
        NodeId to = 0;
        sim::DropReason reason = sim::DropReason::None;
    };
    const std::vector<WebOp>& ops() const { return ops_; }
    const std::deque<Drop>& drops() const { return drops_; }

private:
    void start_queued();
    void collect();

    std::uint64_t seed_;
    kv::KvCluster kv_;
    std::vector<WebOp> ops_;
    std::vector<std::deque<std::size_t>> queued_;  // per client: indices into ops_
    std::vector<std::optional<std::size_t>> running_;
    std::deque<Drop> drops_;
};

// Given: WASM and native builds must behave identically for a seed.
// Approach: a fixed script (crash the leader, writes and reads, a split, a
//           heal, a restart), one line per step with a hash of the snapshot,
//           then the final snapshot. Both builds print it, and CI compares.
std::string scenario_trace(std::uint64_t seed);

}  // namespace quorum::web
