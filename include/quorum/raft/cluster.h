#pragma once

// Given: RaftNodes are I/O-free, and tests need a whole cluster on the
//        simulated network with crash/restart semantics.
// Approach: RaftCluster owns the SimNetwork, one Storage per node and the
//           live RaftNode objects. A crash destroys the node (all volatile
//           state is lost) and a restart builds a new one from its Storage.
//           One tick = deliver due messages, then tick every live node in id
//           order, then run the observers. Each node's applied entries are
//           recorded and asserted to arrive in index order. The record
//           restarts with the node, because the state machine is volatile
//           and is rebuilt from the log.

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "quorum/raft/node.h"
#include "quorum/raft/storage.h"
#include "quorum/sim/network.h"
#include "quorum/sim/rng.h"

namespace quorum::raft {

class RaftCluster {
public:
    using Observer = std::function<void(const RaftCluster&)>;

    // Nodes are numbered 1..size.
    RaftCluster(std::uint64_t seed, std::size_t size, RaftConfig raft = {},
                sim::NetworkConfig net = {});

    RaftCluster(const RaftCluster&) = delete;
    RaftCluster& operator=(const RaftCluster&) = delete;

    void tick();
    void run_for(Tick ticks);
    // Ticks until pred(*this) holds or max_ticks pass. Returns whether it held.
    bool run_until(const std::function<bool(const RaftCluster&)>& pred, Tick max_ticks);

    // Called after every tick, e.g. by an invariant checker.
    void add_observer(Observer observer);

    // Client entry point: propose on node `id`. A crashed node returns
    // NotLeader without a hint.
    ProposeResult propose(NodeId id, std::string command);

    void crash(NodeId id);
    void restart(NodeId id);
    bool is_up(NodeId id) const;

    void partition(const std::vector<std::vector<NodeId>>& groups) { net_.partition(groups); }
    void heal() { net_.heal(); }

    // Null while the node is crashed.
    const RaftNode* node(NodeId id) const;
    const std::vector<NodeId>& ids() const { return ids_; }
    Tick now() const { return net_.now(); }
    const sim::SimNetwork& network() const { return net_; }
    const MemoryStorage& storage(NodeId id) const { return *storage_.at(id); }
    // Entries applied by the current incarnation of `id`; [i] has index i + 1.
    const std::vector<LogEntry>& applied(NodeId id) const { return applied_.at(id); }
    // How many times `id` has been booted (1 after construction).
    std::uint64_t boot_count(NodeId id) const { return boots_.at(id); }

    // Live nodes that currently believe they are leader.
    std::vector<NodeId> leaders() const;
    // The single live leader, if exactly one exists and every live node it can
    // reach is a follower in its term and knows it as leader.
    std::optional<NodeId> stable_leader() const;

private:
    void boot(NodeId id);

    RaftConfig raft_config_;
    sim::SimNetwork net_;
    std::vector<NodeId> ids_;
    std::map<NodeId, std::unique_ptr<MemoryStorage>> storage_;
    std::map<NodeId, sim::Rng> node_rngs_;  // parent stream per node, forked per boot
    std::map<NodeId, std::unique_ptr<RaftNode>> nodes_;
    std::map<NodeId, std::vector<LogEntry>> applied_;
    std::map<NodeId, std::uint64_t> boots_;
    std::vector<Observer> observers_;
};

}  // namespace quorum::raft
