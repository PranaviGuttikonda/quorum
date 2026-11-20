#pragma once

// Given: RaftNodes are I/O-free, and tests need a whole cluster on the
//        simulated network with crash/restart semantics.
// Approach: RaftCluster owns the SimNetwork and the live RaftNode objects,
//           and gets each boot's Storage from a StorageProvider. A crash
//           destroys the node (all volatile state is lost) and tells the
//           provider, and a restart builds a new node from what the provider
//           recovers.
//           One tick = deliver due messages, then tick every live node in id
//           order, then run the observers. Each node's applied entries are
//           recorded and asserted to arrive in index order. The record
//           restarts with the node, because the state machine is volatile
//           and is rebuilt from the log.
//
//           Servers are nodes 1..size. With a ServiceFactory, each boot also
//           creates a Service beside the node. Messages from servers go to
//           Raft, and messages from anyone else (clients) go to the Service.
//           Clients are global network nodes: partitions only split servers.
//
//           Durability barrier: every send from a server, by its node or its
//           service, first calls Storage::sync(). Nothing a server says can
//           depend on a write that a crash could still undo.
//
//           A disk can lose power in the middle of a call (sim::DiskCrash).
//           The node is then mid-step and unusable, so it is crashed as soon
//           as the call unwinds. Its remaining deliveries in that tick are
//           dropped. A DiskCrash during recovery leaves the node down.

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "quorum/raft/node.h"
#include "quorum/raft/service.h"
#include "quorum/raft/storage.h"
#include "quorum/sim/network.h"
#include "quorum/sim/rng.h"

namespace quorum::raft {

// Owns each node's durable state across incarnations.
class StorageProvider {
public:
    virtual ~StorageProvider() = default;
    // Recovers the Storage for a new incarnation of `id`. It stays valid
    // until crash(id).
    virtual Storage& boot(NodeId id) = 0;
    // `id` crashed (power loss): its Storage is no longer used.
    virtual void crash(NodeId id) = 0;
};

// MemoryStorage per node. A crash loses nothing.
class MemoryStorageProvider final : public StorageProvider {
public:
    Storage& boot(NodeId id) override;
    void crash(NodeId) override {}
    const MemoryStorage& storage(NodeId id) const { return *storage_.at(id); }

private:
    std::map<NodeId, std::unique_ptr<MemoryStorage>> storage_;
};

class RaftCluster {
public:
    using Observer = std::function<void(const RaftCluster&)>;

    // Nodes are numbered 1..size. Without a provider, nodes use MemoryStorage.
    RaftCluster(std::uint64_t seed, std::size_t size, RaftConfig raft = {},
                sim::NetworkConfig net = {}, ServiceFactory services = {},
                std::shared_ptr<StorageProvider> storage = nullptr);

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
    // Does nothing if `id` is up. Recovery that loses power partway leaves it down.
    void restart(NodeId id);
    bool is_up(NodeId id) const;

    void partition(const std::vector<std::vector<NodeId>>& groups) { net_.partition(groups); }
    void heal() { net_.heal(); }
    // Takes a node in or out of the partition model (clients start global).
    void set_global(NodeId id, bool global) { net_.set_global(id, global); }

    // Registers a client node. Its id must not be a server id. It starts global.
    void add_client(NodeId id, std::function<void(const sim::Message&)> handler);
    // Sends from a registered client.
    void client_send(NodeId from, NodeId to, std::string payload);
    bool is_server(NodeId id) const { return id >= 1 && id <= ids_.size(); }
    const std::set<NodeId>& clients() const { return clients_; }

    // Null while the node is crashed.
    const RaftNode* node(NodeId id) const;
    // Null while crashed, or when there is no ServiceFactory.
    const Service* service(NodeId id) const { return services_.at(id).get(); }
    const std::vector<NodeId>& ids() const { return ids_; }
    Tick now() const { return net_.now(); }
    const sim::SimNetwork& network() const { return net_; }
    // See SimNetwork::clear_trace.
    void clear_trace() { net_.clear_trace(); }
    StorageProvider& storage() const { return *storage_; }
    // Entries `id` has removed by conflict truncation, over all incarnations.
    std::uint64_t truncated_entries(NodeId id) const;
    // Crashes caused by a disk losing power in the middle of a call.
    std::uint64_t disk_crashes() const { return disk_crashes_; }
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
    void crash_doomed();

    RaftConfig raft_config_;
    sim::SimNetwork net_;
    std::vector<NodeId> ids_;
    std::shared_ptr<StorageProvider> storage_;
    std::map<NodeId, Storage*> live_storage_;  // the current incarnation's
    std::map<NodeId, sim::Rng> node_rngs_;  // parent stream per node, forked per boot
    ServiceFactory service_factory_;
    std::map<NodeId, std::unique_ptr<RaftNode>> nodes_;
    std::map<NodeId, std::unique_ptr<Service>> services_;  // destroyed before nodes_
    std::set<NodeId> clients_;
    std::map<NodeId, std::vector<LogEntry>> applied_;
    std::map<NodeId, std::uint64_t> boots_;
    std::map<NodeId, std::uint64_t> truncated_;  // from earlier incarnations
    std::set<NodeId> doomed_;                    // lost power mid-call this tick
    std::uint64_t disk_crashes_ = 0;
    std::vector<Observer> observers_;
};

}  // namespace quorum::raft
