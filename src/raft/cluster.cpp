#include "quorum/raft/cluster.h"

#include <stdexcept>
#include <utility>

#include "quorum/sim/disk.h"

namespace quorum::raft {

Storage& MemoryStorageProvider::boot(NodeId id) {
    auto& s = storage_[id];
    if (!s) s = std::make_unique<MemoryStorage>();
    return *s;
}

// Given: one seed must determine the network and every node's randomness.
// Approach: a root Rng gives the network its seed, then forks one parent
//           stream per node. Each boot of a node forks again from its parent,
//           so a restarted node gets fresh but reproducible timeouts.
RaftCluster::RaftCluster(std::uint64_t seed, std::size_t size, RaftConfig raft,
                         sim::NetworkConfig net, ServiceFactory services,
                         std::shared_ptr<StorageProvider> storage)
    : raft_config_(raft),
      net_(sim::Rng(seed).next_u64(), net),
      storage_(storage ? std::move(storage) : std::make_shared<MemoryStorageProvider>()),
      service_factory_(std::move(services)) {
    if (size == 0) throw std::invalid_argument("RaftCluster: size must be >= 1");
    validate(raft_config_);
    sim::Rng root(seed);
    (void)root.next_u64();  // consumed by the network seed above

    for (NodeId id = 1; id <= size; ++id) ids_.push_back(id);
    for (NodeId id : ids_) {
        node_rngs_.emplace(id, root.fork());
        services_[id];
        net_.add_node(id, [this, id](const sim::Message& m) {
            // SimNetwork never delivers to a crashed node, but stay defensive.
            auto it = nodes_.find(id);
            if (it == nodes_.end() || !it->second || doomed_.count(id)) return;
            try {
                if (is_server(m.from)) {
                    it->second->receive(m.from, m.payload);
                } else if (const auto& svc = services_.at(id)) {
                    svc->on_client_message(m.from, m.payload);
                }
            } catch (const sim::DiskCrash&) {
                // Crashing here would change the network mid-delivery.
                doomed_.insert(id);
            }
        });
    }
    for (NodeId id : ids_) boot(id);
}

void RaftCluster::boot(NodeId id) {
    Storage& storage = storage_->boot(id);  // recovery; may throw
    live_storage_[id] = &storage;
    std::vector<NodeId> peers;
    for (NodeId p : ids_)
        if (p != id) peers.push_back(p);
    auto send = [this, id](NodeId to, std::string payload) {
        live_storage_.at(id)->sync();  // the durability barrier
        net_.send(id, to, std::move(payload));
    };
    auto apply = [this, id](LogIndex index, const LogEntry& entry) {
        auto& applied = applied_.at(id);
        if (index != applied.size() + 1)
            throw std::logic_error("RaftCluster: node applied out of order");
        applied.push_back(entry);
        if (const auto& svc = services_.at(id)) svc->on_apply(index, entry);
    };
    applied_[id].clear();
    ++boots_[id];
    nodes_[id] = std::make_unique<RaftNode>(id, std::move(peers), raft_config_, storage,
                                            send, node_rngs_.at(id).fork(), std::move(apply));
    if (service_factory_) services_[id] = service_factory_(id, *nodes_[id], send);
}

void RaftCluster::add_client(NodeId id, std::function<void(const sim::Message&)> handler) {
    if (is_server(id)) throw std::invalid_argument("RaftCluster::add_client: id is a server");
    net_.add_node(id, std::move(handler));
    net_.set_global(id, true);
    clients_.insert(id);
}

void RaftCluster::client_send(NodeId from, NodeId to, std::string payload) {
    if (!clients_.count(from)) throw std::invalid_argument("RaftCluster::client_send: not a client");
    net_.send(from, to, std::move(payload));
}

void RaftCluster::tick() {
    net_.tick();
    crash_doomed();
    for (NodeId id : ids_) {
        if (auto& n = nodes_.at(id)) {
            try {
                n->tick();
            } catch (const sim::DiskCrash&) {
                doomed_.insert(id);
            }
        }
    }
    crash_doomed();
    for (const auto& obs : observers_) obs(*this);
}

void RaftCluster::crash_doomed() {
    const std::set<NodeId> doomed = std::move(doomed_);
    doomed_.clear();
    for (NodeId id : doomed) {
        crash(id);
        ++disk_crashes_;
    }
}

void RaftCluster::run_for(Tick ticks) {
    for (Tick i = 0; i < ticks; ++i) tick();
}

bool RaftCluster::run_until(const std::function<bool(const RaftCluster&)>& pred, Tick max_ticks) {
    for (Tick i = 0; i < max_ticks; ++i) {
        if (pred(*this)) return true;
        tick();
    }
    return pred(*this);
}

void RaftCluster::add_observer(Observer observer) {
    observers_.push_back(std::move(observer));
}

ProposeResult RaftCluster::propose(NodeId id, std::string command) {
    auto& n = nodes_.at(id);
    if (!n) return NotLeader{std::nullopt};
    try {
        return n->propose(std::move(command));
    } catch (const sim::DiskCrash&) {
        crash(id);
        ++disk_crashes_;
        return NotLeader{std::nullopt};
    }
}

void RaftCluster::crash(NodeId id) {
    auto& n = nodes_.at(id);
    if (!n) return;
    net_.crash(id);
    services_.at(id).reset();  // it may refer to the node
    truncated_[id] += n->truncated_entries();
    n.reset();
    live_storage_.erase(id);
    storage_->crash(id);
}

void RaftCluster::restart(NodeId id) {
    if (nodes_.at(id)) return;
    try {
        boot(id);
    } catch (const sim::DiskCrash&) {
        live_storage_.erase(id);
        storage_->crash(id);
        ++disk_crashes_;
        return;
    }
    net_.restart(id);
}

std::uint64_t RaftCluster::truncated_entries(NodeId id) const {
    const RaftNode* n = node(id);
    const auto it = truncated_.find(id);
    return (it == truncated_.end() ? 0 : it->second) + (n ? n->truncated_entries() : 0);
}

bool RaftCluster::is_up(NodeId id) const {
    return nodes_.at(id) != nullptr;
}

const RaftNode* RaftCluster::node(NodeId id) const {
    return nodes_.at(id).get();
}

std::vector<NodeId> RaftCluster::leaders() const {
    std::vector<NodeId> out;
    for (NodeId id : ids_)
        if (const RaftNode* n = node(id); n && n->role() == Role::Leader) out.push_back(id);
    return out;
}

std::optional<NodeId> RaftCluster::stable_leader() const {
    const auto ls = leaders();
    if (ls.size() != 1) return std::nullopt;
    const RaftNode& leader = *node(ls[0]);
    for (NodeId id : ids_) {
        const RaftNode* n = node(id);
        if (!n || id == leader.id() || !net_.can_reach(leader.id(), id)) continue;
        if (n->role() != Role::Follower || n->current_term() != leader.current_term() ||
            n->leader_id() != leader.id())
            return std::nullopt;
    }
    return leader.id();
}

}  // namespace quorum::raft
