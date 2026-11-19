#include "quorum/raft/cluster.h"

#include <stdexcept>
#include <utility>

namespace quorum::raft {

// Given: one seed must determine the network and every node's randomness.
// Approach: a root Rng gives the network its seed, then forks one parent
//           stream per node. Each boot of a node forks again from its parent,
//           so a restarted node gets fresh but reproducible timeouts.
RaftCluster::RaftCluster(std::uint64_t seed, std::size_t size, RaftConfig raft,
                         sim::NetworkConfig net)
    : raft_config_(raft), net_(sim::Rng(seed).next_u64(), net) {
    if (size == 0) throw std::invalid_argument("RaftCluster: size must be >= 1");
    validate(raft_config_);
    sim::Rng root(seed);
    (void)root.next_u64();  // consumed by the network seed above

    for (NodeId id = 1; id <= size; ++id) ids_.push_back(id);
    for (NodeId id : ids_) {
        storage_.emplace(id, std::make_unique<MemoryStorage>());
        node_rngs_.emplace(id, root.fork());
        net_.add_node(id, [this, id](const sim::Message& m) {
            // SimNetwork never delivers to a crashed node, but stay defensive.
            if (auto it = nodes_.find(id); it != nodes_.end() && it->second)
                it->second->receive(m.from, m.payload);
        });
    }
    for (NodeId id : ids_) boot(id);
}

void RaftCluster::boot(NodeId id) {
    std::vector<NodeId> peers;
    for (NodeId p : ids_)
        if (p != id) peers.push_back(p);
    auto send = [this, id](NodeId to, std::string payload) { net_.send(id, to, std::move(payload)); };
    auto apply = [this, id](LogIndex index, const LogEntry& entry) {
        auto& applied = applied_.at(id);
        if (index != applied.size() + 1)
            throw std::logic_error("RaftCluster: node applied out of order");
        applied.push_back(entry);
    };
    applied_[id].clear();
    ++boots_[id];
    nodes_[id] = std::make_unique<RaftNode>(id, std::move(peers), raft_config_, *storage_.at(id),
                                            std::move(send), node_rngs_.at(id).fork(),
                                            std::move(apply));
}

void RaftCluster::tick() {
    net_.tick();
    for (NodeId id : ids_)
        if (auto& n = nodes_.at(id)) n->tick();
    for (const auto& obs : observers_) obs(*this);
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
    return n->propose(std::move(command));
}

void RaftCluster::crash(NodeId id) {
    auto& n = nodes_.at(id);
    if (!n) return;
    net_.crash(id);
    n.reset();
}

void RaftCluster::restart(NodeId id) {
    if (nodes_.at(id)) return;
    net_.restart(id);
    boot(id);
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
