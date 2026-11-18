#include "quorum/sim/network.h"

#include <algorithm>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace quorum::sim {

namespace {

// Heap comparator: std::push_heap/pop_heap build a max-heap, so "greater"
// gives a min-heap. Ids are unique, so the order is total and deterministic.
bool later(const auto& a, const auto& b) {
    if (a.msg.deliver_at != b.msg.deliver_at) return a.msg.deliver_at > b.msg.deliver_at;
    return a.msg.id > b.msg.id;
}

}  // namespace

const char* to_string(EventKind kind) {
    switch (kind) {
        case EventKind::Send: return "send";
        case EventKind::Drop: return "drop";
        case EventKind::Deliver: return "deliver";
    }
    return "?";
}

const char* to_string(DropReason reason) {
    switch (reason) {
        case DropReason::None: return "none";
        case DropReason::RandomLoss: return "random-loss";
        case DropReason::Partitioned: return "partitioned";
        case DropReason::SenderDown: return "sender-down";
        case DropReason::ReceiverDown: return "receiver-down";
    }
    return "?";
}

std::string to_string(const TraceEvent& e) {
    std::ostringstream os;
    os << "t=" << e.tick << ' ' << to_string(e.kind) << " #" << e.msg << ' ' << e.from << "->"
       << e.to;
    if (e.kind == EventKind::Drop) os << " (" << to_string(e.reason) << ')';
    return os.str();
}

SimNetwork::SimNetwork(std::uint64_t seed, NetworkConfig config) : config_(config), rng_(seed) {
    if (config_.min_delay < 1) throw std::invalid_argument("NetworkConfig: min_delay must be >= 1");
    if (config_.min_delay > config_.max_delay)
        throw std::invalid_argument("NetworkConfig: min_delay > max_delay");
    // Written so that NaN fails too.
    if (!(config_.drop_probability >= 0.0 && config_.drop_probability <= 1.0))
        throw std::invalid_argument("NetworkConfig: drop_probability must be in [0, 1]");
}

void SimNetwork::add_node(NodeId id, Handler handler) {
    if (!nodes_.emplace(id, Node{std::move(handler)}).second)
        throw std::invalid_argument("SimNetwork::add_node: duplicate node id");
}

SimNetwork::Node& SimNetwork::node(NodeId id) {
    auto it = nodes_.find(id);
    if (it == nodes_.end()) throw std::invalid_argument("SimNetwork: unknown node id");
    return it->second;
}

const SimNetwork::Node& SimNetwork::node(NodeId id) const {
    auto it = nodes_.find(id);
    if (it == nodes_.end()) throw std::invalid_argument("SimNetwork: unknown node id");
    return it->second;
}

void SimNetwork::record(EventKind kind, const Message& m, DropReason reason) {
    trace_.push_back(TraceEvent{clock_.now(), kind, m.id, m.from, m.to, reason});
}

void SimNetwork::drop(const Message& m, DropReason reason) {
    record(EventKind::Drop, m, reason);
    switch (reason) {
        case DropReason::RandomLoss: ++stats_.dropped_random; break;
        case DropReason::Partitioned: ++stats_.dropped_partition; break;
        case DropReason::SenderDown: ++stats_.dropped_sender_down; break;
        case DropReason::ReceiverDown: ++stats_.dropped_receiver_down; break;
        case DropReason::None: break;
    }
}

MessageId SimNetwork::send(NodeId from, NodeId to, std::string payload) {
    const Node& sender = node(from);
    const Node& receiver = node(to);

    const Tick delay = rng_.uniform(config_.min_delay, config_.max_delay);
    const bool lost = rng_.bernoulli(config_.drop_probability);

    Message m;
    m.id = next_id_++;
    m.from = from;
    m.to = to;
    m.sent_at = clock_.now();
    m.deliver_at = m.sent_at + delay;
    m.payload = std::move(payload);

    ++stats_.sent;
    record(EventKind::Send, m, DropReason::None);

    if (!sender.up) {
        drop(m, DropReason::SenderDown);
    } else if (lost) {
        drop(m, DropReason::RandomLoss);
    } else if (!can_reach(from, to)) {
        drop(m, DropReason::Partitioned);
    } else {
        queue_.push_back(Pending{std::move(m), receiver.incarnation});
        std::push_heap(queue_.begin(), queue_.end(), later<Pending, Pending>);
    }
    return next_id_ - 1;
}

void SimNetwork::partition(const std::vector<std::vector<NodeId>>& groups) {
    std::set<NodeId> seen;
    for (const auto& g : groups) {
        for (NodeId id : g) {
            (void)node(id);  // throws if unknown
            if (!seen.insert(id).second)
                throw std::invalid_argument("SimNetwork::partition: node in two groups");
        }
    }
    // Validate everything first, so a bad call leaves the old partition intact.
    for (auto& [id, n] : nodes_) n.group = 0;
    std::uint32_t label = 1;
    for (const auto& g : groups) {
        for (NodeId id : g) nodes_.at(id).group = label;
        ++label;
    }
}

void SimNetwork::heal() {
    for (auto& [id, n] : nodes_) n.group = 0;
}

bool SimNetwork::can_reach(NodeId from, NodeId to) const {
    return node(from).group == node(to).group;
}

void SimNetwork::crash(NodeId id) {
    Node& n = node(id);
    if (!n.up) return;
    n.up = false;
    ++n.incarnation;
}

void SimNetwork::restart(NodeId id) {
    node(id).up = true;
}

bool SimNetwork::is_up(NodeId id) const {
    return node(id).up;
}

void SimNetwork::tick() {
    clock_.advance(1);
    const Tick now = clock_.now();
    while (!queue_.empty() && queue_.front().msg.deliver_at <= now) {
        std::pop_heap(queue_.begin(), queue_.end(), later<Pending, Pending>);
        Pending p = std::move(queue_.back());
        queue_.pop_back();

        Node& receiver = nodes_.at(p.msg.to);
        if (!receiver.up || receiver.incarnation != p.receiver_incarnation) {
            drop(p.msg, DropReason::ReceiverDown);
        } else if (!can_reach(p.msg.from, p.msg.to)) {
            drop(p.msg, DropReason::Partitioned);
        } else {
            ++stats_.delivered;
            record(EventKind::Deliver, p.msg, DropReason::None);
            if (receiver.handler) receiver.handler(p.msg);
        }
    }
}

void SimNetwork::run_for(Tick ticks) {
    for (Tick i = 0; i < ticks; ++i) tick();
}

}  // namespace quorum::sim
