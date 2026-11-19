#pragma once

// Given: Raft must be tested against delay, loss, partitions and crashes, and
//        every failing run must be replayable.
// Approach: a single-threaded message queue ordered by (deliver_at, id). The
//           network owns its clock and a seeded Rng. Each send draws its delay
//           and its loss decision from that Rng, so a run is a pure function of
//           the seed and the sequence of API calls.

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "quorum/sim/clock.h"
#include "quorum/sim/rng.h"

namespace quorum::sim {

using NodeId = std::uint32_t;
using MessageId = std::uint64_t;

struct NetworkConfig {
    Tick min_delay = 1;  // must be >= 1: nothing arrives in the tick it was sent
    Tick max_delay = 10;
    double drop_probability = 0.0;       // in [0, 1]
    double duplicate_probability = 0.0;  // in [0, 1]: a delivered message arrives twice
};

struct Message {
    MessageId id = 0;
    NodeId from = 0;
    NodeId to = 0;
    Tick sent_at = 0;
    Tick deliver_at = 0;
    std::string payload;
};

enum class DropReason : std::uint8_t {
    None,
    RandomLoss,    // lost with probability p
    Partitioned,   // sender and receiver in different partition groups
    SenderDown,    // sent by a crashed node
    ReceiverDown,  // receiver crashed, or restarted since the message was sent
};

// Duplicate is recorded at send time when a second copy is scheduled. Both
// copies then get their own Deliver or Drop event.
enum class EventKind : std::uint8_t { Send, Duplicate, Drop, Deliver };

struct TraceEvent {
    Tick tick = 0;
    EventKind kind = EventKind::Send;
    MessageId msg = 0;
    NodeId from = 0;
    NodeId to = 0;
    DropReason reason = DropReason::None;

    bool operator==(const TraceEvent&) const = default;
};

struct NetworkStats {
    std::uint64_t sent = 0;
    std::uint64_t duplicated = 0;
    std::uint64_t delivered = 0;
    std::uint64_t dropped_random = 0;
    std::uint64_t dropped_partition = 0;
    std::uint64_t dropped_sender_down = 0;
    std::uint64_t dropped_receiver_down = 0;

    std::uint64_t dropped() const {
        return dropped_random + dropped_partition + dropped_sender_down + dropped_receiver_down;
    }
};

const char* to_string(EventKind kind);
const char* to_string(DropReason reason);
std::string to_string(const TraceEvent& event);

class SimNetwork {
public:
    using Handler = std::function<void(const Message&)>;

    // Throws std::invalid_argument if the config is out of range.
    SimNetwork(std::uint64_t seed, NetworkConfig config);

    // Registers a node. Throws if the id is already registered.
    void add_node(NodeId id, Handler handler);

    // Given: a message from `from` to `to`.
    // Approach: always draw the delay, the loss decision, the duplicate
    //           decision and the copy's delay, then apply the faults. RNG
    //           consumption per send stays constant, so a partition or crash
    //           doesn't shift the randomness of later messages. A duplicate is
    //           an independent second copy with its own delay, so it can arrive
    //           before the original. A lost message has no copies.
    // Throws if either node is unknown. Returns the message id.
    MessageId send(NodeId from, NodeId to, std::string payload);

    // Given: disjoint groups of known node ids.
    // Approach: give each group a label. Unlisted nodes share one extra group.
    //           Reachability is checked at send and again at delivery, so
    //           in-flight messages crossing a new partition are dropped too.
    // Throws if a node is unknown or appears twice. Replaces any earlier partition.
    void partition(const std::vector<std::vector<NodeId>>& groups);
    void heal();
    bool can_reach(NodeId from, NodeId to) const;

    // Given: a crashed node must receive nothing, including messages already in
    //        flight to it.
    // Approach: each crash bumps the node's incarnation. A message records its
    //           receiver's incarnation at send time and is dropped on delivery
    //           if the receiver is down or has restarted since. Messages the
    //           node sent before crashing are already on the wire and still
    //           arrive.
    void crash(NodeId id);
    void restart(NodeId id);
    bool is_up(NodeId id) const;

    // Advances the clock by one tick and delivers every message due at the new
    // tick. Handlers may call send(); new messages arrive at a later tick.
    void tick();
    void run_for(Tick ticks);

    Tick now() const { return clock_.now(); }
    std::size_t in_flight() const { return queue_.size(); }
    const std::vector<TraceEvent>& trace() const { return trace_; }
    const NetworkStats& stats() const { return stats_; }

private:
    struct Node {
        Handler handler;
        bool up = true;
        std::uint64_t incarnation = 0;
        std::uint32_t group = 0;
    };

    struct Pending {
        Message msg;
        std::uint64_t receiver_incarnation = 0;
        std::uint8_t copy = 0;  // 0 = original, 1 = duplicate; tie-break for the heap
    };

    Node& node(NodeId id);
    const Node& node(NodeId id) const;
    void record(EventKind kind, const Message& m, DropReason reason);
    void drop(const Message& m, DropReason reason);

    NetworkConfig config_;
    Rng rng_;
    SimClock clock_;
    std::map<NodeId, Node> nodes_;  // ordered: iteration is deterministic
    std::vector<Pending> queue_;    // min-heap on (deliver_at, id, copy)
    MessageId next_id_ = 1;
    std::vector<TraceEvent> trace_;
    NetworkStats stats_;
};

}  // namespace quorum::sim
