#pragma once

// Given: Raft leader election (paper §5.2) must be testable under a
//        deterministic simulator.
// Approach: RaftNode is a passive state machine with no I/O. It is driven
//           only by tick() and receive(), sends through an injected callback,
//           and draws randomness only from its own Rng. RaftCluster wires it
//           to SimNetwork.

#include <functional>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "quorum/raft/messages.h"
#include "quorum/raft/storage.h"
#include "quorum/raft/types.h"
#include "quorum/sim/rng.h"

namespace quorum::raft {

struct RaftConfig {
    Tick election_timeout_min = 150;
    Tick election_timeout_max = 300;
    Tick heartbeat_interval = 50;  // must be < election_timeout_min
};

// Throws std::invalid_argument unless 1 <= heartbeat < min <= max.
void validate(const RaftConfig& config);

class RaftNode {
public:
    using SendFn = std::function<void(NodeId to, std::string payload)>;

    // Given: a node id, the ids of every other cluster member, and storage
    //        that may hold state from an earlier incarnation.
    // Approach: load currentTerm and votedFor from storage, start as a
    //           follower, and arm a fresh randomized election timer.
    //           Everything else is volatile and starts empty.
    // Throws if `peers` contains `id` or a duplicate.
    RaftNode(NodeId id, std::vector<NodeId> peers, RaftConfig config, Storage& storage,
             SendFn send, sim::Rng rng);

    // Advances this node's timers by one tick.
    void tick();

    // Handles one payload from `from`. Malformed payloads are ignored, and so
    // are payloads from nodes that aren't cluster members.
    void receive(NodeId from, std::string_view payload);

    NodeId id() const { return id_; }
    Role role() const { return role_; }
    Term current_term() const { return current_term_; }
    std::optional<NodeId> voted_for() const { return voted_for_; }
    // The leader this node currently follows, if it knows of one this term.
    std::optional<NodeId> leader_id() const { return leader_id_; }
    Tick election_timeout() const { return election_timeout_; }
    std::size_t cluster_size() const { return peers_.size() + 1; }

private:
    void handle(NodeId from, const RequestVote& m);
    void handle(NodeId from, const RequestVoteReply& m);
    void handle(NodeId from, const AppendEntries& m);
    void handle(NodeId from, const AppendEntriesReply& m);

    void start_election();
    void become_leader();
    void step_down(Term term);
    void send_heartbeats();
    void reset_election_timer();
    void persist();
    void send(NodeId to, const RaftMessage& msg);
    bool has_majority() const;
    bool is_peer(NodeId id) const;

    // Log accessors. The log is empty until replication (step 3).
    LogIndex last_log_index() const { return 0; }
    Term last_log_term() const { return 0; }

    const NodeId id_;
    const std::vector<NodeId> peers_;  // sorted, excludes id_
    const RaftConfig config_;
    Storage& storage_;
    const SendFn send_;
    sim::Rng rng_;

    // Persistent (mirrored to storage_ before any reply that depends on it).
    Term current_term_ = 0;
    std::optional<NodeId> voted_for_;

    // Volatile.
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_id_;
    std::set<NodeId> votes_;  // a set, so duplicate grants count once
    Tick election_elapsed_ = 0;
    Tick election_timeout_ = 0;
    Tick heartbeat_elapsed_ = 0;
};

}  // namespace quorum::raft
