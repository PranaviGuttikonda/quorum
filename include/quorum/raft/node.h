#pragma once

// Given: Raft leader election (§5.2) and log replication (§5.3–5.4) must be
//        testable under a deterministic simulator.
// Approach: RaftNode is a passive state machine with no I/O. It is driven
//           only by tick(), receive() and propose(). It sends through an
//           injected callback, applies committed entries through another, and
//           draws randomness only from its own Rng. RaftCluster wires it to
//           SimNetwork.

#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "quorum/raft/log.h"
#include "quorum/raft/messages.h"
#include "quorum/raft/storage.h"
#include "quorum/raft/types.h"
#include "quorum/sim/rng.h"

namespace quorum::raft {

struct RaftConfig {
    Tick election_timeout_min = 150;
    Tick election_timeout_max = 300;
    Tick heartbeat_interval = 50;  // must be < election_timeout_min
    std::size_t max_entries_per_append = 64;
    // §8: a new leader appends a no-op entry of its own term, so entries from
    // earlier terms commit as soon as the no-op does. Tests that exercise the
    // raw §5.4.2 commit rule (Figure 8) turn it off.
    bool append_noop_on_election = true;
};

// Throws std::invalid_argument unless 1 <= heartbeat < min <= max and
// max_entries_per_append >= 1.
void validate(const RaftConfig& config);

struct Proposed {
    LogIndex index = 0;
    Term term = 0;
    bool operator==(const Proposed&) const = default;
};
struct NotLeader {
    std::optional<NodeId> leader_hint;  // the leader this node follows, if known
    bool operator==(const NotLeader&) const = default;
};
using ProposeResult = std::variant<Proposed, NotLeader>;

class RaftNode {
public:
    using SendFn = std::function<void(NodeId to, std::string payload)>;
    using ApplyFn = std::function<void(LogIndex index, const LogEntry& entry)>;

    // Given: a node id, the ids of every other cluster member, and storage
    //        that may hold state from an earlier incarnation.
    // Approach: load currentTerm, votedFor and the log from storage, start as
    //           a follower, and arm a fresh randomized election timer.
    //           commitIndex and lastApplied start at 0 (volatile, Figure 2).
    //           Committed entries are re-applied once the node learns the
    //           commit index again.
    // Throws if `peers` contains `id` or a duplicate.
    RaftNode(NodeId id, std::vector<NodeId> peers, RaftConfig config, Storage& storage,
             SendFn send, sim::Rng rng, ApplyFn apply = {});

    // Advances this node's timers by one tick.
    void tick();

    // Handles one payload from `from`. Malformed payloads are ignored, and so
    // are payloads from nodes that aren't cluster members.
    void receive(NodeId from, std::string_view payload);

    // Given: a client command.
    // Approach: a leader appends it to its log (persisting it first), starts
    //           replicating it at once, and returns its index and term. That
    //           only means "accepted": the entry may still be lost if this
    //           leader is deposed before it commits. Anyone else returns
    //           NotLeader with a hint.
    ProposeResult propose(std::string command);

    NodeId id() const { return id_; }
    Role role() const { return role_; }
    Term current_term() const { return current_term_; }
    std::optional<NodeId> voted_for() const { return voted_for_; }
    // The leader this node currently follows, if it knows of one this term.
    std::optional<NodeId> leader_id() const { return leader_id_; }
    Tick election_timeout() const { return election_timeout_; }
    std::size_t cluster_size() const { return peers_.size() + 1; }

    const std::vector<LogEntry>& log() const { return log_; }
    LogIndex last_log_index() const { return log_.size(); }
    Term last_log_term() const { return term_at(last_log_index()); }
    // Term of the entry at `index`; 0 for index 0. Index must be <= last.
    Term term_at(LogIndex index) const { return index == 0 ? 0 : log_[index - 1].term; }
    LogIndex commit_index() const { return commit_index_; }
    LogIndex last_applied() const { return last_applied_; }
    // Number of conflict truncations since this node object was created.
    std::uint64_t truncations() const { return truncations_; }
    // Entries removed by those truncations.
    std::uint64_t truncated_entries() const { return truncated_entries_; }

    // Leader-only replication state (meaningless in other roles).
    LogIndex next_index(NodeId peer) const { return next_index_.at(peer); }
    LogIndex match_index(NodeId peer) const { return match_index_.at(peer); }

private:
    void handle(NodeId from, const RequestVote& m);
    void handle(NodeId from, const RequestVoteReply& m);
    void handle(NodeId from, const AppendEntries& m);
    void handle(NodeId from, const AppendEntriesReply& m);

    void start_election();
    void become_leader();
    void step_down(Term term);
    void broadcast_append();
    void send_append(NodeId peer);
    void advance_leader_commit();
    void apply_committed();
    void reset_election_timer();
    void persist_term_and_vote();
    void send(NodeId to, const RaftMessage& msg);
    bool has_majority() const;
    bool is_peer(NodeId id) const;

    const NodeId id_;
    const std::vector<NodeId> peers_;  // sorted, excludes id_
    const RaftConfig config_;
    Storage& storage_;
    const SendFn send_;
    const ApplyFn apply_;
    sim::Rng rng_;

    // Persistent: mirrored in storage_ before any message that depends on it.
    Term current_term_ = 0;
    std::optional<NodeId> voted_for_;
    std::vector<LogEntry> log_;

    // Volatile, all roles.
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_id_;
    std::set<NodeId> votes_;  // a set, so duplicate grants count once
    LogIndex commit_index_ = 0;
    LogIndex last_applied_ = 0;
    Tick election_elapsed_ = 0;
    Tick election_timeout_ = 0;
    Tick heartbeat_elapsed_ = 0;
    std::uint64_t truncations_ = 0;
    std::uint64_t truncated_entries_ = 0;

    // Volatile, leader only: reinitialized by become_leader().
    std::map<NodeId, LogIndex> next_index_;
    std::map<NodeId, LogIndex> match_index_;
};

}  // namespace quorum::raft
