#include "quorum/raft/node.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace quorum::raft {

void validate(const RaftConfig& c) {
    if (c.heartbeat_interval < 1)
        throw std::invalid_argument("RaftConfig: heartbeat_interval must be >= 1");
    if (c.heartbeat_interval >= c.election_timeout_min)
        throw std::invalid_argument("RaftConfig: heartbeat_interval must be < election_timeout_min");
    if (c.election_timeout_min > c.election_timeout_max)
        throw std::invalid_argument("RaftConfig: election_timeout_min > election_timeout_max");
}

namespace {

std::vector<NodeId> sorted_peers(NodeId self, std::vector<NodeId> peers) {
    std::sort(peers.begin(), peers.end());
    if (std::adjacent_find(peers.begin(), peers.end()) != peers.end())
        throw std::invalid_argument("RaftNode: duplicate peer id");
    if (std::binary_search(peers.begin(), peers.end(), self))
        throw std::invalid_argument("RaftNode: peers must not include self");
    return peers;
}

}  // namespace

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers, RaftConfig config, Storage& storage,
                   SendFn send, sim::Rng rng)
    : id_(id),
      peers_(sorted_peers(id, std::move(peers))),
      config_(config),
      storage_(storage),
      send_(std::move(send)),
      rng_(rng) {
    validate(config_);
    const PersistentState s = storage_.load();
    current_term_ = s.current_term;
    voted_for_ = s.voted_for;
    reset_election_timer();
}

// Given: Figure 2: a follower or candidate starts an election when its
//        timeout elapses, and a leader sends heartbeats during idle periods.
// Approach: one counter per timer, incremented once per tick.
void RaftNode::tick() {
    if (role_ == Role::Leader) {
        if (++heartbeat_elapsed_ >= config_.heartbeat_interval) send_heartbeats();
        return;
    }
    if (++election_elapsed_ >= election_timeout_) start_election();
}

// Given: "If RPC request or response contains term T > currentTerm: set
//        currentTerm = T, convert to follower" (Figure 2, all servers).
// Approach: apply that rule once, centrally, before dispatching by type.
//           Every handler then only sees term <= currentTerm.
void RaftNode::receive(NodeId from, std::string_view payload) {
    if (!is_peer(from)) return;
    const auto msg = decode(payload);
    if (!msg) return;
    if (term_of(*msg) > current_term_) step_down(term_of(*msg));
    std::visit([&](const auto& m) { handle(from, m); }, *msg);
}

// Given: §5.2 + §5.4.1: grant if the term is current, we haven't voted for
//        someone else this term, and the candidate's log is at least as
//        up-to-date as ours.
// Approach: persist the vote before replying. Granting resets the election
//           timer (Figure 2, followers).
void RaftNode::handle(NodeId from, const RequestVote& m) {
    const bool log_ok = m.last_log_term > last_log_term() ||
                        (m.last_log_term == last_log_term() && m.last_log_index >= last_log_index());
    const bool can_vote = !voted_for_ || *voted_for_ == m.candidate_id;
    bool granted = false;
    if (m.term == current_term_ && can_vote && log_ok) {
        if (voted_for_ != m.candidate_id) {
            voted_for_ = m.candidate_id;
            persist();
        }
        reset_election_timer();
        granted = true;
    }
    send(from, RequestVoteReply{current_term_, granted});
}

// Given: a candidate wins once it holds votes from a majority of the full
//        cluster in its current term.
// Approach: ignore replies from older terms and from any role but candidate.
//           votes_ is a set, so a duplicated grant can't be counted twice.
void RaftNode::handle(NodeId from, const RequestVoteReply& m) {
    if (role_ != Role::Candidate || m.term != current_term_ || !m.vote_granted) return;
    votes_.insert(from);
    if (has_majority()) become_leader();
}

// Given: §5.2: a candidate that hears from a leader of its own term (or newer)
//        returns to follower. A stale leader gets rejected with our term.
// Approach: with an empty log the consistency check (prev index/term) always
//           passes, so any current-term AppendEntries succeeds.
void RaftNode::handle(NodeId from, const AppendEntries& m) {
    if (m.term < current_term_) {
        send(from, AppendEntriesReply{current_term_, false, 0, 0});
        return;
    }
    if (role_ == Role::Leader) {
        // Two leaders in one term would violate election safety. Election
        // safety says this can't happen, so reject without changing state.
        send(from, AppendEntriesReply{current_term_, false, 0, 0});
        return;
    }
    role_ = Role::Follower;
    leader_id_ = m.leader_id;
    reset_election_timer();
    send(from, AppendEntriesReply{current_term_, true, 0, 0});
}

void RaftNode::handle(NodeId, const AppendEntriesReply&) {
    // Only the term matters here; receive() has already applied it.
    // Replication (step 3) will use success/failure.
}

// Given: Figure 2, candidates: increment currentTerm, vote for self, reset the
//        election timer, and send RequestVote to all other servers.
// Approach: persist term and vote before any RequestVote leaves the node.
void RaftNode::start_election() {
    role_ = Role::Candidate;
    ++current_term_;
    voted_for_ = id_;
    leader_id_.reset();
    persist();
    votes_ = {id_};
    reset_election_timer();
    if (has_majority()) {  // single-node cluster
        become_leader();
        return;
    }
    const RequestVote rv{current_term_, id_, last_log_index(), last_log_term()};
    for (NodeId peer : peers_) send(peer, rv);
}

void RaftNode::become_leader() {
    role_ = Role::Leader;
    leader_id_ = id_;
    votes_.clear();
    send_heartbeats();  // assert leadership immediately (§5.2)
}

// Given: a higher term was observed.
// Approach: adopt it, clear the vote and become a follower. A deposed leader
//           re-arms its election timer, which wasn't running while it led. A
//           candidate keeps its running timer, as in Figure 2.
void RaftNode::step_down(Term term) {
    const bool was_leader = role_ == Role::Leader;
    current_term_ = term;
    voted_for_.reset();
    role_ = Role::Follower;
    leader_id_.reset();
    votes_.clear();
    persist();
    if (was_leader) reset_election_timer();
}

void RaftNode::send_heartbeats() {
    heartbeat_elapsed_ = 0;
    const AppendEntries ae{current_term_, id_, last_log_index(), last_log_term(), 0, {}};
    for (NodeId peer : peers_) send(peer, ae);
}

// Approach: a fresh timeout, uniform in [min, max], for every reset (§5.2).
//           This keeps split votes from repeating in lockstep.
void RaftNode::reset_election_timer() {
    election_elapsed_ = 0;
    election_timeout_ = rng_.uniform(config_.election_timeout_min, config_.election_timeout_max);
}

void RaftNode::persist() {
    storage_.save_term_and_vote(current_term_, voted_for_);
}

void RaftNode::send(NodeId to, const RaftMessage& msg) {
    send_(to, encode(msg));
}

bool RaftNode::has_majority() const {
    return votes_.size() * 2 > cluster_size();
}

bool RaftNode::is_peer(NodeId id) const {
    return std::binary_search(peers_.begin(), peers_.end(), id);
}

}  // namespace quorum::raft
