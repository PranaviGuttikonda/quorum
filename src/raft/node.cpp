#include "quorum/raft/node.h"

#include <algorithm>
#include <span>
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
    if (c.max_entries_per_append < 1)
        throw std::invalid_argument("RaftConfig: max_entries_per_append must be >= 1");
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
                   SendFn send, sim::Rng rng, ApplyFn apply)
    : id_(id),
      peers_(sorted_peers(id, std::move(peers))),
      config_(config),
      storage_(storage),
      send_(std::move(send)),
      apply_(std::move(apply)),
      rng_(rng) {
    validate(config_);
    PersistentState s = storage_.load();
    current_term_ = s.current_term;
    voted_for_ = s.voted_for;
    log_ = std::move(s.log);
    reset_election_timer();
}

// Given: Figure 2: a follower or candidate starts an election when its
//        timeout elapses, and a leader sends heartbeats during idle periods.
// Approach: one counter per timer, incremented once per tick.
void RaftNode::tick() {
    if (role_ == Role::Leader) {
        if (++heartbeat_elapsed_ >= config_.heartbeat_interval) broadcast_append();
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

ProposeResult RaftNode::propose(std::string command) {
    if (role_ != Role::Leader) return NotLeader{leader_id_};
    LogEntry entry{current_term_, std::move(command)};
    storage_.write_log(last_log_index(), std::span<const LogEntry>(&entry, 1));
    log_.push_back(std::move(entry));
    const Proposed result{last_log_index(), current_term_};
    advance_leader_commit();  // a single-node cluster commits at once
    broadcast_append();
    return result;
}

// Given: §5.2 + §5.4.1: grant if the term is current, we haven't voted for
//        someone else this term, and the candidate's log is at least as
//        up-to-date as ours: a higher last term, or the same last term and
//        at least as long.
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
            persist_term_and_vote();
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

// Given: Figure 2, AppendEntries receiver, plus a network that duplicates and
//        reorders messages.
// Approach:
//   1. Reject a stale term. A candidate of the same term yields to the leader.
//   2. Reject if the log has no entry at prevLogIndex with prevLogTerm. The
//      reply carries our log length as a backtracking hint.
//   3. Skip entries we already hold with the same term. Truncate only at the
//      first conflicting term, then append the rest. A duplicated or stale
//      request from this leader therefore never removes entries: it is a
//      no-op, so handling is idempotent.
//   4. Persist before replying. Advance commitIndex to
//      min(leaderCommit, last index this request proved), never backwards.
void RaftNode::handle(NodeId from, const AppendEntries& m) {
    if (m.term < current_term_) {
        send(from, AppendEntriesReply{current_term_, false, 0, last_log_index()});
        return;
    }
    if (role_ == Role::Leader) {
        // Two leaders in one term would violate election safety. Election
        // safety says this can't happen, so reject without changing state.
        send(from, AppendEntriesReply{current_term_, false, 0, last_log_index()});
        return;
    }
    role_ = Role::Follower;
    leader_id_ = m.leader_id;
    reset_election_timer();

    if (m.prev_log_index > last_log_index() || term_at(m.prev_log_index) != m.prev_log_term) {
        send(from, AppendEntriesReply{current_term_, false, 0, last_log_index()});
        return;
    }

    std::size_t k = 0;
    for (; k < m.entries.size(); ++k) {
        const LogIndex index = m.prev_log_index + 1 + k;
        if (index > last_log_index() || term_at(index) != m.entries[k].term) break;
    }
    if (k < m.entries.size()) {
        const LogIndex keep = m.prev_log_index + k;
        if (keep < last_log_index()) {
            // Committed entries are never in conflict with a current leader
            // (Leader Completeness). Hitting this is a safety bug.
            if (keep < commit_index_)
                throw std::logic_error("RaftNode: conflict truncation below commitIndex");
            ++truncations_;
        }
        const std::span<const LogEntry> fresh(m.entries.data() + k, m.entries.size() - k);
        storage_.write_log(keep, fresh);
        log_.resize(keep);
        log_.insert(log_.end(), fresh.begin(), fresh.end());
    }

    const LogIndex proven = m.prev_log_index + m.entries.size();
    if (m.leader_commit > commit_index_)
        commit_index_ = std::max(commit_index_, std::min(m.leader_commit, proven));
    apply_committed();
    send(from, AppendEntriesReply{current_term_, true, proven, last_log_index()});
}

// Given: replies can be duplicated, reordered or stale (from earlier requests
//        of this same term).
// Approach: matchIndex only moves forward, and nextIndex stays above it. On
//           failure, back up one entry, or straight to just past the
//           follower's log if that is shorter, but never at or below
//           matchIndex. Then resend at once instead of waiting for the
//           heartbeat.
void RaftNode::handle(NodeId from, const AppendEntriesReply& m) {
    if (role_ != Role::Leader || m.term != current_term_) return;
    LogIndex& match = match_index_.at(from);
    LogIndex& next = next_index_.at(from);
    if (m.success) {
        match = std::max(match, std::min(m.match_index, last_log_index()));
        next = std::max(next, match + 1);
        advance_leader_commit();
        if (next <= last_log_index()) send_append(from);
    } else {
        const LogIndex decremented = next > 1 ? next - 1 : 1;
        next = std::max(match + 1, std::min(decremented, m.last_log_index + 1));
        send_append(from);
    }
}

// Given: Figure 2, candidates: increment currentTerm, vote for self, reset the
//        election timer, and send RequestVote to all other servers.
// Approach: persist term and vote before any RequestVote leaves the node.
void RaftNode::start_election() {
    role_ = Role::Candidate;
    ++current_term_;
    voted_for_ = id_;
    leader_id_.reset();
    persist_term_and_vote();
    votes_ = {id_};
    reset_election_timer();
    if (has_majority()) {  // single-node cluster
        become_leader();
        return;
    }
    const RequestVote rv{current_term_, id_, last_log_index(), last_log_term()};
    for (NodeId peer : peers_) send(peer, rv);
}

// Approach: nextIndex starts just past our last entry and matchIndex at 0
//           (Figure 2, volatile leader state). The no-op (§8) is appended
//           after nextIndex is set, so the first AppendEntries round, sent at
//           once to assert leadership (§5.2), already carries it.
void RaftNode::become_leader() {
    role_ = Role::Leader;
    leader_id_ = id_;
    votes_.clear();
    for (NodeId peer : peers_) {
        next_index_[peer] = last_log_index() + 1;
        match_index_[peer] = 0;
    }
    if (config_.append_noop_on_election) {
        const LogEntry noop = LogEntry::noop(current_term_);
        storage_.write_log(last_log_index(), std::span<const LogEntry>(&noop, 1));
        log_.push_back(noop);
        advance_leader_commit();  // a single-node cluster commits at once
    }
    broadcast_append();
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
    persist_term_and_vote();
    if (was_leader) reset_election_timer();
}

void RaftNode::broadcast_append() {
    heartbeat_elapsed_ = 0;
    for (NodeId peer : peers_) send_append(peer);
}

// Approach: send everything from nextIndex up to max_entries_per_append
//           entries. With nothing to send it is a heartbeat that still
//           carries prevLogIndex/prevLogTerm and leaderCommit.
void RaftNode::send_append(NodeId peer) {
    LogIndex& next = next_index_.at(peer);
    next = std::min(next, last_log_index() + 1);
    const LogIndex prev = next - 1;
    AppendEntries ae{current_term_, id_, prev, term_at(prev), commit_index_, {}};
    const LogIndex last = std::min<LogIndex>(last_log_index(), prev + config_.max_entries_per_append);
    for (LogIndex i = next; i <= last; ++i) ae.entries.push_back(log_[i - 1]);
    send(peer, ae);
}

// Given: §5.4.2 and Figure 8: replica counting can only commit an entry from
//        the leader's current term. Older entries commit indirectly, through
//        Log Matching, once a current-term entry above them commits.
// Approach: scan down from the last entry. Terms never decrease along the
//           log, so the first older-term entry ends the scan.
void RaftNode::advance_leader_commit() {
    for (LogIndex n = last_log_index(); n > commit_index_; --n) {
        if (term_at(n) != current_term_) break;
        std::size_t replicas = 1;  // the leader itself
        for (NodeId peer : peers_)
            if (match_index_.at(peer) >= n) ++replicas;
        if (replicas * 2 > cluster_size()) {
            commit_index_ = n;
            apply_committed();
            return;
        }
    }
}

// Approach: apply strictly in index order, each entry once per incarnation.
void RaftNode::apply_committed() {
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        if (apply_) apply_(last_applied_, log_[last_applied_ - 1]);
    }
}

// Approach: a fresh timeout, uniform in [min, max], for every reset (§5.2).
//           This keeps split votes from repeating in lockstep.
void RaftNode::reset_election_timer() {
    election_elapsed_ = 0;
    election_timeout_ = rng_.uniform(config_.election_timeout_min, config_.election_timeout_max);
}

void RaftNode::persist_term_and_vote() {
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
