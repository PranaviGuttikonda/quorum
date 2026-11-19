// Given: the election rules (§5.2) pinned down on a single node.
// Approach: NodeHarness drives the node by hand; no network involved.

#include "quorum/raft/node.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "harness.h"
#include "node_harness.h"

using namespace quorum::raft;
using quorum::sim::Rng;

using Harness = quorum::raft::testing::NodeHarness;

TEST(raft_node_config_is_validated) {
    CHECK_THROWS(validate(RaftConfig{150, 300, 0}));
    CHECK_THROWS(validate(RaftConfig{150, 300, 150}));  // heartbeat must be < min timeout
    CHECK_THROWS(validate(RaftConfig{300, 150, 50}));
    validate(RaftConfig{150, 150, 149});  // fixed timeout is allowed

    MemoryStorage s;
    auto noop = [](NodeId, std::string) {};
    CHECK_THROWS(RaftNode(1, {2, 1}, {}, s, noop, Rng(1)));  // self in peers
    CHECK_THROWS(RaftNode(1, {2, 2}, {}, s, noop, Rng(1)));  // duplicate peer
}

TEST(raft_node_starts_as_follower_in_term_zero) {
    Harness h;
    CHECK(h.node->role() == Role::Follower);
    CHECK_EQ(h.node->current_term(), Term{0});
    CHECK(!h.node->voted_for().has_value());
    CHECK(!h.node->leader_id().has_value());
    CHECK(h.outbox.empty());
}

TEST(raft_node_timeout_is_randomized_within_range) {
    std::vector<Tick> seen;
    for (std::uint64_t seed = 0; seed < 200; ++seed) {
        Harness h({2, 3}, seed);
        const Tick t = h.node->election_timeout();
        CHECK(t >= h.config.election_timeout_min && t <= h.config.election_timeout_max);
        seen.push_back(t);
    }
    std::sort(seen.begin(), seen.end());
    const auto distinct = std::unique(seen.begin(), seen.end()) - seen.begin();
    CHECK(distinct > 50);  // not stuck on a few values
}

TEST(raft_node_follower_times_out_and_requests_votes) {
    Harness h;
    const Tick expected = h.node->election_timeout();
    CHECK_EQ(h.tick_until_candidate(), expected);
    CHECK_EQ(h.node->current_term(), Term{1});
    CHECK_EQ(h.node->voted_for(), std::optional<NodeId>{1});
    CHECK((h.storage.load() == PersistentState{1, NodeId{1}, {}}));  // persisted before sending

    const auto rvs = h.take<RequestVote>();
    CHECK_EQ(rvs.size(), std::size_t{4});
    for (NodeId i = 0; i < 4; ++i) {
        CHECK_EQ(rvs[i].first, NodeId{i + 2});
        CHECK((rvs[i].second == RequestVote{1, 1, 0, 0}));  // lastLogIndex/Term are 0
    }
}

TEST(raft_node_candidate_restarts_election_on_timeout) {
    Harness h;
    h.tick_until_candidate();
    h.take<RequestVote>();
    const Tick second = h.node->election_timeout();
    h.tick(second);
    CHECK(h.node->role() == Role::Candidate);
    CHECK_EQ(h.node->current_term(), Term{2});
    CHECK_EQ(h.take<RequestVote>().size(), std::size_t{4});
}

TEST(raft_node_grants_at_most_one_vote_per_term) {
    Harness h;
    h.deliver(2, RequestVote{1, 2, 0, 0});
    h.deliver(3, RequestVote{1, 3, 0, 0});  // different candidate, same term
    h.deliver(2, RequestVote{1, 2, 0, 0});  // retry from the same candidate
    const auto replies = h.take<RequestVoteReply>();
    CHECK_EQ(replies.size(), std::size_t{3});
    CHECK((replies[0] == std::pair<NodeId, RequestVoteReply>{2, {1, true}}));
    CHECK((replies[1] == std::pair<NodeId, RequestVoteReply>{3, {1, false}}));
    CHECK((replies[2] == std::pair<NodeId, RequestVoteReply>{2, {1, true}}));
    CHECK((h.storage.load() == PersistentState{1, NodeId{2}, {}}));
}

TEST(raft_node_new_term_allows_a_new_vote) {
    Harness h;
    h.deliver(2, RequestVote{1, 2, 0, 0});
    h.deliver(3, RequestVote{2, 3, 0, 0});
    const auto r = h.take<RequestVoteReply>();
    CHECK((r[1].second == RequestVoteReply{2, true}));
    CHECK_EQ(h.node->voted_for(), std::optional<NodeId>{3});
}

TEST(raft_node_rejects_stale_term_vote_request) {
    Harness h;
    h.deliver(2, AppendEntries{5, 2, 0, 0, 0, {}});
    h.take<AppendEntriesReply>();
    h.deliver(3, RequestVote{4, 3, 0, 0});
    const auto r = h.take<RequestVoteReply>();
    CHECK((r.at(0).second == RequestVoteReply{5, false}));  // reply carries our term
    CHECK(!h.node->voted_for().has_value());
}

TEST(raft_node_grants_vote_to_equally_up_to_date_log) {
    // Our log is empty, so any (lastLogTerm, lastLogIndex) >= (0, 0) is
    // up to date. Equal is enough (§5.4.1).
    Harness h;
    h.deliver(2, RequestVote{1, 2, 0, 0});
    CHECK(h.take<RequestVoteReply>().at(0).second.vote_granted);
}

TEST(raft_node_steps_down_on_higher_term_in_any_message) {
    for (int kind = 0; kind < 4; ++kind) {
        Harness h;
        h.tick_until_candidate();  // candidate in term 1, voted for self
        h.outbox.clear();
        const Term t = 7;
        RaftMessage m = kind == 0   ? RaftMessage{RequestVote{t, 3, 0, 0}}
                        : kind == 1 ? RaftMessage{RequestVoteReply{t, false}}
                        : kind == 2 ? RaftMessage{AppendEntries{t, 3, 0, 0, 0, {}}}
                                    : RaftMessage{AppendEntriesReply{t, false, 0, 0}};
        h.deliver(3, m);
        CHECK(h.node->role() == Role::Follower);
        CHECK_EQ(h.node->current_term(), t);
        CHECK_EQ(h.storage.load().current_term, t);
        // votedFor is cleared, except that a RequestVote in the new term
        // may legitimately take it.
        if (kind == 0) CHECK_EQ(h.node->voted_for(), std::optional<NodeId>{3});
        else CHECK(!h.node->voted_for().has_value());
    }
}

TEST(raft_node_becomes_leader_with_majority_and_heartbeats) {
    Harness h;
    h.tick_until_candidate();
    h.outbox.clear();
    h.deliver(2, RequestVoteReply{1, true});
    CHECK(h.node->role() == Role::Candidate);  // 2 of 5
    h.deliver(2, RequestVoteReply{1, true});   // duplicate must not count
    CHECK(h.node->role() == Role::Candidate);
    h.deliver(4, RequestVoteReply{1, false});
    CHECK(h.node->role() == Role::Candidate);
    h.deliver(3, RequestVoteReply{1, true});  // 3 of 5
    CHECK(h.node->role() == Role::Leader);
    CHECK_EQ(h.node->leader_id(), std::optional<NodeId>{1});

    // Heartbeats go out immediately, then every heartbeat_interval ticks.
    CHECK_EQ(h.take<AppendEntries>().size(), std::size_t{4});
    h.tick(h.config.heartbeat_interval - 1);
    CHECK(h.take<AppendEntries>().empty());
    h.tick();
    const auto hb = h.take<AppendEntries>();
    CHECK_EQ(hb.size(), std::size_t{4});
    CHECK((hb[0].second == AppendEntries{1, 1, 0, 0, 0, {}}));

    // A leader never times out into a new election.
    h.tick(10 * h.config.election_timeout_max);
    CHECK(h.node->role() == Role::Leader);
    CHECK_EQ(h.node->current_term(), Term{1});
}

TEST(raft_node_ignores_stale_vote_replies) {
    Harness h;
    h.tick_until_candidate();  // term 1
    h.tick(h.node->election_timeout());  // term 2
    h.deliver(2, RequestVoteReply{1, true});
    h.deliver(3, RequestVoteReply{1, true});
    CHECK(h.node->role() == Role::Candidate);
    CHECK_EQ(h.node->current_term(), Term{2});
}

TEST(raft_node_candidate_yields_to_current_leader) {
    Harness h;
    h.tick_until_candidate();
    h.outbox.clear();
    h.deliver(3, AppendEntries{1, 3, 0, 0, 0, {}});
    CHECK(h.node->role() == Role::Follower);
    CHECK_EQ(h.node->current_term(), Term{1});
    CHECK_EQ(h.node->leader_id(), std::optional<NodeId>{3});
    CHECK((h.take<AppendEntriesReply>().at(0).second == AppendEntriesReply{1, true, 0, 0}));
}

TEST(raft_node_heartbeats_suppress_elections) {
    Harness h;
    for (int i = 0; i < 100; ++i) {
        h.tick(h.config.election_timeout_min - 1);
        h.deliver(2, AppendEntries{1, 2, 0, 0, 0, {}});
    }
    CHECK(h.node->role() == Role::Follower);
    CHECK_EQ(h.node->current_term(), Term{1});
}

TEST(raft_node_rejects_stale_leader) {
    Harness h;
    h.deliver(2, AppendEntries{3, 2, 0, 0, 0, {}});
    h.outbox.clear();
    h.deliver(4, AppendEntries{2, 4, 0, 0, 0, {}});
    CHECK((h.take<AppendEntriesReply>().at(0).second == AppendEntriesReply{3, false, 0, 0}));
    CHECK_EQ(h.node->leader_id(), std::optional<NodeId>{2});
}

TEST(raft_node_ignores_garbage_and_strangers) {
    Harness h;
    h.node->receive(2, "not a message");
    h.node->receive(9, encode(RequestVote{50, 9, 0, 0}));  // not a peer
    CHECK_EQ(h.node->current_term(), Term{0});
    CHECK(h.outbox.empty());
}

TEST(raft_node_restart_keeps_term_and_vote) {
    Harness h;
    h.deliver(2, RequestVote{4, 2, 0, 0});
    h.boot();  // crash and restart: volatile state is gone
    CHECK(h.node->role() == Role::Follower);
    CHECK_EQ(h.node->current_term(), Term{4});
    CHECK_EQ(h.node->voted_for(), std::optional<NodeId>{2});

    h.outbox.clear();
    h.deliver(3, RequestVote{4, 3, 0, 0});  // must not vote twice in term 4
    CHECK((h.take<RequestVoteReply>().at(0).second == RequestVoteReply{4, false}));
}

TEST(raft_node_single_node_cluster_elects_itself) {
    Harness h({});
    h.tick(h.node->election_timeout());
    CHECK(h.node->role() == Role::Leader);
    CHECK_EQ(h.node->current_term(), Term{1});
    CHECK(h.outbox.empty());
}
