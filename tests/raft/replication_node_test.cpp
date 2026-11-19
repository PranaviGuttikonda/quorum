// Given: the replication and commit rules (§5.3–5.4) pinned down on a single
//        node.
// Approach: NodeHarness drives the node by hand and decodes what it sends.
//           Each test covers one rule from Figure 2 or one corner case
//           introduced by duplication and reordering.

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "harness.h"
#include "node_harness.h"

using namespace quorum::raft;
using Harness = quorum::raft::testing::NodeHarness;

namespace {

std::vector<LogEntry> entries(Term term, std::initializer_list<const char*> cmds) {
    std::vector<LogEntry> out;
    for (const char* c : cmds) out.push_back({term, c});
    return out;
}

std::vector<std::string> applied_commands(const Harness& h) {
    std::vector<std::string> out;
    for (const auto& [i, e] : h.applied) out.push_back(e.command);
    return out;
}

using Strings = std::vector<std::string>;

}  // namespace

TEST(replication_propose_on_non_leader_returns_hint) {
    Harness h;
    CHECK((h.node->propose("x") == ProposeResult{NotLeader{std::nullopt}}));
    h.deliver(3, AppendEntries{1, 3, 0, 0, 0, {}});
    CHECK((h.node->propose("x") == ProposeResult{NotLeader{NodeId{3}}}));
    h.tick(h.node->election_timeout());  // candidate: no known leader
    CHECK(h.node->role() == Role::Candidate);
    CHECK((h.node->propose("x") == ProposeResult{NotLeader{std::nullopt}}));
    CHECK(h.node->log().empty());
}

TEST(replication_leader_appends_persists_and_replicates) {
    Harness h;
    h.make_leader();  // term 1
    CHECK((h.node->propose("a") == ProposeResult{Proposed{1, 1}}));
    CHECK((h.storage.load().log == entries(1, {"a"})));
    const auto aes = h.take<AppendEntries>();
    CHECK_EQ(aes.size(), std::size_t{4});
    for (const auto& [to, ae] : aes) CHECK((ae == AppendEntries{1, 1, 0, 0, 0, entries(1, {"a"})}));
    CHECK_EQ(h.node->commit_index(), LogIndex{0});  // not yet replicated
}

TEST(replication_leader_commits_on_majority_and_applies_in_order) {
    Harness h;
    h.make_leader();
    h.node->propose("a");
    h.node->propose("b");
    h.outbox.clear();
    h.deliver(2, AppendEntriesReply{1, true, 2, 2});
    h.deliver(2, AppendEntriesReply{1, true, 2, 2});  // a duplicate must not count twice
    CHECK_EQ(h.node->commit_index(), LogIndex{0});
    h.deliver(3, AppendEntriesReply{1, true, 1, 1});  // 3 of 5 hold index 1
    CHECK_EQ(h.node->commit_index(), LogIndex{1});
    h.deliver(4, AppendEntriesReply{1, true, 2, 2});  // 3 of 5 hold index 2
    CHECK_EQ(h.node->commit_index(), LogIndex{2});
    CHECK((applied_commands(h) == Strings{"a", "b"}));
    CHECK_EQ(h.applied[0].first, LogIndex{1});
    CHECK_EQ(h.applied[1].first, LogIndex{2});

    // Followers learn the commit index on the next heartbeat.
    h.outbox.clear();
    h.tick(h.config.heartbeat_interval);
    for (const auto& [to, ae] : h.take<AppendEntries>()) CHECK_EQ(ae.leader_commit, LogIndex{2});
}

TEST(replication_figure8_leader_never_commits_older_term_by_counting) {
    // Given: a leader whose log holds entries from earlier terms, replicated to
    //        a majority.
    // Approach: §5.4.2: those entries must not commit until an entry from the
    //           leader's own term is replicated on a majority. Then they all
    //           commit together.
    Harness h;
    h.preload(2, std::nullopt, {{1, "x"}, {2, "y"}});
    h.make_leader();  // term 3
    CHECK_EQ(h.node->current_term(), Term{3});
    h.deliver(2, AppendEntriesReply{3, true, 2, 2});
    h.deliver(3, AppendEntriesReply{3, true, 2, 2});
    h.deliver(4, AppendEntriesReply{3, true, 2, 2});
    CHECK_EQ(h.node->commit_index(), LogIndex{0});  // all 5 of 5 hold index 2: still not committed
    CHECK(h.applied.empty());

    h.node->propose("z");  // index 3, term 3
    h.deliver(2, AppendEntriesReply{3, true, 3, 3});
    CHECK_EQ(h.node->commit_index(), LogIndex{0});
    h.deliver(3, AppendEntriesReply{3, true, 3, 3});
    CHECK_EQ(h.node->commit_index(), LogIndex{3});
    CHECK((applied_commands(h) == Strings{"x", "y", "z"}));
}

TEST(replication_follower_appends_and_reports_match) {
    Harness h;
    h.deliver(2, AppendEntries{1, 2, 0, 0, 0, entries(1, {"a", "b"})});
    CHECK((h.node->log() == entries(1, {"a", "b"})));
    CHECK((h.storage.load().log == entries(1, {"a", "b"})));
    CHECK((h.take<AppendEntriesReply>().at(0).second == AppendEntriesReply{1, true, 2, 2}));
}

TEST(replication_follower_persists_before_replying) {
    MemoryStorage storage;
    bool checked = false;
    RaftNode node(
        1, {2, 3}, {}, storage,
        [&](NodeId, std::string payload) {
            const auto m = decode(payload);
            if (const auto* r = std::get_if<AppendEntriesReply>(&*m); r && r->success) {
                CHECK_EQ(storage.load().log.size(), std::size_t{2});
                checked = true;
            }
        },
        quorum::sim::Rng(1));
    node.receive(2, encode(AppendEntries{1, 2, 0, 0, 0, entries(1, {"a", "b"})}));
    CHECK(checked);
}

TEST(replication_follower_rejects_missing_prev_with_hint) {
    Harness h;
    h.deliver(2, AppendEntries{1, 2, 0, 0, 0, entries(1, {"a"})});
    h.outbox.clear();
    h.deliver(2, AppendEntries{1, 2, 5, 1, 0, entries(1, {"f"})});
    CHECK((h.take<AppendEntriesReply>().at(0).second == AppendEntriesReply{1, false, 0, 1}));
    CHECK_EQ(h.node->last_log_index(), LogIndex{1});
}

TEST(replication_follower_rejects_term_mismatch_at_prev) {
    Harness h;
    h.preload(1, std::nullopt, entries(1, {"a", "b"}));
    h.deliver(2, AppendEntries{2, 2, 2, 2, 0, entries(2, {"c"})});
    CHECK((h.take<AppendEntriesReply>().at(0).second == AppendEntriesReply{2, false, 0, 2}));
    CHECK((h.node->log() == entries(1, {"a", "b"})));
}

TEST(replication_follower_truncates_only_at_conflict) {
    Harness h;
    h.preload(1, std::nullopt, entries(1, {"a", "b", "c"}));
    h.deliver(2, AppendEntries{2, 2, 1, 1, 0, entries(2, {"x"})});
    CHECK((h.node->log() == std::vector<LogEntry>{{1, "a"}, {2, "x"}}));
    CHECK_EQ(h.node->truncations(), std::uint64_t{1});
    CHECK_EQ(h.storage.truncated_entries(), std::uint64_t{2});
    CHECK((h.take<AppendEntriesReply>().at(0).second == AppendEntriesReply{2, true, 2, 2}));
}

TEST(replication_follower_keeps_matching_suffix_beyond_request) {
    // Given: entries 2..3 already match, but the request only covers index 2.
    // Approach: §5.3: only a conflict truncates, so index 3 survives.
    Harness h;
    h.deliver(2, AppendEntries{1, 2, 0, 0, 0, entries(1, {"a", "b", "c"})});
    h.outbox.clear();
    h.deliver(2, AppendEntries{1, 2, 1, 1, 0, entries(1, {"b"})});
    CHECK_EQ(h.node->last_log_index(), LogIndex{3});
    CHECK((h.take<AppendEntriesReply>().at(0).second == AppendEntriesReply{1, true, 2, 3}));
}

TEST(replication_duplicate_and_reordered_appends_are_idempotent) {
    Harness h;
    const AppendEntries first{1, 2, 0, 0, 0, entries(1, {"a", "b"})};
    const AppendEntries second{1, 2, 2, 1, 2, entries(1, {"c"})};
    h.deliver(2, first);
    h.deliver(2, second);
    const auto writes = h.storage.log_writes();
    const auto applied = h.applied.size();

    h.deliver(2, first);   // late duplicate of an older request
    h.deliver(2, second);  // duplicate of the latest
    h.deliver(2, first);
    CHECK((h.node->log() == entries(1, {"a", "b", "c"})));
    CHECK_EQ(h.storage.log_writes(), writes);  // nothing new was written
    CHECK_EQ(h.node->truncations(), std::uint64_t{0});
    CHECK_EQ(h.applied.size(), applied);       // nothing re-applied
    CHECK_EQ(h.node->commit_index(), LogIndex{2});
}

TEST(replication_follower_commit_is_bounded_by_proven_prefix) {
    // Given: a follower with an unverified suffix, and a leader whose commit
    //        index covers that range.
    // Approach: commitIndex = min(leaderCommit, prevLogIndex + entries), so
    //           the unverified entries aren't committed.
    Harness h;
    h.preload(1, std::nullopt, entries(1, {"a", "b", "c"}));
    h.deliver(2, AppendEntries{1, 2, 1, 1, 3, {}});
    CHECK_EQ(h.node->commit_index(), LogIndex{1});
    CHECK((applied_commands(h) == Strings{"a"}));
    h.deliver(2, AppendEntries{1, 2, 3, 1, 3, {}});
    CHECK_EQ(h.node->commit_index(), LogIndex{3});
    h.deliver(2, AppendEntries{1, 2, 1, 1, 1, {}});  // a stale, lower commit
    CHECK_EQ(h.node->commit_index(), LogIndex{3});    // never moves backwards
    CHECK((applied_commands(h) == Strings{"a", "b", "c"}));
}

TEST(replication_vote_uses_real_log_up_to_date_check) {
    Harness h;
    h.preload(2, std::nullopt, {{1, "a"}, {2, "b"}});
    auto vote = [&](Term term, NodeId cand, LogIndex idx, Term lterm) {
        h.deliver(cand, RequestVote{term, cand, idx, lterm});
        return h.take<RequestVoteReply>().at(0).second.vote_granted;
    };
    CHECK(!vote(3, 2, 9, 1));  // longer, but lower last term
    CHECK(!vote(4, 3, 1, 2));  // same last term, shorter
    CHECK(vote(5, 4, 2, 2));   // same last term and length
    CHECK(vote(6, 5, 1, 3));   // shorter, but higher last term
}

TEST(replication_leader_backtracks_with_hint_then_decrement) {
    Harness h;
    std::vector<LogEntry> log;
    for (int i = 0; i < 10; ++i) log.push_back({1, "e" + std::to_string(i)});
    h.preload(1, std::nullopt, log);
    h.make_leader();  // term 2
    CHECK_EQ(h.node->next_index(2), LogIndex{11});

    // The follower only has 3 entries, so jump straight to prev = 3.
    h.deliver(2, AppendEntriesReply{2, false, 0, 3});
    auto ae = h.take<AppendEntries>().at(0).second;
    CHECK_EQ(ae.prev_log_index, LogIndex{3});
    CHECK_EQ(ae.entries.size(), std::size_t{7});

    // Its entry 3 conflicts too: back up by one.
    h.deliver(2, AppendEntriesReply{2, false, 0, 3});
    CHECK_EQ(h.take<AppendEntries>().at(0).second.prev_log_index, LogIndex{2});

    h.deliver(2, AppendEntriesReply{2, true, 10, 10});
    CHECK_EQ(h.node->match_index(2), LogIndex{10});
    CHECK_EQ(h.node->next_index(2), LogIndex{11});
    CHECK(h.take<AppendEntries>().empty());  // caught up: nothing more to send

    // A late failure reply never moves nextIndex to or below matchIndex.
    h.deliver(2, AppendEntriesReply{2, false, 0, 0});
    CHECK_EQ(h.node->next_index(2), LogIndex{11});
    CHECK_EQ(h.take<AppendEntries>().at(0).second.prev_log_index, LogIndex{10});
}

TEST(replication_leader_batches_appends) {
    Harness h;
    std::vector<LogEntry> log;
    for (int i = 0; i < 150; ++i) log.push_back({1, "e"});
    h.preload(1, std::nullopt, log);
    h.make_leader();
    h.deliver(2, AppendEntriesReply{2, false, 0, 0});
    const auto ae = h.take<AppendEntries>().at(0).second;
    CHECK_EQ(ae.prev_log_index, LogIndex{0});
    CHECK_EQ(ae.entries.size(), h.config.max_entries_per_append);
    // Each success pulls the next batch at once.
    h.deliver(2, AppendEntriesReply{2, true, 64, 64});
    const auto next = h.take<AppendEntries>().at(0).second;
    CHECK_EQ(next.prev_log_index, LogIndex{64});
    CHECK_EQ(next.entries.size(), std::size_t{64});
}

TEST(replication_leader_ignores_stale_replies) {
    Harness h;
    h.make_leader();
    h.node->propose("a");
    h.deliver(2, AppendEntriesReply{0, true, 1, 1});  // from an older term
    h.deliver(3, AppendEntriesReply{0, true, 1, 1});
    CHECK_EQ(h.node->match_index(2), LogIndex{0});
    CHECK_EQ(h.node->commit_index(), LogIndex{0});
}

TEST(replication_restart_keeps_log_and_reapplies_from_start) {
    Harness h;
    h.deliver(2, AppendEntries{1, 2, 0, 0, 2, entries(1, {"a", "b"})});
    CHECK((applied_commands(h) == Strings{"a", "b"}));
    h.boot();  // crash and restart
    CHECK((h.node->log() == entries(1, {"a", "b"})));
    CHECK_EQ(h.node->commit_index(), LogIndex{0});  // volatile
    CHECK_EQ(h.node->last_applied(), LogIndex{0});
    CHECK(h.applied.empty());
    h.deliver(2, AppendEntries{1, 2, 2, 1, 2, {}});
    CHECK((applied_commands(h) == Strings{"a", "b"}));
    CHECK_EQ(h.applied[0].first, LogIndex{1});
}

TEST(replication_single_node_commits_on_propose) {
    Harness h({});
    h.tick(h.node->election_timeout());
    CHECK(h.node->role() == Role::Leader);
    CHECK((h.node->propose("solo") == ProposeResult{Proposed{1, 1}}));
    CHECK_EQ(h.node->commit_index(), LogIndex{1});
    CHECK((applied_commands(h) == Strings{"solo"}));
}

TEST(replication_config_rejects_zero_batch) {
    RaftConfig c;
    c.max_entries_per_append = 0;
    CHECK_THROWS(validate(c));
}

namespace {

void enable_noop(Harness& h) {
    h.config.append_noop_on_election = true;
    h.boot();
}

}  // namespace

TEST(replication_new_leader_appends_noop_and_sends_it_at_once) {
    Harness h;
    enable_noop(h);
    h.tick_until_candidate();
    h.outbox.clear();
    h.deliver(2, RequestVoteReply{1, true});
    h.deliver(3, RequestVoteReply{1, true});
    CHECK(h.node->role() == Role::Leader);
    CHECK((h.node->log() == std::vector<LogEntry>{LogEntry::noop(1)}));
    CHECK((h.storage.load().log == std::vector<LogEntry>{LogEntry::noop(1)}));  // persisted first
    const auto aes = h.take<AppendEntries>();
    CHECK_EQ(aes.size(), std::size_t{4});
    for (const auto& [to, ae] : aes)
        CHECK((ae == AppendEntries{1, 1, 0, 0, 0, {LogEntry::noop(1)}}));
    // A client command goes after the no-op.
    CHECK((h.node->propose("a") == ProposeResult{Proposed{2, 1}}));
}

TEST(replication_noop_commits_older_entries_promptly) {
    // Given: a leader with uncommitted entries from older terms.
    // Approach: §8: once its own no-op is on a majority, the older entries
    //           commit with it, without waiting for a client write. The state
    //           machine is handed the no-op too, and is expected to skip it.
    Harness h;
    enable_noop(h);
    h.preload(2, std::nullopt, {{1, "x"}, {2, "y"}});
    h.make_leader();  // term 3, log [x, y, noop]
    CHECK_EQ(h.node->last_log_index(), LogIndex{3});
    CHECK(h.node->log()[2].is_noop());
    h.deliver(2, AppendEntriesReply{3, true, 3, 3});
    h.deliver(3, AppendEntriesReply{3, true, 3, 3});
    CHECK_EQ(h.node->commit_index(), LogIndex{3});
    CHECK_EQ(h.applied.size(), std::size_t{3});
    CHECK(h.applied[2].second.is_noop());
}

TEST(replication_single_node_commits_its_noop_on_election) {
    Harness h({});
    enable_noop(h);
    h.tick(h.node->election_timeout());
    CHECK(h.node->role() == Role::Leader);
    CHECK_EQ(h.node->commit_index(), LogIndex{1});
    CHECK(h.applied.at(0).second.is_noop());
}
