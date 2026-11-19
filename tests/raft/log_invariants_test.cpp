// Given: the fuzz tests trust LogSafetyChecker as their oracle.
// Approach: feed it hand-built NodeView histories. Each invariant must fire on
//           a crafted violation and stay quiet on legitimate behaviour:
//           truncating uncommitted entries, reboots that re-apply, and stale
//           leaders.

#include <string>
#include <vector>

#include "harness.h"
#include "quorum/raft/invariants.h"

using namespace quorum::raft;

namespace {

// Mutable stand-in for one node; view() snapshots it.
struct FakeNode {
    explicit FakeNode(NodeId i) : id(i) {}

    NodeId id;
    std::uint64_t boot = 1;
    Role role = Role::Follower;
    Term term = 1;
    std::vector<LogEntry> log;
    LogIndex commit = 0;
    std::uint64_t truncations = 0;
    std::vector<LogEntry> applied;

    NodeView view() const {
        return NodeView{id, boot, role, term, &log, commit, truncations, &applied};
    }
};

void check(LogSafetyChecker& chk, Tick t, std::initializer_list<const FakeNode*> nodes) {
    std::vector<NodeView> vs;
    for (const FakeNode* n : nodes) vs.push_back(n->view());
    chk.check(t, vs);
}

bool mentions(const LogSafetyChecker& chk, const std::string& what) {
    return chk.violation() && chk.violation()->find(what) != std::string::npos;
}

}  // namespace

TEST(log_checker_accepts_consistent_history) {
    LogSafetyChecker chk;
    FakeNode a{1}, b{2};
    a.role = Role::Leader;
    a.log = {{1, "x"}, {1, "y"}};
    b.log = {{1, "x"}};
    check(chk, 1, {&a, &b});
    a.commit = 2;
    a.applied = a.log;
    b.log = a.log;
    b.commit = 1;
    b.applied = {{1, "x"}};
    check(chk, 2, {&a, &b});
    CHECK(chk.ok());
    CHECK_EQ(chk.committed(), LogIndex{2});
}

TEST(log_checker_detects_log_matching_violation) {
    LogSafetyChecker chk;
    FakeNode a{1}, b{2};
    a.log = {{1, "x"}, {2, "y"}};
    // Index 1 has different terms, so it doesn't collide on its own. Index 2
    // collides on (2, term 2), and only the differing prefix shows the problem.
    b.log = {{2, "w"}, {2, "y"}};
    check(chk, 7, {&a, &b});
    CHECK(mentions(chk, "log matching"));
    CHECK(mentions(chk, "index 2 term 2"));
}

TEST(log_checker_log_matching_spans_history) {
    // Given: node 1 held (1, term 1) = "x" and was then truncated.
    // Approach: a later log holding (1, term 1) with a different command is
    //           still a violation, even though no live log shows the conflict.
    LogSafetyChecker chk;
    FakeNode a{1};
    a.log = {{1, "x"}};
    check(chk, 1, {&a});
    a.log = {{2, "z"}};
    a.truncations = 1;
    check(chk, 2, {&a});
    CHECK(chk.ok());  // truncating uncommitted entries is legitimate
    FakeNode b{2};
    b.log = {{1, "not-x"}};
    check(chk, 3, {&a, &b});
    CHECK(mentions(chk, "log matching"));
}

TEST(log_checker_detects_conflicting_commits) {
    LogSafetyChecker chk;
    FakeNode a{1}, b{2};
    a.log = {{1, "x"}};
    a.commit = 1;
    b.log = {{2, "y"}};  // different term, so Log Matching alone can't see it
    b.commit = 1;
    check(chk, 5, {&a, &b});
    CHECK(mentions(chk, "state machine safety"));
}

TEST(log_checker_detects_commit_beyond_log) {
    LogSafetyChecker chk;
    FakeNode a{1};
    a.log = {{1, "x"}};
    a.commit = 2;
    check(chk, 1, {&a});
    CHECK(mentions(chk, "beyond its log"));
}

TEST(log_checker_detects_different_applied_commands) {
    LogSafetyChecker chk;
    FakeNode a{1}, b{2};
    a.log = {{1, "x"}};
    a.commit = 1;
    a.applied = {{1, "x"}};
    check(chk, 1, {&a});
    // Node 2 claims to have applied something else at index 1, without
    // committing it (so only the applied check can catch it).
    b.log = {{1, "x"}};
    b.commit = 1;
    b.applied = {{1, "evil"}};
    check(chk, 2, {&a, &b});
    CHECK(mentions(chk, "state machine safety: index 1 applied as \"x\""));
}

TEST(log_checker_allows_reapply_after_reboot) {
    LogSafetyChecker chk;
    FakeNode a{1};
    a.log = {{1, "x"}, {1, "y"}};
    a.commit = 2;
    a.applied = a.log;
    check(chk, 1, {&a});
    a.boot = 2;  // restart: volatile state is gone
    a.commit = 0;
    a.applied.clear();
    check(chk, 2, {&a});
    a.commit = 2;
    a.applied = a.log;
    check(chk, 3, {&a});
    CHECK(chk.ok());
}

TEST(log_checker_detects_applied_beyond_commit) {
    LogSafetyChecker chk;
    FakeNode a{1};
    a.log = {{1, "x"}};
    a.applied = {{1, "x"}};
    check(chk, 1, {&a});
    CHECK(mentions(chk, "beyond its commit index"));
}

TEST(log_checker_detects_leader_missing_committed_entry) {
    LogSafetyChecker chk;
    FakeNode a{1}, b{2};
    a.term = 2;
    a.log = {{1, "x"}, {2, "y"}};
    a.commit = 2;
    check(chk, 1, {&a});
    b.role = Role::Leader;
    b.term = 3;
    b.log = {{1, "x"}};  // lacks committed index 2
    check(chk, 2, {&b});
    CHECK(mentions(chk, "leader completeness: leader 2 of term 3"));
}

TEST(log_checker_exempts_stale_leaders) {
    // Given: an isolated leader from an older term, behind the commits made
    //        after it lost contact.
    // Approach: only leaders of the commit term or later are bound by Leader
    //           Completeness.
    LogSafetyChecker chk;
    FakeNode a{1}, stale{2};
    a.term = 5;
    a.log = {{1, "x"}, {5, "y"}};
    a.commit = 2;
    stale.role = Role::Leader;
    stale.term = 4;
    stale.log = {{1, "x"}};
    check(chk, 1, {&a, &stale});
    CHECK(chk.ok());
}

TEST(log_checker_reports_only_first_violation) {
    LogSafetyChecker chk;
    FakeNode a{1}, b{2};
    a.log = {{1, "x"}};
    b.log = {{1, "y"}};
    check(chk, 10, {&a, &b});
    const std::string first = *chk.violation();
    FakeNode c{3};
    c.log = {{1, "z"}};
    check(chk, 11, {&a, &b, &c});
    CHECK_EQ(*chk.violation(), first);
    CHECK(mentions(chk, "(t=10)"));
}
