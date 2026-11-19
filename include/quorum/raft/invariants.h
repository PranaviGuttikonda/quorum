#pragma once

// Given: the safety properties of Raft Figure 3 must hold at every point in
//        every run, not just at the end.
// Approach: checkers that run after every tick and keep their own history,
//           so a violation is caught even if the evidence later disappears
//           (a crash, a truncation, a step-down).

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "quorum/raft/cluster.h"

namespace quorum::raft {

// Given: Election Safety: at most one leader can be elected in a given term.
// Approach: remember, for every term, the first node seen leading it. A
//           different node leading the same term, at any later tick, is a
//           violation.
class ElectionSafetyChecker {
public:
    // Records every live leader in the cluster.
    void check(const RaftCluster& cluster);
    // Records that `id` was seen leading `term` at tick `now`.
    void observe(Tick now, NodeId id, Term term);

    bool ok() const { return !violation_.has_value(); }
    // First violation, human-readable, or nullopt.
    const std::optional<std::string>& violation() const { return violation_; }
    // Every term that had a leader, with that leader.
    const std::map<Term, NodeId>& leaders_by_term() const { return leaders_; }
    std::uint64_t checks() const { return checks_; }

private:
    std::map<Term, NodeId> leaders_;
    std::optional<std::string> violation_;
    std::uint64_t checks_ = 0;
};

// A read-only snapshot of one live node, as seen by LogSafetyChecker.
struct NodeView {
    NodeId id = 0;
    std::uint64_t boot = 0;  // changes on every restart
    Role role = Role::Follower;
    Term term = 0;
    const std::vector<LogEntry>* log = nullptr;
    LogIndex commit_index = 0;
    std::uint64_t truncations = 0;                  // per boot
    const std::vector<LogEntry>* applied = nullptr;  // per boot; [i] has index i + 1
};

std::vector<NodeView> views(const RaftCluster& cluster);

// Given: three log properties from Figure 3, checked every tick across all
//        live nodes and all history:
//   - Log Matching: two logs holding an entry with the same index and term
//     are identical up to that index.
//   - State Machine Safety: no two nodes apply (or commit) different entries
//     at the same index.
//   - Leader Completeness: a committed entry is in the log of every leader of
//     a later term.
// Approach: a full pairwise comparison every tick is too slow for 1,000-seed
//           fuzzing. Instead, each node's log is summarized by a chain of
//           prefix hashes, h[i] = H(h[i-1], entry i), so equal h[i] means
//           (up to hash collisions) identical prefixes. Only entries new since
//           the last tick are hashed. A node's chain is rebuilt after a
//           truncation or reboot.
//   - Log Matching keeps, for every (index, term) ever seen, the prefix hash
//     at that point.
//   - Commits extend a global committed prefix. Every node's commit point
//     must agree with it.
//   - Each committed index remembers the lowest term in which it was seen
//     committed. A leader of that term or later must hold the same prefix.
class LogSafetyChecker {
public:
    void check(const RaftCluster& cluster);
    void check(Tick now, const std::vector<NodeView>& nodes);

    bool ok() const { return !violation_.has_value(); }
    // First violation, prefixed with the invariant's name.
    const std::optional<std::string>& violation() const { return violation_; }
    // Length of the committed prefix observed so far.
    LogIndex committed() const { return committed_.size(); }
    // Every entry ever applied, by index (index i + 1 at [i]).
    std::vector<LogEntry> applied_history() const;

private:
    struct NodeCache {
        std::uint64_t boot = 0;
        std::uint64_t truncations = 0;
        std::vector<std::uint64_t> prefix;  // prefix[i] = hash of entries 1..i+1
        std::size_t applied_seen = 0;
    };
    struct Committed {
        std::uint64_t prefix = 0;
        Term term = 0;
    };

    void fail(Tick now, const std::string& what);
    void update_hashes(Tick now, const NodeView& v, NodeCache& c);
    void update_commits(Tick now, const NodeView& v, const NodeCache& c);
    void update_applied(Tick now, const NodeView& v, NodeCache& c);
    void check_leader(Tick now, const NodeView& v, const NodeCache& c);

    std::map<NodeId, NodeCache> cache_;
    std::map<std::pair<LogIndex, Term>, std::pair<std::uint64_t, NodeId>> seen_;
    std::vector<Committed> committed_;
    std::vector<std::pair<LogEntry, NodeId>> applied_;  // first application of each index
    std::optional<std::string> violation_;
};

}  // namespace quorum::raft
