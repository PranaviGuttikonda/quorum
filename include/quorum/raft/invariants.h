#pragma once

// Given: Election Safety (Raft Figure 3): at most one leader can be elected in
//        a given term.
// Approach: run after every tick and remember, for every term, the first node
//           seen leading it. A different node leading the same term, at any
//           later tick, is a violation. This checks the whole history, not
//           just leaders that coexist at the same tick.

#include <cstdint>
#include <map>
#include <optional>
#include <string>

#include "quorum/raft/cluster.h"

namespace quorum::raft {

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

}  // namespace quorum::raft
