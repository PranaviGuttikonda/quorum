#include "quorum/raft/invariants.h"

#include <sstream>

namespace quorum::raft {

void ElectionSafetyChecker::check(const RaftCluster& cluster) {
    ++checks_;
    for (NodeId id : cluster.leaders()) observe(cluster.now(), id, cluster.node(id)->current_term());
}

void ElectionSafetyChecker::observe(Tick now, NodeId id, Term term) {
    const auto [it, inserted] = leaders_.emplace(term, id);
    if (!inserted && it->second != id && !violation_) {
        std::ostringstream os;
        os << "election safety violated at t=" << now << ": term " << term << " led by node "
           << it->second << " and node " << id;
        violation_ = os.str();
    }
}

}  // namespace quorum::raft
