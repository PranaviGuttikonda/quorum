#include "quorum/raft/invariants.h"

#include <algorithm>
#include <sstream>

namespace quorum::raft {

namespace {

std::uint64_t mix(std::uint64_t z) {
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

// Approach: fold the term and the command bytes (FNV-1a style) into the
//           previous prefix hash, then finalize with the length.
std::uint64_t chain(std::uint64_t prev, const LogEntry& e) {
    std::uint64_t h = mix(prev ^ (e.term * 0x9e3779b97f4a7c15ULL));
    for (unsigned char ch : e.command) {
        h ^= ch;
        h *= 0x100000001b3ULL;
    }
    return mix(h ^ e.command.size());
}

constexpr std::uint64_t kEmptyPrefix = 0x6a09e667f3bcc909ULL;

}  // namespace

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

std::vector<NodeView> views(const RaftCluster& cluster) {
    std::vector<NodeView> out;
    for (NodeId id : cluster.ids()) {
        const RaftNode* n = cluster.node(id);
        if (!n) continue;
        out.push_back(NodeView{id, cluster.boot_count(id), n->role(), n->current_term(), &n->log(),
                               n->commit_index(), n->truncations(), &cluster.applied(id)});
    }
    return out;
}

void LogSafetyChecker::check(const RaftCluster& cluster) {
    check(cluster.now(), views(cluster));
}

// Approach: hash new entries first, so commits, applies and leaders are
//           compared against up-to-date chains.
void LogSafetyChecker::check(Tick now, const std::vector<NodeView>& nodes) {
    for (const NodeView& v : nodes) update_hashes(now, v, cache_[v.id]);
    for (const NodeView& v : nodes) update_commits(now, v, cache_.at(v.id));
    for (const NodeView& v : nodes) update_applied(now, v, cache_.at(v.id));
    for (const NodeView& v : nodes)
        if (v.role == Role::Leader) check_leader(now, v, cache_.at(v.id));
}

void LogSafetyChecker::fail(Tick now, const std::string& what) {
    if (violation_) return;
    violation_ = what + " (t=" + std::to_string(now) + ")";
}

void LogSafetyChecker::update_hashes(Tick now, const NodeView& v, NodeCache& c) {
    const auto& log = *v.log;
    if (c.boot != v.boot) c = NodeCache{v.boot, v.truncations, {}, 0};
    if (c.truncations != v.truncations || log.size() < c.prefix.size()) {
        c.truncations = v.truncations;
        c.prefix.clear();
    }
    for (std::size_t i = c.prefix.size(); i < log.size(); ++i) {
        const std::uint64_t h = chain(i == 0 ? kEmptyPrefix : c.prefix[i - 1], log[i]);
        c.prefix.push_back(h);
        const auto key = std::make_pair(LogIndex{i + 1}, log[i].term);
        const auto [it, inserted] = seen_.emplace(key, std::make_pair(h, v.id));
        if (!inserted && it->second.first != h) {
            std::ostringstream os;
            os << "log matching: nodes " << it->second.second << " and " << v.id
               << " both hold index " << i + 1 << " term " << log[i].term
               << " but their logs differ before it";
            fail(now, os.str());
        }
    }
}

void LogSafetyChecker::update_commits(Tick now, const NodeView& v, const NodeCache& c) {
    if (v.commit_index > c.prefix.size()) {
        fail(now, "state machine safety: node " + std::to_string(v.id) + " commit index " +
                      std::to_string(v.commit_index) + " is beyond its log");
        return;
    }
    // Agree with the known committed prefix where they overlap...
    const std::size_t overlap = std::min<std::size_t>(v.commit_index, committed_.size());
    if (overlap > 0 && c.prefix[overlap - 1] != committed_[overlap - 1].prefix) {
        fail(now, "state machine safety: node " + std::to_string(v.id) +
                      " committed a different entry at or before index " + std::to_string(overlap));
        return;
    }
    // ...then extend it, and lower the recorded commit term where this node is in an
    // older term than any earlier observer.
    for (std::size_t i = 0; i < overlap; ++i)
        committed_[i].term = std::min(committed_[i].term, v.term);
    for (std::size_t i = committed_.size(); i < v.commit_index; ++i)
        committed_.push_back(Committed{c.prefix[i], v.term});
}

void LogSafetyChecker::update_applied(Tick now, const NodeView& v, NodeCache& c) {
    const auto& ap = *v.applied;
    if (ap.size() > v.commit_index) {
        fail(now, "state machine safety: node " + std::to_string(v.id) + " applied index " +
                      std::to_string(ap.size()) + " beyond its commit index");
        return;
    }
    for (std::size_t j = c.applied_seen; j < ap.size(); ++j) {
        if (j == applied_.size()) {
            applied_.emplace_back(ap[j], v.id);
        } else if (!(applied_[j].first == ap[j])) {
            std::ostringstream os;
            os << "state machine safety: index " << j + 1 << " applied as \""
               << applied_[j].first.command << "\" (term " << applied_[j].first.term
               << ") by node " << applied_[j].second << " but as \"" << ap[j].command
               << "\" (term " << ap[j].term << ") by node " << v.id;
            fail(now, os.str());
        }
    }
    c.applied_seen = ap.size();
}

// Approach: find the highest committed index whose commit term is <= this
//           leader's term. Matching the prefix hash there proves that every
//           entry below it is present too.
void LogSafetyChecker::check_leader(Tick now, const NodeView& v, const NodeCache& c) {
    for (std::size_t i = committed_.size(); i-- > 0;) {
        if (committed_[i].term > v.term) continue;
        if (i >= c.prefix.size() || c.prefix[i] != committed_[i].prefix) {
            std::ostringstream os;
            os << "leader completeness: leader " << v.id << " of term " << v.term
               << " is missing committed entries up to index " << i + 1 << " (committed in term "
               << committed_[i].term << ")";
            fail(now, os.str());
        }
        return;
    }
}

std::vector<LogEntry> LogSafetyChecker::applied_history() const {
    std::vector<LogEntry> out;
    out.reserve(applied_.size());
    for (const auto& [e, id] : applied_) out.push_back(e);
    return out;
}

}  // namespace quorum::raft
