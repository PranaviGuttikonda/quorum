#pragma once

// Given: the Raft and KV fuzz tests need the same seed-driven fault schedules.
// Approach: a FaultProfile (per-tick probabilities plus network config) and a
//           FaultInjector that applies one tick's worth of faults to a
//           RaftCluster before the cluster ticks. Its own Rng is derived from
//           the seed, so the schedule doesn't depend on the workload's draws.
//   - general(): 5 nodes, random crashes (at most 2 down), restarts, two-way
//     partitions and heals.
//   - nemesis(): 3 nodes, aimed at the Figure 8 window. It crashes new
//     leaders 1..40 ticks after they win, and cuts the newest leader off
//     alone, replacing any current partition.

#include <cstdint>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "quorum/raft/cluster.h"
#include "quorum/sim/rng.h"

namespace quorum::testing {

using raft::NodeId;
using raft::RaftCluster;
using raft::Term;
using sim::Tick;

struct FaultProfile {
    double crash = 0.004;           // crash a random live node
    double crash_new_leader = 0.0;  // per new leader: crash it 1..40 ticks after it wins
    double restart = 0.01;          // per crashed node
    double partition = 0.002;       // random two-way split
    double isolate_leader = 0.0;    // cut the newest leader off alone
    double heal = 0.004;
    std::size_t nodes = 5;
    std::size_t max_down = 2;
    sim::NetworkConfig net{
        .min_delay = 1, .max_delay = 10, .drop_probability = 0.1, .duplicate_probability = 0.1};

    static FaultProfile general() { return {}; }
    static FaultProfile nemesis() {
        FaultProfile p;
        p.nodes = 3;
        p.max_down = 1;
        p.crash_new_leader = 0.5;
        p.isolate_leader = 0.02;
        p.heal = 0.03;
        p.restart = 0.02;
        return p;
    }
};

// Fisher-Yates on the portable Rng (std::shuffle is implementation-defined).
inline void shuffle(std::vector<NodeId>& v, sim::Rng& rng) {
    for (std::size_t i = v.size(); i > 1; --i) {
        const auto j = static_cast<std::size_t>(rng.uniform(0, i - 1));
        std::swap(v[i - 1], v[j]);
    }
}

// The leader with the highest term. When a partition leaves a stale leader
// behind, this is the one the majority follows.
inline std::optional<NodeId> newest_leader(const RaftCluster& c) {
    std::optional<NodeId> best;
    for (NodeId id : c.leaders())
        if (!best || c.node(id)->current_term() > c.node(*best)->current_term()) best = id;
    return best;
}

class FaultInjector {
public:
    FaultInjector(const FaultProfile& p, std::uint64_t seed)
        : p_(p), rng_(seed ^ 0xfa017fa017fa017fULL) {}

    // Applies the faults for tick `t`. Call it before the cluster ticks.
    void step(RaftCluster& c, Tick t) {
        // A nemesis that targets the window right after an election: the new
        // leader crashes while it is still bringing followers up to date.
        for (NodeId l : c.leaders()) {
            const Term term = c.node(l)->current_term();
            if (seen_leaders_.insert({l, term}).second && rng_.bernoulli(p_.crash_new_leader))
                doomed_.push_back({t + rng_.uniform(1, 40), {l, term}});
        }
        for (auto it = doomed_.begin(); it != doomed_.end();) {
            if (it->first > t) {
                ++it;
                continue;
            }
            const auto [l, term] = it->second;
            if (c.is_up(l) && c.node(l)->role() == raft::Role::Leader && c.node(l)->current_term() == term &&
                down(c) < p_.max_down) {
                c.crash(l);
                ++crashes;
            }
            it = doomed_.erase(it);
        }

        std::vector<NodeId> up_ids, down_ids;
        for (NodeId id : c.ids()) (c.is_up(id) ? up_ids : down_ids).push_back(id);
        if (down_ids.size() < p_.max_down && rng_.bernoulli(p_.crash)) {
            c.crash(up_ids[rng_.uniform(0, up_ids.size() - 1)]);
            ++crashes;
        }
        for (NodeId id : down_ids)
            if (rng_.bernoulli(p_.restart)) c.restart(id);

        // Cutting off the newest leader may replace an existing partition, for
        // example isolating a new majority leader while the previous one is
        // still cut off. Figure 8 needs exactly that. Whatever the isolated
        // leader accepts now stays on that one node.
        if (rng_.bernoulli(p_.isolate_leader)) {
            if (const auto l = newest_leader(c)) {
                std::vector<NodeId> others;
                for (NodeId id : c.ids())
                    if (id != *l) others.push_back(id);
                c.partition({{*l}, others});
                partitioned_ = true;
                ++partitions;
            }
        } else if (!partitioned_ && rng_.bernoulli(p_.partition)) {
            std::vector<NodeId> ids = c.ids();
            shuffle(ids, rng_);
            const auto cut = static_cast<std::ptrdiff_t>(rng_.uniform(1, p_.nodes - 1));
            c.partition({{ids.begin(), ids.begin() + cut}, {ids.begin() + cut, ids.end()}});
            partitioned_ = true;
            ++partitions;
        } else if (partitioned_ && rng_.bernoulli(p_.heal)) {
            c.heal();
            partitioned_ = false;
        }
    }

    // Heals the network and restarts every crashed node.
    void clear(RaftCluster& c) {
        c.heal();
        partitioned_ = false;
        for (NodeId id : c.ids()) c.restart(id);
    }

    std::uint64_t crashes = 0;
    std::uint64_t partitions = 0;

private:
    static std::size_t down(const RaftCluster& c) {
        std::size_t n = 0;
        for (NodeId id : c.ids()) n += c.is_up(id) ? 0 : 1;
        return n;
    }

    FaultProfile p_;
    sim::Rng rng_;
    bool partitioned_ = false;
    std::set<std::pair<NodeId, Term>> seen_leaders_;
    std::vector<std::pair<Tick, std::pair<NodeId, Term>>> doomed_;
};

}  // namespace quorum::testing
