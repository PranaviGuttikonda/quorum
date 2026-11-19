#pragma once

// Given: the per-rule tests are easiest to write against one node in
//        isolation.
// Approach: drive a RaftNode by hand: tick it, feed it encoded messages, and
//           decode what it sends from a captured outbox. Applied entries are
//           recorded. boot() simulates a crash and restart over the same
//           storage.

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "harness.h"
#include "quorum/raft/node.h"

namespace quorum::raft::testing {

struct Sent {
    NodeId to;
    RaftMessage msg;
};

struct NodeHarness {
    MemoryStorage storage;
    std::vector<Sent> outbox;
    std::vector<std::pair<LogIndex, LogEntry>> applied;
    // The per-rule tests check raw mechanics, so the §8 no-op is off by
    // default here. Tests of the no-op turn it back on.
    RaftConfig config = [] {
        RaftConfig c;
        c.append_noop_on_election = false;
        return c;
    }();
    std::vector<NodeId> peers;
    std::uint64_t seed;
    std::optional<RaftNode> node;

    explicit NodeHarness(std::vector<NodeId> peer_ids = {2, 3, 4, 5}, std::uint64_t s = 1)
        : peers(std::move(peer_ids)), seed(s) {
        boot();
    }
    // The node's callbacks capture `this`, so the harness must stay put.
    NodeHarness(const NodeHarness&) = delete;
    NodeHarness& operator=(const NodeHarness&) = delete;

    // A brand-new node object over the same storage. Volatile state,
    // including what was applied, is gone.
    void boot() {
        applied.clear();
        node.emplace(
            NodeId{1}, peers, config, storage,
            [this](NodeId to, std::string payload) {
                auto m = decode(payload);
                if (!m) quorum::test::fail(__FILE__, __LINE__, "node sent garbage: " + payload);
                outbox.push_back({to, *m});
            },
            sim::Rng(seed), [this](LogIndex i, const LogEntry& e) { applied.emplace_back(i, e); });
    }

    // Seeds storage as if from an earlier incarnation, then reboots.
    void preload(Term term, std::optional<NodeId> vote, std::vector<LogEntry> log) {
        storage.save_term_and_vote(term, vote);
        storage.write_log(0, log);
        boot();
    }

    void deliver(NodeId from, const RaftMessage& m) { node->receive(from, encode(m)); }

    void tick(Tick n = 1) {
        for (Tick i = 0; i < n; ++i) node->tick();
    }

    // Ticks until the node becomes a candidate. Returns the number of ticks.
    Tick tick_until_candidate() {
        for (Tick t = 1; t <= config.election_timeout_max; ++t) {
            node->tick();
            if (node->role() == Role::Candidate) return t;
        }
        quorum::test::fail(__FILE__, __LINE__, "no election within election_timeout_max");
    }

    // Wins an election with votes from a bare majority of the peers. The
    // outbox is cleared.
    void make_leader() {
        tick_until_candidate();
        const std::size_t needed = (peers.size() + 1) / 2;  // a majority, minus our own vote
        for (std::size_t i = 0; i < needed; ++i)
            deliver(peers[i], RequestVoteReply{node->current_term(), true});
        if (node->role() != Role::Leader) quorum::test::fail(__FILE__, __LINE__, "did not win");
        outbox.clear();
    }

    template <class T>
    std::vector<std::pair<NodeId, T>> take() {
        std::vector<std::pair<NodeId, T>> out;
        for (const Sent& s : outbox)
            if (const T* m = std::get_if<T>(&s.msg)) out.emplace_back(s.to, *m);
        outbox.clear();
        return out;
    }
};

}  // namespace quorum::raft::testing
