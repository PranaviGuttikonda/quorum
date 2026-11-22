// Given: the client's retry rules (#22): random picks avoid a server that
//        timed out within the last 2 x retry_timeout unless that excludes
//        every server, and a hint-less redirect is retried once on the same
//        server before moving on.
// Approach: drive a KvClient directly with a fake clock and network, so each
//           rule can be checked exactly, over many seeds where randomness
//           matters.

#include <optional>
#include <set>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/kv/client.h"
#include "quorum/kv/command.h"

using namespace quorum::kv;
using quorum::raft::NodeId;
using quorum::sim::Tick;

namespace {

struct Rig {
    Tick now = 0;
    History history;
    std::vector<NodeId> sent;  // destination of every request, in order
    KvClient client;

    Rig(std::uint64_t seed, std::vector<NodeId> servers, Tick timeout = 10)
        : client(1000, 1, std::move(servers), ClientConfig{.retry_timeout = timeout, .redirect_backoff = 2},
                 quorum::sim::Rng(seed), history, [this](NodeId to, std::string) { sent.push_back(to); },
                 [this] { return now; }) {}

    void run(Tick ticks) {
        for (Tick i = 0; i < ticks; ++i) {
            ++now;
            client.tick();
        }
    }
    // A reply from `from` to the outstanding operation.
    void redirect(NodeId from, std::optional<NodeId> hint) {
        deliver(from, Reply{1, client.attempts().empty() ? 0 : seq(), Redirect{hint}});
    }
    void ok(NodeId from) { deliver(from, Reply{1, seq(), Result{true, std::nullopt}}); }
    Seq seq() const { return history.ops().back().command.seq; }
    void deliver(NodeId from, const Reply& r) {
        quorum::sim::Message m;
        m.from = from;
        m.to = 1000;
        m.payload = encode(r);
        client.on_message(m);
    }
};

}  // namespace

TEST(client_retry_skips_a_server_that_just_timed_out) {
    // Two timeouts in a row: the third pick is neither of them, on every seed.
    for (std::uint64_t seed = 1; seed <= 300; ++seed) {
        Rig r(seed, {1, 2, 3, 4, 5});
        r.client.submit(Op::Put, "k", "v");
        r.run(10);  // first target times out
        r.run(10);  // so does the second
        CHECK_EQ(r.sent.size(), std::size_t{3});
        CHECK(r.sent[1] != r.sent[0]);
        CHECK(r.sent[2] != r.sent[0] && r.sent[2] != r.sent[1]);
        const auto avoided = r.client.avoided_servers();
        CHECK_EQ(avoided.size(), std::size_t{2});
    }
}

TEST(client_retry_exclusion_expires) {
    // Three servers, timeout 10, exclusion 20. Each target times out in turn.
    // At t=30 the first server's exclusion has just ended, while the other
    // two are still excluded, so the fourth send must go back to it.
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
        Rig r(seed, {1, 2, 3});
        r.client.submit(Op::Put, "k", "v");
        r.run(30);
        CHECK_EQ(r.sent.size(), std::size_t{4});
        CHECK(std::set<NodeId>(r.sent.begin(), r.sent.begin() + 3).size() == 3);
        CHECK_EQ(r.sent[3], r.sent[0]);
        // Much later, nothing is excluded any more.
        r.ok(r.sent[3]);
        r.run(100);
        CHECK(r.client.avoided_servers().empty());
    }
}

TEST(client_retry_with_everything_excluded_still_picks_one) {
    // Two servers, both timed out within the window: the client still sends.
    std::set<NodeId> picked;
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
        Rig r(seed, {1, 2});
        r.client.submit(Op::Put, "k", "v");
        r.run(20);  // both time out: 1 and 2 are excluded at t=20
        CHECK_EQ(r.client.avoided_servers().size(), std::size_t{2});
        CHECK_EQ(r.sent.size(), std::size_t{3});
        picked.insert(r.sent[2]);
        CHECK(!r.client.idle());
    }
    CHECK_EQ(picked.size(), std::size_t{2});  // any server will do, both happen
}

TEST(client_retry_hintless_redirect_retries_the_same_server_once) {
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
        Rig r(seed, {1, 2, 3, 4, 5});
        r.client.submit(Op::Put, "k", "v");
        const NodeId first = r.sent[0];
        r.redirect(first, std::nullopt);  // no leader known yet
        CHECK_EQ(r.sent.size(), std::size_t{1});
        r.run(2);  // the backoff
        CHECK_EQ(r.sent.size(), std::size_t{2});
        CHECK_EQ(r.sent[1], first);  // the same server again
        // It still knows no leader: now someone else, after the backoff.
        r.redirect(first, std::nullopt);
        r.run(2);
        CHECK_EQ(r.sent.size(), std::size_t{3});
        CHECK(r.sent[2] != first);
        // A hint is still followed at once.
        const NodeId leader = r.sent[2] == 5 ? 4 : 5;
        r.redirect(r.sent[2], leader);
        CHECK_EQ(r.sent.size(), std::size_t{4});
        CHECK_EQ(r.sent[3], leader);
        r.ok(leader);
        CHECK(r.client.idle());
    }
}

TEST(client_retry_a_timeout_after_a_repeat_falls_back_to_random) {
    // The repeated server crashes before answering: the timeout moves on and
    // excludes it.
    for (std::uint64_t seed = 1; seed <= 100; ++seed) {
        Rig r(seed, {1, 2, 3});
        r.client.submit(Op::Put, "k", "v");
        const NodeId first = r.sent[0];
        r.redirect(first, std::nullopt);
        r.run(2);
        CHECK_EQ(r.sent[1], first);
        r.run(10);  // no answer to the repeat
        CHECK_EQ(r.sent.size(), std::size_t{3});
        CHECK(r.sent[2] != first);
        CHECK_EQ(r.client.avoided_servers(), std::vector<NodeId>{first});
    }
}
