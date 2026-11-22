// Given: KvClient records each send of an operation as an Attempt, for
//        display only.
// Approach: script a case with a redirect and a crash, then check the record
//           against what the client actually did, as seen on the network:
//           one attempt per request sent, in order, with the same tick and
//           server, outcomes that add up to the client's own counters, and
//           redirect hints that the next attempt followed. Also check that
//           recording changes nothing: the same seed with and without
//           reading attempts gives the same history.

#include <optional>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/kv/client.h"
#include "quorum/kv/cluster.h"

using namespace quorum::kv;
using quorum::raft::NodeId;
using quorum::sim::EventKind;

namespace {

struct Sent {
    quorum::sim::Tick tick;
    NodeId to;
};

// Requests from client 0's node, in the order they were sent.
std::vector<Sent> requests(const KvCluster& k, std::size_t trace_from) {
    std::vector<Sent> out;
    const auto& trace = k.raft().network().trace();
    for (std::size_t i = trace_from; i < trace.size(); ++i)
        if (trace[i].kind == EventKind::Send && trace[i].from == KvCluster::kFirstClientNode)
            out.push_back({trace[i].tick, trace[i].to});
    return out;
}

void check_against_trace(const KvCluster& k, std::size_t trace_from) {
    const KvClient& c = k.client(0);
    const auto& a = c.attempts();
    const auto sent = requests(k, trace_from);
    CHECK_EQ(a.size(), sent.size());
    CHECK_EQ(a.size(), c.op_stats().sends);
    std::uint64_t timeouts = 0, redirects = 0, replies = 0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        CHECK_EQ(a[i].sent, sent[i].tick);
        CHECK_EQ(a[i].server, sent[i].to);
        timeouts += a[i].outcome == AttemptOutcome::Timeout;
        redirects += a[i].outcome == AttemptOutcome::Redirect;
        replies += a[i].outcome == AttemptOutcome::Reply;
        // Clients tick before the clock advances, so a send made while a
        // reply was being delivered times out one tick sooner.
        if (a[i].outcome == AttemptOutcome::Timeout)
            CHECK(a[i].ended - a[i].sent == c.config().retry_timeout ||
                  a[i].ended - a[i].sent + 1 == c.config().retry_timeout);
        if (a[i].outcome != AttemptOutcome::InFlight) CHECK(a[i].ended >= a[i].sent);
        // A redirect naming another server is followed at once.
        if (a[i].outcome == AttemptOutcome::Redirect && a[i].hint && *a[i].hint != a[i].server && i + 1 < a.size() &&
            !a[i].late) {
            CHECK_EQ(a[i + 1].server, *a[i].hint);
            CHECK_EQ(a[i + 1].sent, a[i].ended);
        }
    }
    CHECK_EQ(timeouts, c.op_stats().timeouts);
    CHECK_EQ(redirects, c.op_stats().redirects);
    if (c.idle()) {
        CHECK_EQ(replies, std::uint64_t{1});
        CHECK(a.back().outcome == AttemptOutcome::Reply || a.back().outcome == AttemptOutcome::Superseded);
        for (const auto& x : a) {
            CHECK(x.outcome != AttemptOutcome::InFlight);
            if (x.outcome == AttemptOutcome::Reply) CHECK(std::optional<NodeId>(x.server) == c.answered_by());
        }
    } else {
        CHECK(a.back().outcome == AttemptOutcome::InFlight);
    }
}

}  // namespace

TEST(client_attempts_follow_a_redirect_and_a_crash) {
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        KvClusterConfig cfg;
        cfg.servers = 5;
        cfg.clients = 1;
        KvCluster k(seed, cfg);
        CHECK(k.raft().run_until([](const auto& c) { return c.stable_leader().has_value(); }, 3000));
        const NodeId leader = *k.raft().stable_leader();
        const NodeId follower = leader == 1 ? 2 : 1;

        // Ask a follower first: it redirects to the leader. Crash the leader
        // the moment the client sends to it, so that request is lost and the
        // client has to time out and look elsewhere.
        const std::size_t trace_from = k.raft().network().trace().size();
        k.client(0).set_leader_hint(follower);
        k.client(0).submit(Op::Put, "k", "v");
        check_against_trace(k, trace_from);
        CHECK(k.run_until([](const KvCluster& x) { return x.client(0).attempts().size() >= 2; }, 1000));
        k.raft().crash(leader);
        check_against_trace(k, trace_from);
        CHECK(k.run_until([](const KvCluster& x) { return x.client(0).idle(); }, 5000));
        check_against_trace(k, trace_from);

        const auto& a = k.client(0).attempts();
        CHECK(a[0].server == follower && a[0].outcome == AttemptOutcome::Redirect);
        CHECK(a[0].hint == std::optional<NodeId>(leader));
        CHECK(a[1].server == leader && a[1].outcome == AttemptOutcome::Timeout);
        CHECK(a.size() >= 3);
        CHECK(k.history().ops().back().result && k.history().ops().back().result->ok);
    }
}

TEST(client_attempts_change_nothing) {
    // The same run with attempts read along the way and without: identical
    // histories and client counters.
    auto run = [](bool read) {
        KvClusterConfig cfg;
        cfg.servers = 5;
        cfg.clients = 2;
        KvCluster k(42, cfg);
        std::uint64_t seen = 0;
        for (int i = 0; i < 40; ++i) {
            for (std::size_t c = 0; c < 2; ++c)
                if (k.client(c).idle()) k.client(c).submit(i % 2 ? Op::Get : Op::Put, "k", std::to_string(i));
            if (i == 10) k.raft().crash(k.raft().stable_leader().value_or(1));
            if (i == 25) k.raft().restart(k.raft().ids()[0]);
            for (int t = 0; t < 40; ++t) {
                k.tick();
                if (read)
                    for (std::size_t c = 0; c < 2; ++c) seen += k.client(c).attempts().size();
            }
        }
        std::string out;
        for (const auto& op : k.history().ops())
            out += std::to_string(op.invoked) + ":" + std::to_string(op.completed.value_or(0)) + ";";
        for (std::size_t c = 0; c < 2; ++c) out += std::to_string(k.client(c).stats().sends) + ",";
        return std::make_pair(out, seen);
    };
    const auto a = run(false), b = run(true);
    CHECK_EQ(a.first, b.first);
    CHECK(b.second > 0);
}
