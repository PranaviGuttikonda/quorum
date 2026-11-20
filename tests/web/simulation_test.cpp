// Given: the visualizer draws only what Simulation::snapshot() reports, so
//        the snapshot must say exactly what the engine's state is.
// Approach: run scripted sessions (crashes, splits, client operations) for
//           several seeds, and every few ticks parse the snapshot and compare
//           every field with the engine: nodes, logs, messages on the wire,
//           partition groups, clients and operations. Plus input validation,
//           queueing, JSON escaping, and reproducibility of the scenario
//           trace that CI compares across native and WASM builds.

#include <string>

#include "harness.h"
#include "quorum/kv/command.h"
#include "quorum/web/simulation.h"
#include "web/json.h"

using namespace quorum;
using quorum::testing::JsonValue;
using quorum::testing::parse_json;
using web::Simulation;

namespace {

#define CHECK_AT(s, expr)                                                                       \
    do {                                                                                        \
        if (!(expr))                                                                            \
            ::quorum::test::fail(__FILE__, __LINE__,                                            \
                                 "seed " + std::to_string((s).seed()) + " t=" + std::to_string((s).now()) + \
                                     ": " #expr);                                               \
    } while (0)

std::optional<std::uint64_t> opt_num(const JsonValue& v) {
    if (v.is_null()) return std::nullopt;
    return v.num();
}

void check_log(const Simulation& s, const JsonValue& jlog, const std::vector<raft::LogEntry>& entries,
               raft::LogIndex commit) {
    const auto& log = jlog.arr();
    const std::size_t tail = std::min(entries.size(), Simulation::kLogTail);
    CHECK_AT(s, log.size() == tail);
    for (std::size_t i = 0; i < log.size(); ++i) {
        const raft::LogIndex index = entries.size() - tail + i + 1;
        const raft::LogEntry& e = entries[index - 1];
        const JsonValue& je = log[i];
        CHECK_AT(s, je["i"].num() == index);
        CHECK_AT(s, je["t"].num() == e.term);
        CHECK_AT(s, je["committed"].boolean() == (index <= commit));
        CHECK_AT(s, je.has("noop") == e.is_noop());
        if (!e.is_noop()) {
            const auto cmd = kv::decode_command(e.command);
            CHECK_AT(s, cmd.has_value());
            CHECK_AT(s, je["client"].num() == cmd->client && je["seq"].num() == cmd->seq);
            CHECK_AT(s, je["op"].str() == kv::to_string(cmd->op) && je["key"].str() == cmd->key);
            CHECK_AT(s, je.has("value") == (cmd->op == kv::Op::Put));
            if (cmd->op == kv::Op::Put) CHECK_AT(s, je["value"].str() == cmd->value);
        }
    }
}

void check_snapshot(const Simulation& s) {
    const JsonValue j = parse_json(s.snapshot());
    const raft::RaftCluster& c = s.cluster().raft();
    CHECK_AT(s, j["seed"].num() == s.seed());
    CHECK_AT(s, j["now"].num() == c.now());
    CHECK_AT(s, opt_num(j["leader"]) == (c.stable_leader() ? std::optional<std::uint64_t>(*c.stable_leader())
                                                           : std::nullopt));

    const auto& nodes = j["nodes"].arr();
    CHECK_AT(s, nodes.size() == c.ids().size());
    for (std::size_t k = 0; k < nodes.size(); ++k) {
        const JsonValue& jn = nodes[k];
        const raft::NodeId id = c.ids()[k];
        const raft::RaftNode* n = c.node(id);
        CHECK_AT(s, jn["id"].num() == id);
        CHECK_AT(s, jn["up"].boolean() == (n != nullptr));
        CHECK_AT(s, jn["group"].num() == c.network().group_of(id));
        CHECK_AT(s, jn["boots"].num() == c.boot_count(id));
        // A crashed server reports what it persisted: term, vote and log.
        const auto& mem = dynamic_cast<const raft::MemoryStorageProvider&>(c.storage());
        const raft::PersistentState disk = mem.storage(id).load();
        const raft::Term term = n ? n->current_term() : disk.current_term;
        const auto vote = n ? n->voted_for() : disk.voted_for;
        const std::vector<raft::LogEntry>& entries = n ? n->log() : disk.log;
        const raft::LogIndex commit = n ? n->commit_index() : 0;
        CHECK_AT(s, jn["term"].num() == term);
        CHECK_AT(s, opt_num(jn["votedFor"]) == (vote ? std::optional<std::uint64_t>(*vote) : std::nullopt));
        CHECK_AT(s, jn["lastIndex"].num() == entries.size());
        check_log(s, jn["log"], entries, commit);
        if (!n) {
            CHECK_AT(s, !jn.has("role") && !jn.has("commit"));
            continue;
        }
        CHECK_AT(s, jn["role"].str() == raft::to_string(n->role()));
        CHECK_AT(s, opt_num(jn["leaderId"]) == (n->leader_id() ? std::optional<std::uint64_t>(*n->leader_id())
                                                               : std::nullopt));
        CHECK_AT(s, jn["commit"].num() == n->commit_index());
        CHECK_AT(s, jn["applied"].num() == n->last_applied());
        CHECK_AT(s, jn["lastIndex"].num() == n->last_log_index());
        CHECK_AT(s, jn["timeout"].num() == n->election_timeout());
    }

    const auto wire = c.network().in_flight_messages();
    const auto& msgs = j["messages"].arr();
    CHECK_AT(s, msgs.size() == wire.size());
    for (std::size_t i = 0; i < msgs.size(); ++i) {
        const JsonValue& jm = msgs[i];
        const auto& f = wire[i];
        CHECK_AT(s, jm["id"].num() == f.msg.id && jm["from"].num() == f.msg.from && jm["to"].num() == f.msg.to);
        CHECK_AT(s, jm["sent"].num() == f.msg.sent_at && jm["at"].num() == f.msg.deliver_at);
        CHECK_AT(s, jm["copy"].boolean() == f.duplicate && jm["doomed"].boolean() == f.doomed);
        // The kind follows the payload's type.
        const std::string kind = jm["kind"].str();
        if (c.is_server(f.msg.from) && c.is_server(f.msg.to)) {
            const auto m = raft::decode(f.msg.payload);
            CHECK_AT(s, m.has_value());
            if (std::holds_alternative<raft::RequestVote>(*m) || std::holds_alternative<raft::RequestVoteReply>(*m))
                CHECK_AT(s, kind == "vote");
            else if (const auto* ae = std::get_if<raft::AppendEntries>(&*m))
                CHECK_AT(s, kind == (ae->entries.empty() ? "heartbeat" : "append"));
            else
                CHECK_AT(s, kind == "append" || kind == "heartbeat");
        } else {
            CHECK_AT(s, kind == (c.is_server(f.msg.to) ? "request" : "reply"));
        }
        CHECK_AT(s, !jm["text"].str().empty());
    }

    const auto& clients = j["clients"].arr();
    CHECK_AT(s, clients.size() == s.cluster().client_count());
    for (std::size_t i = 0; i < clients.size(); ++i) {
        const auto& cl = s.cluster().client(i);
        CHECK_AT(s, clients[i]["id"].num() == cl.client_id() && clients[i]["node"].num() == cl.node_id());
        CHECK_AT(s, clients[i]["idle"].boolean() == cl.idle());
    }

    const auto& ops = j["ops"].arr();
    const auto& all = s.ops();
    CHECK_AT(s, ops.size() == std::min(all.size(), Simulation::kOpTail));
    for (std::size_t i = 0; i < ops.size(); ++i) {
        const auto& w = all[all.size() - ops.size() + i];
        const JsonValue& jo = ops[i];
        CHECK_AT(s, jo["id"].num() == static_cast<std::uint64_t>(w.id) && jo["client"].num() == w.client);
        CHECK_AT(s, jo["op"].str() == kv::to_string(w.op) && jo["key"].str() == w.key);
        if (!w.history) {
            CHECK_AT(s, jo["status"].str() == "queued");
            continue;
        }
        const kv::OpRecord& r = s.cluster().history().ops().at(*w.history);
        CHECK_AT(s, r.command.key == w.key && r.command.op == w.op);
        CHECK_AT(s, jo["invoked"].num() == r.invoked);
        CHECK_AT(s, opt_num(jo["completed"]) == r.completed);
        CHECK_AT(s, jo["status"].str() == (r.completed ? "done" : "running"));
        CHECK_AT(s, jo.has("ok") == r.result.has_value());
        if (r.result) {
            CHECK_AT(s, jo["ok"].boolean() == r.result->ok);
            CHECK_AT(s, (jo["result"].is_null() ? std::optional<std::string>() : jo["result"].str()) ==
                            r.result->value);
            CHECK_AT(s, !jo["server"].is_null() && c.is_server(static_cast<raft::NodeId>(jo["server"].num())));
            CHECK_AT(s, jo["sends"].num() >= 1);
        }
    }
}

// Ticks in small steps, checking the snapshot each time.
void run_checked(Simulation& s, int ticks) {
    for (int i = 0; i < ticks; i += 7) {
        s.tick(7);
        check_snapshot(s);
    }
}

}  // namespace

TEST(web_snapshot_matches_the_engine) {
    for (std::uint64_t seed : {1ULL, 2ULL, 3ULL, 4ULL, 5ULL}) {
        Simulation s(seed, 5);
        check_snapshot(s);
        run_checked(s, 500);
        CHECK_AT(s, s.leader().has_value());
        const auto first = *s.leader();
        CHECK_AT(s, s.crash(first));
        CHECK_AT(s, s.submit(1, "put", "x", "1") > 0);
        CHECK_AT(s, s.submit(2, "put", "y", "\"quoted\"\n") > 0);
        CHECK_AT(s, s.submit(1, "get", "x", "") > 0);
        run_checked(s, 700);
        const auto second = s.leader().value_or(1);
        CHECK_AT(s, s.partition(std::to_string(second)));  // the leader alone
        CHECK_AT(s, s.submit(1, "delete", "x", "") > 0);
        run_checked(s, 600);
        s.heal();
        CHECK_AT(s, s.restart(first));
        run_checked(s, 900);
        // Every operation finished, and the reads saw the writes.
        for (const auto& w : s.ops()) CHECK_AT(s, w.history && s.cluster().history().ops()[*w.history].completed);
        const auto& get = s.cluster().history().ops()[*s.ops()[2].history];
        CHECK_AT(s, get.result && get.result->value == std::optional<std::string>("1"));
    }
}

TEST(web_snapshot_reports_the_partition_and_doomed_messages) {
    Simulation s(9, 5);
    s.tick(400);
    CHECK(s.partition("1,2|3,4,5"));
    const JsonValue j = parse_json(s.snapshot());
    for (const auto& n : j["nodes"].arr()) CHECK_EQ(n["group"].num(), n["id"].num() <= 2 ? 1u : 2u);
    // Anything already on the wire across the cut is shown as doomed.
    for (const auto& m : j["messages"].arr()) {
        const bool across = (m["from"].num() <= 2) != (m["to"].num() <= 2) && m["from"].num() <= 5 &&
                            m["to"].num() <= 5;
        if (across) CHECK(m["doomed"].boolean());
    }
    // Sends across the cut are dropped, and listed for a few ticks.
    bool partition_drop = false;
    for (int i = 0; i < 120; ++i) {
        s.tick();
        const JsonValue snap = parse_json(s.snapshot());
        for (const auto& d : snap["drops"].arr()) {
            CHECK(d["at"].num() + Simulation::kDropMemory >= s.now());
            partition_drop |= d["reason"].str() == "partition";
        }
    }
    CHECK(partition_drop);
    s.heal();
    const JsonValue healed = parse_json(s.snapshot());
    for (const auto& n : healed["nodes"].arr()) CHECK_EQ(n["group"].num(), 0u);
}

TEST(web_rejects_bad_calls) {
    Simulation s(1, 5);
    CHECK(!s.crash(0) && !s.crash(6) && !s.restart(1));
    CHECK(s.crash(1));
    CHECK(!s.crash(1));
    CHECK(s.restart(1));
    for (const char* bad : {"", "|", "1,", "1,1", "1|1", "6", "a", "1;2", "1,2|", "0", "1234"})
        CHECK(!s.partition(bad));
    CHECK(s.partition("1"));
    CHECK_EQ(s.submit(0, "put", "k", "v"), std::int64_t{-1});
    CHECK_EQ(s.submit(3, "put", "k", "v"), std::int64_t{-1});
    CHECK_EQ(s.submit(1, "cas", "k", "v"), std::int64_t{-1});
    CHECK_EQ(s.submit(1, "put", "", "v"), std::int64_t{-1});
    CHECK_EQ(s.submit(1, "put", "k", ""), std::int64_t{-1});
    CHECK_EQ(s.submit(1, "get", std::string(65, 'k'), ""), std::int64_t{-1});
    CHECK_THROWS(Simulation(1, 0));
    CHECK_THROWS(Simulation(1, 10));
}

TEST(web_operations_queue_per_client_in_order) {
    Simulation s(3, 3);
    s.tick(400);
    const auto a = s.submit(1, "put", "k", "1");
    const auto b = s.submit(1, "put", "k", "2");
    const auto c = s.submit(1, "get", "k", "");
    CHECK_EQ(a, std::int64_t{1});
    CHECK_EQ(c, std::int64_t{3});
    CHECK(!s.ops()[1].history);  // waits behind the first
    s.tick(1500);
    const auto& h = s.cluster().history().ops();
    CHECK(s.ops()[0].history < s.ops()[1].history && s.ops()[1].history < s.ops()[2].history);
    CHECK(h[*s.ops()[2].history].result->value == std::optional<std::string>("2"));
    (void)b;
}

TEST(web_snapshot_is_bounded_however_long_it_runs) {
    Simulation s(4, 5);
    for (int i = 0; i < 60; ++i) s.submit(1 + i % 2, "put", "k" + std::to_string(i % 3), "v");
    s.tick(20000);
    const JsonValue j = parse_json(s.snapshot());
    CHECK_EQ(j["ops"].arr().size(), Simulation::kOpTail);
    for (const auto& n : j["nodes"].arr())
        if (n["up"].boolean()) CHECK(n["log"].arr().size() <= Simulation::kLogTail);
    CHECK(s.cluster().raft().network().trace().empty());
    CHECK(s.drops().size() < 200);
}

TEST(web_scenario_trace_is_reproducible) {
    const std::string a = web::scenario_trace(1);
    CHECK_EQ(a, web::scenario_trace(1));
    CHECK(a != web::scenario_trace(2));
    CHECK(a.find("crash") != std::string::npos && a.find("split") != std::string::npos);
}
