// Given: the KV state machine and its codecs are the source of truth for every
//        replica.
// Approach: check operation semantics, the session rules (execute, duplicate,
//           stale), that no-ops and garbage are skipped, codec round trips and
//           rejection, and that replaying the same log always rebuilds the
//           same state.

#include "quorum/kv/state_machine.h"

#include <string>
#include <vector>

#include "harness.h"
#include "quorum/sim/rng.h"

using namespace quorum::kv;
using quorum::raft::LogEntry;

namespace {

// Feeds commands as consecutive committed entries.
struct Feeder {
    KvStateMachine sm;
    std::optional<Applied> operator()(const Command& c) {
        return sm.apply(sm.last_applied() + 1, LogEntry{1, encode(c)});
    }
    Result result(const Command& c) { return (*this)(c)->result; }
};

const std::optional<std::string> absent;
std::optional<std::string> some(std::string s) { return s; }

}  // namespace

TEST(kv_put_get_delete_semantics) {
    Feeder f;
    CHECK((f.result(Command::get(1, 1, "k")) == Result{true, absent}));
    CHECK((f.result(Command::put(1, 2, "k", "v1")) == Result{true, absent}));
    CHECK((f.result(Command::get(1, 3, "k")) == Result{true, some("v1")}));
    CHECK((f.result(Command::put(1, 4, "k", "v2")) == Result{true, absent}));
    CHECK((f.result(Command::get(1, 5, "k")) == Result{true, some("v2")}));
    CHECK((f.result(Command::del(1, 6, "k")) == Result{true, absent}));   // existed
    CHECK((f.result(Command::del(1, 7, "k")) == Result{false, absent}));  // already gone
    CHECK((f.result(Command::get(1, 8, "k")) == Result{true, absent}));
    CHECK(f.sm.data().empty());
}

TEST(kv_cas_semantics) {
    Feeder f;
    // Create only if absent.
    CHECK((f.result(Command::cas(1, 1, "k", absent, "a")) == Result{true, absent}));
    CHECK((f.result(Command::cas(1, 2, "k", absent, "b")) == Result{false, some("a")}));
    // Swap only on an exact match. The result shows the prior value.
    CHECK((f.result(Command::cas(1, 3, "k", some("x"), "b")) == Result{false, some("a")}));
    CHECK((f.result(Command::cas(1, 4, "k", some("a"), "b")) == Result{true, some("a")}));
    CHECK_EQ(f.sm.data().at("k"), std::string("b"));
}

TEST(kv_duplicate_returns_cached_result_without_executing) {
    Feeder f;
    const Command create = Command::cas(7, 1, "k", absent, "mine");
    CHECK((f.result(create) == Result{true, absent}));
    // Another client changes the key, then client 7's retry is committed again.
    f(Command::put(8, 1, "k", "theirs"));
    const auto again = f(create);
    CHECK(again->duplicate);
    CHECK((again->result == Result{true, absent}));  // cached, not re-evaluated
    CHECK_EQ(f.sm.data().at("k"), std::string("theirs"));
    CHECK_EQ(f.sm.stats().executed, std::uint64_t{2});
    CHECK_EQ(f.sm.stats().duplicates, std::uint64_t{1});
    CHECK((f.sm.cached(7, 1) == std::optional<Result>{Result{true, absent}}));
}

TEST(kv_reads_are_deduplicated_too) {
    Feeder f;
    f(Command::put(1, 1, "k", "v1"));
    const Command read = Command::get(2, 1, "k");
    CHECK((f.result(read) == Result{true, some("v1")}));
    f(Command::put(1, 2, "k", "v2"));
    CHECK((f.result(read) == Result{true, some("v1")}));  // the original answer
}

TEST(kv_stale_requests_are_ignored) {
    Feeder f;
    f(Command::put(1, 5, "k", "new"));
    CHECK(!f(Command::put(1, 4, "k", "old")).has_value());
    CHECK(f.sm.is_stale(1, 4));
    CHECK(!f.sm.is_stale(1, 5));
    CHECK(!f.sm.cached(1, 4).has_value());
    CHECK_EQ(f.sm.data().at("k"), std::string("new"));
    CHECK_EQ(f.sm.stats().stale, std::uint64_t{1});
}

TEST(kv_skips_noops_and_garbage_but_advances) {
    KvStateMachine sm;
    CHECK(!sm.apply(1, LogEntry::noop(1)).has_value());
    CHECK(!sm.apply(2, LogEntry{1, "not a command"}).has_value());
    CHECK(sm.apply(3, LogEntry{1, encode(Command::put(1, 1, "k", "v"))}).has_value());
    CHECK_EQ(sm.last_applied(), LogIndex{3});
    CHECK_EQ(sm.stats().noops, std::uint64_t{1});
    CHECK_EQ(sm.stats().malformed, std::uint64_t{1});
    CHECK_EQ(sm.executions().size(), std::size_t{1});
    CHECK_EQ(sm.executions()[0].index, LogIndex{3});
}

TEST(kv_rejects_out_of_order_apply) {
    KvStateMachine sm;
    CHECK_THROWS(sm.apply(2, LogEntry::noop(1)));
    sm.apply(1, LogEntry::noop(1));
    CHECK_THROWS(sm.apply(1, LogEntry::noop(1)));
}

TEST(kv_replaying_a_log_rebuilds_identical_state) {
    // Given: a random log with retries, stale requests, no-ops and garbage.
    // Approach: replay it into two fresh machines. Data, sessions and the
    //           execution record must match exactly.
    quorum::sim::Rng rng(11);
    std::vector<LogEntry> log;
    std::vector<Seq> seq(4, 0);
    for (int i = 0; i < 2000; ++i) {
        const auto roll = rng.uniform(0, 99);
        if (roll < 3) {
            log.push_back(LogEntry::noop(1));
            continue;
        }
        if (roll < 4) {
            log.push_back(LogEntry{1, "garbage"});
            continue;
        }
        const ClientId c = rng.uniform(1, 3);
        // Mostly new requests, some retries, a few stale ones.
        const auto kind = rng.uniform(0, 9);
        if (kind < 7 || seq[c] == 0) ++seq[c];
        const Seq s = kind == 9 && seq[c] > 1 ? seq[c] - 1 : seq[c];
        const std::string key = "k" + std::to_string(rng.uniform(0, 4));
        const std::string val = "v" + std::to_string(i);
        Command cmd;
        switch (rng.uniform(0, 3)) {
            case 0: cmd = Command::put(c, s, key, val); break;
            case 1: cmd = Command::get(c, s, key); break;
            case 2: cmd = Command::del(c, s, key); break;
            default:
                cmd = Command::cas(c, s, key, rng.bernoulli(0.5) ? absent : some("v" + std::to_string(i - 3)),
                                   val);
        }
        log.push_back(LogEntry{1, encode(cmd)});
    }
    KvStateMachine a, b;
    for (std::size_t i = 0; i < log.size(); ++i) {
        a.apply(i + 1, log[i]);
        b.apply(i + 1, log[i]);
    }
    CHECK(a.data() == b.data());
    CHECK(a.sessions() == b.sessions());
    CHECK(a.executions() == b.executions());
    CHECK(a.stats().duplicates > 0 && a.stats().stale > 0 && a.stats().executed > 1000);
}

TEST(kv_codecs_round_trip) {
    const std::string bin("a b\0\n", 5);
    for (const Command& c : {Command::put(1, 1, "k", "v"), Command::put(2, 9, bin, bin),
                             Command::get(3, 4, ""), Command::del(4, 5, "key with spaces"),
                             Command::cas(5, 6, "k", absent, "new"),
                             Command::cas(5, 7, "k", some(""), ""), Command::cas(5, 8, "k", some(bin), bin)}) {
        CHECK((decode_command(encode(c)) == std::optional<Command>{c}));
        CHECK((decode_request(encode_request(c)) == std::optional<Command>{c}));
    }
    for (const Reply& r : {Reply{1, 2, Result{true, absent}}, Reply{1, 2, Result{false, some(bin)}},
                           Reply{3, 4, Redirect{std::nullopt}}, Reply{3, 4, Redirect{NodeId{5}}}}) {
        CHECK((decode_reply(encode(r)) == std::optional<Reply>{r}));
    }
    CHECK_EQ(encode(Command::put(1, 2, "k", "v")), std::string("PUT 1 2 1 k 1 v"));
    CHECK_EQ(encode(Reply{1, 2, Redirect{NodeId{3}}}), std::string("KRESP 1 2 1 1 3"));
}

TEST(kv_codecs_reject_malformed) {
    for (const char* bad : {"", "PUT 1 2 1 k", "PUT 1 0 1 k 1 v", "PUT 1 2 1 k 1 v ", "GET 1 2 1 k 1 v",
                            "FOO 1 2 1 k", "CAS 1 2 1 k 1 v", "CAS 1 2 1 k 2 1 v", "DEL -1 2 1 k"})
        CHECK(!decode_command(bad).has_value());
    for (const char* bad : {"KREQ", "KREQ 3 GET", "KREQ 15 GET 1 2 1 k", "XREQ 13 GET 1 2 1 k"})
        CHECK(!decode_request(bad).has_value());
    CHECK(decode_request("KREQ 11 GET 1 2 1 k").has_value());
    for (const char* bad : {"KRESP 1 2", "KRESP 1 2 0 1", "KRESP 1 2 2 0", "KRESP 1 2 1 1",
                            "KRESP 1 2 1 0 7", "KRESP 1 2 0 1 0 "})
        CHECK(!decode_reply(bad).has_value());
}
