// Given: the linearizability checker is the oracle for every KV fuzz run, so
//        it must be right in both directions.
// Approach: hand-built histories with known answers: sequential, stale reads,
//           concurrency that allows either order, incomplete writes that did
//           or didn't happen, cas, the real-time tie rule, per-key isolation,
//           the search bound, and explanation contents.

#include "quorum/kv/linearizability.h"

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "harness.h"
#include "quorum/kv/state_machine.h"

using namespace quorum::kv;

namespace {

const std::optional<std::string> absent;
std::optional<std::string> some(std::string s) { return s; }
constexpr std::optional<Tick> pending = std::nullopt;

// Builders: (client, seq) are made unique automatically.
struct H {
    std::vector<OpRecord> ops;
    Seq next = 1;

    H& add(Command c, Tick inv, std::optional<Tick> done, std::optional<Result> r) {
        c.seq = next++;
        ops.push_back(OpRecord{std::move(c), inv, done, done ? r : std::nullopt});
        return *this;
    }
    H& put(ClientId c, std::string k, std::string v, Tick inv, std::optional<Tick> done) {
        return add(Command::put(c, 0, std::move(k), std::move(v)), inv, done, Result{true, absent});
    }
    H& get(ClientId c, std::string k, std::optional<std::string> saw, Tick inv, Tick done) {
        return add(Command::get(c, 0, std::move(k)), inv, done, Result{true, std::move(saw)});
    }
    H& del(ClientId c, std::string k, bool existed, Tick inv, std::optional<Tick> done) {
        return add(Command::del(c, 0, std::move(k)), inv, done, Result{existed, absent});
    }
    H& cas(ClientId c, std::string k, std::optional<std::string> expected, std::string v, bool ok,
           std::optional<std::string> was, Tick inv, std::optional<Tick> done) {
        return add(Command::cas(c, 0, std::move(k), std::move(expected), std::move(v)), inv, done,
                   Result{ok, std::move(was)});
    }
    Verdict verdict(CheckOptions o = {}) const { return check_linearizable(ops, o).verdict; }
};

bool mentions(const CheckResult& r, const std::string& what) {
    return r.explanation.find(what) != std::string::npos;
}

}  // namespace

TEST(lin_empty_and_sequential_histories_pass) {
    CHECK(H{}.verdict() == Verdict::Linearizable);
    H h;
    h.get(1, "k", absent, 0, 5).put(1, "k", "a", 10, 20).get(1, "k", some("a"), 30, 40);
    h.cas(1, "k", some("a"), "b", true, some("a"), 50, 60).del(1, "k", true, 70, 80).get(2, "k", absent, 90, 99);
    const CheckResult r = check_linearizable(h.ops);
    CHECK(r.verdict == Verdict::Linearizable);
    CHECK_EQ(r.ops, std::uint64_t{6});
    CHECK(r.explanation.empty());
}

TEST(lin_stale_read_after_completed_write_fails_with_explanation) {
    H h;
    h.put(1, "k", "a", 0, 10).put(1, "k", "b", 20, 30).get(2, "k", some("a"), 40, 50);
    const CheckResult r = check_linearizable(h.ops);
    CHECK(r.verdict == Verdict::NotLinearizable);
    CHECK(mentions(r, "key \"k\" is not linearizable"));
    CHECK(mentions(r, "Culprit: [t=40, t=50]  client 2 #3  get()  -> \"a\""));
    CHECK(mentions(r, "the key held \"b\", so it would have returned \"b\", not \"a\""));
    CHECK(mentions(r, "[t=20, t=30]  client 1 #2  put(\"b\")"));
}

TEST(lin_concurrent_ops_are_fine_in_either_order) {
    // A get overlapping a put may see the old value or the new one.
    for (const auto& saw : {absent, some("a")}) {
        H h;
        h.put(1, "k", "a", 0, 100).get(2, "k", saw, 10, 20);
        CHECK(h.verdict() == Verdict::Linearizable);
    }
    // Two overlapping puts: a later read may see either one...
    for (const char* last : {"a", "b"}) {
        H h;
        h.put(1, "k", "a", 0, 50).put(2, "k", "b", 10, 60).get(3, "k", some(last), 70, 80);
        CHECK(h.verdict() == Verdict::Linearizable);
    }
    // ...but two later reads can't disagree about which one won.
    H h;
    h.put(1, "k", "a", 0, 50).put(2, "k", "b", 10, 60).get(3, "k", some("a"), 70, 80).get(3, "k", some("b"), 90, 95);
    CHECK(h.verdict() == Verdict::NotLinearizable);
}

TEST(lin_incomplete_write_that_happened) {
    H h;
    h.put(1, "k", "a", 0, pending).get(2, "k", some("a"), 50, 60);
    CHECK(h.verdict() == Verdict::Linearizable);
}

TEST(lin_incomplete_write_that_did_not_happen) {
    H h;
    h.put(1, "k", "a", 0, pending).get(2, "k", absent, 50, 60).get(2, "k", absent, 70, 80);
    CHECK(h.verdict() == Verdict::Linearizable);
}

TEST(lin_incomplete_write_takes_effect_only_after_invoke_and_sticks) {
    // Can't be seen before it was invoked.
    H early;
    early.get(2, "k", some("a"), 0, 10).put(1, "k", "a", 20, pending);
    CHECK(early.verdict() == Verdict::NotLinearizable);
    // Once seen, it has happened: a later read can't go back to absent.
    H flicker;
    flicker.put(1, "k", "a", 0, pending).get(2, "k", some("a"), 50, 60).get(2, "k", absent, 70, 80);
    const CheckResult r = check_linearizable(flicker.ops);
    CHECK(r.verdict == Verdict::NotLinearizable);
    CHECK(mentions(r, "[t=0, no reply]  client 1 #1  put(\"a\")  -> (unknown)"));
}

TEST(lin_cas_cases) {
    // Two concurrent create-if-absent CAS can't both succeed.
    H both;
    both.cas(1, "k", absent, "x", true, absent, 0, 50).cas(2, "k", absent, "y", true, absent, 10, 60);
    const CheckResult r = check_linearizable(both.ops);
    CHECK(r.verdict == Verdict::NotLinearizable);
    CHECK(mentions(r, "cas(absent -> \""));
    // One wins, the other fails and sees the winner's value.
    H one;
    one.cas(1, "k", absent, "x", true, absent, 0, 50).cas(2, "k", absent, "y", false, some("x"), 10, 60);
    CHECK(one.verdict() == Verdict::Linearizable);
    // A failed CAS must report the value actually there.
    H wrong;
    wrong.put(1, "k", "a", 0, 10).cas(2, "k", some("z"), "q", false, some("b"), 20, 30);
    CHECK(wrong.verdict() == Verdict::NotLinearizable);
    // An incomplete CAS that may have swapped explains a later read.
    H maybe;
    maybe.put(1, "k", "a", 0, 10).cas(2, "k", some("a"), "b", false, absent, 20, pending);
    maybe.get(3, "k", some("b"), 40, 50);
    CHECK(maybe.verdict() == Verdict::Linearizable);
}

TEST(lin_delete_cases) {
    H ok;
    ok.put(1, "k", "a", 0, 10).del(2, "k", true, 20, 30).del(2, "k", false, 40, 50).get(3, "k", absent, 60, 70);
    CHECK(ok.verdict() == Verdict::Linearizable);
    H bad;
    bad.del(1, "k", true, 0, 10);  // nothing was ever there
    CHECK(bad.verdict() == Verdict::NotLinearizable);
}

TEST(lin_reply_and_invoke_in_the_same_tick_are_ordered) {
    // A reply delivered at tick 10 precedes an operation invoked at tick 10.
    H h;
    h.put(1, "k", "a", 0, 10).get(2, "k", absent, 10, 20);
    CHECK(h.verdict() == Verdict::NotLinearizable);
    H overlap;
    overlap.put(1, "k", "a", 0, 11).get(2, "k", absent, 10, 20);
    CHECK(overlap.verdict() == Verdict::Linearizable);
}

TEST(lin_keys_are_checked_independently) {
    H h;
    h.put(1, "a", "1", 0, 10).get(2, "a", some("1"), 20, 30);
    h.put(1, "b", "1", 0, 10).put(1, "b", "2", 20, 30).get(2, "b", some("1"), 40, 50);
    const CheckResult r = check_linearizable(h.ops);
    CHECK(r.verdict == Verdict::NotLinearizable);
    CHECK_EQ(r.keys.size(), std::size_t{2});
    CHECK(r.keys[0].key == "a" && r.keys[0].verdict == Verdict::Linearizable);
    CHECK(r.keys[1].key == "b" && r.keys[1].verdict == Verdict::NotLinearizable);
    CHECK(mentions(r, "key \"b\""));
    CHECK(!mentions(r, "key \"a\""));
}

TEST(lin_explanation_is_minimal) {
    // Lots of harmless reads and a late op on another key. The explanation
    // keeps only the two puts and the stale read.
    H h;
    h.put(1, "k", "a", 0, 10);
    for (int i = 0; i < 10; ++i) h.get(3, "k", some("a"), 11 + 2 * i, 12 + 2 * i);
    h.put(1, "k", "b", 40, 50);
    for (int i = 0; i < 10; ++i) h.get(3, "k", some("b"), 51 + 2 * i, 52 + 2 * i);
    h.get(2, "k", some("a"), 80, 90);                  // the stale read
    h.put(1, "k", "c", 100, 110).get(3, "k", some("c"), 120, 130);  // after the culprit
    const CheckResult r = check_linearizable(h.ops);
    CHECK(r.verdict == Verdict::NotLinearizable);
    const auto listed = std::count(r.explanation.begin(), r.explanation.end(), '\n');
    CHECK_EQ(listed, std::ptrdiff_t{7});  // 4 header lines + 3 operations
    CHECK(mentions(r, "put(\"a\")") && mentions(r, "put(\"b\")") && !mentions(r, "put(\"c\")"));
}

namespace {

// n concurrent puts and n concurrent gets. Get i (by invoke order) saw the
// value of put n-1-i, so the history is linearizable, but the search can only
// find the right order by trying exponentially many subsets.
H overlapping(int n) {
    H h;
    for (int i = 0; i < n; ++i) h.put(1 + i % 4, "k", "v" + std::to_string(i), 0, 1000);
    for (int i = 0; i < n; ++i) h.get(5, "k", some("v" + std::to_string(n - 1 - i)), 10 + i, 900);
    return h;
}

}  // namespace

TEST(lin_bound_reports_unknown_not_pass) {
    const H small = overlapping(5);
    CHECK(small.verdict() == Verdict::Linearizable);
    CHECK(small.verdict(CheckOptions{20}) == Verdict::Unknown);

    // 24 concurrent operations exceed even the default bound of 1M states. The
    // verdict is Unknown, never a pass.
    const CheckResult big = check_linearizable(overlapping(12).ops);
    CHECK(big.verdict == Verdict::Unknown);
    CHECK(big.explanation.find("unknown") != std::string::npos);
    CHECK(big.explanation.find("24 operations") != std::string::npos);
}

TEST(lin_rejects_malformed_histories) {
    std::vector<OpRecord> backwards{OpRecord{Command::put(1, 1, "k", "v"), 10, Tick{5}, Result{true, absent}}};
    CHECK_THROWS(check_linearizable(backwards));
    std::vector<OpRecord> no_result{OpRecord{Command::put(1, 1, "k", "v"), 0, Tick{5}, std::nullopt}};
    CHECK_THROWS(check_linearizable(no_result));
}

TEST(lin_model_matches_the_state_machine) {
    // The checker's sequential model must agree with KvStateMachine.
    const std::vector<Command> cmds{Command::put(1, 1, "k", "a"),        Command::get(1, 2, "k"),
                                    Command::cas(1, 3, "k", some("a"), "b"), Command::cas(1, 4, "k", absent, "c"),
                                    Command::del(1, 5, "k"),             Command::del(1, 6, "k"),
                                    Command::cas(1, 7, "k", absent, "d"), Command::get(1, 8, "k")};
    KvStateMachine sm;
    std::optional<std::string> state;
    for (std::size_t i = 0; i < cmds.size(); ++i) {
        const ModelStep st = model_step(state, cmds[i]);
        const auto applied = sm.apply(i + 1, quorum::raft::LogEntry{1, encode(cmds[i])});
        CHECK((applied->result == st.result));
        state = st.next;
        const auto it = sm.data().find("k");
        CHECK((state == (it == sm.data().end() ? absent : some(it->second))));
    }
}

TEST(lin_explanation_drops_overwritten_writes_but_keeps_observed_ones) {
    // put a, put b (unobserved, overwritten by put c), put c, stale get -> b.
    H h;
    h.put(1, "k", "a", 0, 10).put(1, "k", "b", 20, 30).put(1, "k", "c", 40, 50).get(2, "k", some("b"), 60, 70);
    const CheckResult r = check_linearizable(h.ops);
    CHECK(r.verdict == Verdict::NotLinearizable);
    CHECK(!mentions(r, "put(\"a\")"));  // overwritten before anyone could look
    CHECK(mentions(r, "put(\"c\")") && mentions(r, "put(\"b\")"));
    // A write that someone observed must stay, even though a later put overwrote it.
    H seen;
    seen.put(1, "k", "a", 0, 10).get(3, "k", some("a"), 12, 18).put(1, "k", "b", 20, 30);
    seen.get(2, "k", some("a"), 40, 50);
    const CheckResult r2 = check_linearizable(seen.ops);
    CHECK(r2.verdict == Verdict::NotLinearizable);
    CHECK(mentions(r2, "put(\"a\")") && mentions(r2, "put(\"b\")"));
}
