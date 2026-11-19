#include "quorum/kv/linearizability.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace quorum::kv {

const char* to_string(Verdict v) {
    switch (v) {
        case Verdict::Linearizable: return "linearizable";
        case Verdict::NotLinearizable: return "not linearizable";
        case Verdict::Unknown: return "unknown";
    }
    return "?";
}

ModelStep model_step(const std::optional<std::string>& state, const Command& c) {
    switch (c.op) {
        case Op::Put: return {c.value, Result{true, std::nullopt}};
        case Op::Get: return {state, Result{true, state}};
        case Op::Delete: return {std::nullopt, Result{state.has_value(), std::nullopt}};
        case Op::Cas:
            if (state == c.expected) return {c.value, Result{true, state}};
            return {state, Result{false, state}};
    }
    return {state, Result{}};
}

namespace {

constexpr Tick kNever = std::numeric_limits<Tick>::max();

// One operation on one key, as the search sees it.
struct SearchOp {
    std::size_t history_index = 0;
    Command command;
    Tick invoked = 0;
    std::optional<Tick> completed;  // nullopt: incomplete (may or may not have happened)
    std::optional<Result> result;
    bool wildcard = false;  // completed, but any result is accepted (for explanations)
};

enum class Outcome { Ok, Fail, Unknown };

struct SearchResult {
    Outcome outcome = Outcome::Ok;
    std::uint64_t states = 0;
    std::set<std::optional<std::string>> culprit_states;  // exhaustive mode only
};

// --- Event list ------------------------------------------------------------

struct Event {
    bool is_call = false;
    std::size_t op = 0;
    Tick time = 0;
    int phase = 0;  // within a tick: earlier-invoked returns, then calls, then same-tick returns
};

// Approach: sort by (time, phase, op). An incomplete op returns at kNever.
//           The phase puts a reply delivered at tick t before an operation
//           invoked at t (they are sequential in KvCluster). It keeps an op
//           invoked and completed in the same tick ordered call-then-return.
std::vector<Event> build_events(const std::vector<SearchOp>& ops) {
    std::vector<Event> ev;
    ev.reserve(ops.size() * 2);
    for (std::size_t i = 0; i < ops.size(); ++i) {
        ev.push_back(Event{true, i, ops[i].invoked, 1});
        const Tick t = ops[i].completed ? *ops[i].completed : kNever;
        ev.push_back(Event{false, i, t, t == ops[i].invoked ? 2 : 0});
    }
    std::sort(ev.begin(), ev.end(), [](const Event& a, const Event& b) {
        if (a.time != b.time) return a.time < b.time;
        if (a.phase != b.phase) return a.phase < b.phase;
        return a.op < b.op;
    });
    return ev;
}

// --- Memoization -----------------------------------------------------------

struct Config {
    std::vector<std::uint64_t> bits;  // which ops are linearized
    std::optional<std::string> state;
    bool operator==(const Config&) const = default;
};

struct ConfigHash {
    std::size_t operator()(const Config& c) const {
        std::uint64_t h = 0x9e3779b97f4a7c15ULL;
        for (std::uint64_t w : c.bits) h = (h ^ w) * 0x100000001b3ULL;
        h ^= c.state ? std::hash<std::string>{}(*c.state) : 0x5bd1e995ULL;
        return static_cast<std::size_t>(h ^ (h >> 29));
    }
};

// --- Search ----------------------------------------------------------------

// Given: the operations on one key.
// Approach: Wing & Gong with Lowe's memoization, over a doubly linked event
//           list. At the list head, try each call that appears before the
//           first return. A call can be linearized if the model gives it its
//           recorded result. On success lift its call and return out of the
//           list. On reaching a return (an op that must already have been
//           linearized), backtrack. Done once every completed op is placed.
//           With `culprit` set, explore exhaustively and record every state
//           in which the culprit could take effect.
SearchResult search(const std::vector<SearchOp>& ops, std::uint64_t max_states,
                    std::optional<std::size_t> culprit = std::nullopt) {
    SearchResult out;
    const std::vector<Event> ev = build_events(ops);
    const int n = static_cast<int>(ev.size());
    const int head = n;  // sentinel
    std::vector<int> next(n + 1), prev(n + 1), match(n);
    std::vector<int> call_of(ops.size()), ret_of(ops.size());
    for (int i = 0; i < n; ++i) (ev[i].is_call ? call_of : ret_of)[ev[i].op] = i;
    for (int i = 0; i < n; ++i) match[i] = ev[i].is_call ? ret_of[ev[i].op] : call_of[ev[i].op];
    next[head] = n > 0 ? 0 : -1;
    for (int i = 0; i < n; ++i) {
        prev[i] = i == 0 ? head : i - 1;
        next[i] = i + 1 < n ? i + 1 : -1;
    }
    auto lift = [&](int e) {
        next[prev[e]] = next[e];
        prev[next[e]] = prev[e];  // a call is always followed by its own return
        const int m = match[e];
        next[prev[m]] = next[m];
        if (next[m] != -1) prev[next[m]] = prev[m];
    };
    auto unlift = [&](int e) {
        const int m = match[e];
        next[prev[m]] = m;
        if (next[m] != -1) prev[next[m]] = m;
        next[prev[e]] = e;
        prev[next[e]] = e;
    };

    std::size_t remaining = 0;
    for (const SearchOp& o : ops) remaining += o.completed ? 1 : 0;
    const std::size_t words = (ops.size() + 63) / 64;
    Config cur{std::vector<std::uint64_t>(words, 0), std::nullopt};
    std::unordered_set<Config, ConfigHash> seen;
    struct Frame {
        int entry;
        std::optional<std::string> state;
    };
    std::vector<Frame> stack;
    bool found = false;
    int entry = next[head];

    for (;;) {
        bool backtrack = false;
        if (remaining == 0) {
            found = true;
            if (!culprit) {
                out.outcome = Outcome::Ok;
                return out;
            }
            backtrack = true;
        } else if (ev[entry].is_call) {
            const std::size_t i = ev[entry].op;
            const SearchOp& o = ops[i];
            if (culprit && i == *culprit) out.culprit_states.insert(cur.state);
            const ModelStep st = model_step(cur.state, o.command);
            const bool fits = !o.completed || o.wildcard || st.result == *o.result;
            if (fits) {
                cur.bits[i / 64] |= std::uint64_t{1} << (i % 64);
                Config key{cur.bits, st.next};
                if (seen.insert(std::move(key)).second) {
                    if (++out.states > max_states) {
                        out.outcome = Outcome::Unknown;
                        return out;
                    }
                    stack.push_back(Frame{entry, cur.state});
                    cur.state = st.next;
                    lift(entry);
                    if (o.completed) --remaining;
                    entry = next[head];
                    continue;
                }
                cur.bits[i / 64] &= ~(std::uint64_t{1} << (i % 64));
            }
            entry = next[entry];
            continue;
        } else {
            backtrack = true;  // reached the return of an op not yet linearized
        }

        if (backtrack) {
            if (stack.empty()) {
                out.outcome = found ? Outcome::Ok : Outcome::Fail;
                return out;
            }
            const Frame f = stack.back();
            stack.pop_back();
            const std::size_t i = ev[f.entry].op;
            cur.bits[i / 64] &= ~(std::uint64_t{1} << (i % 64));
            cur.state = f.state;
            if (ops[i].completed) ++remaining;
            unlift(f.entry);
            entry = next[f.entry];
        }
    }
}

// --- Explanations ------------------------------------------------------------

std::string quote(const std::optional<std::string>& v) {
    if (!v) return "absent";
    std::string out = "\"";
    for (unsigned char ch : *v) {
        if (ch == '"' || ch == '\\') {
            out += '\\';
            out += static_cast<char>(ch);
        } else if (ch >= 0x20 && ch < 0x7f) {
            out += static_cast<char>(ch);
        } else {
            static const char* hex = "0123456789abcdef";
            out += "\\x";
            out += hex[ch >> 4];
            out += hex[ch & 15];
        }
    }
    return out + "\"";
}

std::string describe_command(const Command& c) {
    switch (c.op) {
        case Op::Put: return "put(" + quote(c.value) + ")";
        case Op::Get: return "get()";
        case Op::Delete: return "delete()";
        case Op::Cas: return "cas(" + quote(c.expected) + " -> " + quote(c.value) + ")";
    }
    return "?";
}

std::string describe_result(Op op, const Result& r) {
    switch (op) {
        case Op::Put: return "ok";
        case Op::Get: return quote(r.value);
        case Op::Delete: return r.ok ? "deleted" : "was absent";
        case Op::Cas: return std::string(r.ok ? "swapped" : "failed") + ", was " + quote(r.value);
    }
    return "?";
}

std::string describe_op(const SearchOp& o) {
    std::ostringstream os;
    os << "[t=" << o.invoked << ", ";
    if (o.completed) os << "t=" << *o.completed;
    else os << "no reply";
    os << "]  client " << o.command.client << " #" << o.command.seq << "  "
       << describe_command(o.command) << "  -> ";
    os << (o.result ? describe_result(o.command.op, *o.result) : "(unknown)");
    return os.str();
}

bool changes_nothing(const SearchOp& o) {
    // A completed op whose own result shows it left the key unchanged. Removing
    // it removes a constraint and can never create a failure.
    if (!o.completed || !o.result) return false;
    switch (o.command.op) {
        case Op::Get: return true;
        case Op::Cas:
        case Op::Delete: return !o.result->ok;
        case Op::Put: return false;
    }
    return false;
}

// a happened before b in real time (a's reply came no later than b's invoke).
bool precedes(const SearchOp& a, const SearchOp& b) {
    return a.completed && *a.completed <= b.invoked;
}

bool observes(const SearchOp& o) {
    // Its result depends on the key's value. A plain put's doesn't.
    return o.command.op != Op::Put;
}

// Given: a write W, and a completed put P that started after W finished.
// Approach: if no operation that observes the key overlaps the span from W to
//           P (each one ends before W starts or starts after P ends), nobody
//           can ever see W's effect. In any linearization, removing W changes
//           no result, so removing it can't create a failure.
bool superseded(const std::vector<SearchOp>& ops, std::size_t w) {
    if (ops[w].command.op == Op::Get) return false;
    for (std::size_t p = 0; p < ops.size(); ++p) {
        if (p == w || ops[p].command.op != Op::Put || !ops[p].completed || !precedes(ops[w], ops[p])) continue;
        bool hidden = true;
        for (std::size_t o = 0; o < ops.size() && hidden; ++o) {
            if (o == w || o == p || !observes(ops[o])) continue;
            if (!ops[o].completed) continue;  // no result to constrain; its effect is overwritten by P
            hidden = precedes(ops[o], ops[w]) || precedes(ops[p], ops[o]);
        }
        if (hidden) return true;
    }
    return false;
}

std::string explain(const std::string& key, const std::vector<SearchOp>& ops, std::uint64_t max_states) {
    // 1. Shortest failing real-time prefix: at each reply, in event order,
    //    take the ops already invoked. Those not yet answered become
    //    incomplete (incomplete gets are dropped).
    const std::vector<Event> ev = build_events(ops);
    std::vector<SearchOp> prefix;
    std::size_t culprit = 0;
    bool located = false;
    std::vector<bool> invoked(ops.size(), false), answered(ops.size(), false);
    for (const Event& e : ev) {
        if (e.is_call) {
            invoked[e.op] = true;
            continue;
        }
        if (!ops[e.op].completed) break;  // only incomplete returns remain
        answered[e.op] = true;
        std::vector<SearchOp> cut;
        std::size_t c = 0;
        for (std::size_t i = 0; i < ops.size(); ++i) {
            if (!invoked[i]) continue;
            SearchOp o = ops[i];
            if (!answered[i]) {
                if (o.command.op == Op::Get) continue;
                o.completed.reset();
                o.result.reset();
            }
            if (i == e.op) c = cut.size();
            cut.push_back(std::move(o));
        }
        const SearchResult r = search(cut, max_states);
        if (r.outcome == Outcome::Unknown) return "the search bound was reached while explaining the failure";
        if (r.outcome == Outcome::Fail) {
            prefix = std::move(cut);
            culprit = c;
            located = true;
            break;
        }
    }
    if (!located) return "no failing prefix found (the full history fails only as a whole)";

    // 2. Drop operations whose removal can't create a failure (ones that
    //    leave the key unchanged, and writes nobody could observe) for as long
    //    as the rest still fails. Repeat until nothing more can go. For
    //    readability, keep the write of the value the culprit actually
    //    returned, so a stale read still shows where its value came from.
    const std::optional<std::string> culprit_saw =
        prefix[culprit].result ? prefix[culprit].result->value : std::nullopt;
    auto wrote_what_culprit_saw = [&](const SearchOp& o) {
        return culprit_saw && (o.command.op == Op::Put || o.command.op == Op::Cas) && o.command.value == *culprit_saw;
    };
    for (bool changed = true; changed;) {
        changed = false;
        for (std::size_t i = 0; i < prefix.size();) {
            const bool removable =
                changes_nothing(prefix[i]) || (superseded(prefix, i) && !wrote_what_culprit_saw(prefix[i]));
            if (i == culprit || !removable) {
                ++i;
                continue;
            }
            std::vector<SearchOp> without = prefix;
            without.erase(without.begin() + static_cast<std::ptrdiff_t>(i));
            const std::size_t c = culprit > i ? culprit - 1 : culprit;
            if (search(without, max_states).outcome == Outcome::Fail) {
                prefix = std::move(without);
                culprit = c;
                changed = true;
            } else {
                ++i;
            }
        }
    }

    // 3. Where could the culprit take effect, and what would it have seen?
    std::vector<SearchOp> probe = prefix;
    probe[culprit].wildcard = true;
    const SearchResult where = search(probe, max_states, culprit);
    const SearchOp& bad = prefix[culprit];

    std::ostringstream os;
    os << "key " << quote(key) << " is not linearizable.\n";
    os << "  Culprit: " << describe_op(bad) << "\n";
    if (where.outcome == Outcome::Unknown) {
        os << "  (the search bound was reached while listing the possible states)\n";
    } else if (where.culprit_states.empty()) {
        os << "  It has no point at which it could take effect consistently with the other operations.\n";
    } else {
        os << "  Whenever it could take effect, the key held ";
        std::set<std::string> would;
        bool first = true;
        for (const auto& s : where.culprit_states) {
            os << (first ? "" : " or ") << quote(s);
            first = false;
            would.insert(describe_result(bad.command.op, model_step(s, bad.command).result));
        }
        os << ", so it would have returned ";
        first = true;
        for (const auto& w : would) {
            os << (first ? "" : " or ") << w;
            first = false;
        }
        os << ", not " << describe_result(bad.command.op, *bad.result) << ".\n";
    }
    os << "  Operations involved (minimal subset, by invoke time):\n";
    std::vector<std::size_t> order(prefix.size());
    for (std::size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::stable_sort(order.begin(), order.end(),
                     [&](std::size_t a, std::size_t b) { return prefix[a].invoked < prefix[b].invoked; });
    for (std::size_t i : order) {
        os << "    " << describe_op(prefix[i]);
        if (i == culprit) os << "   <- culprit";
        os << "\n";
    }
    return os.str();
}

}  // namespace

CheckResult check_linearizable(const std::vector<OpRecord>& history, CheckOptions options) {
    std::map<std::string, std::vector<SearchOp>> by_key;
    for (std::size_t i = 0; i < history.size(); ++i) {
        const OpRecord& r = history[i];
        if (r.completed && *r.completed < r.invoked)
            throw std::invalid_argument("check_linearizable: completion before invoke");
        if (r.completed.has_value() != r.result.has_value())
            throw std::invalid_argument("check_linearizable: completion and result must come together");
        if (!r.completed && r.command.op == Op::Get) continue;  // no effect, no constraint
        by_key[r.command.key].push_back(SearchOp{i, r.command, r.invoked, r.completed, r.result, false});
    }

    CheckResult out;
    for (const auto& [key, ops] : by_key) {
        KeyReport rep;
        rep.key = key;
        rep.ops = ops.size();
        const SearchResult r = search(ops, options.max_states_per_key);
        rep.states = r.states;
        if (r.outcome == Outcome::Fail) {
            rep.verdict = Verdict::NotLinearizable;
            rep.explanation = explain(key, ops, options.max_states_per_key);
        } else if (r.outcome == Outcome::Unknown) {
            rep.verdict = Verdict::Unknown;
            rep.explanation = "key " + quote(key) + ": unknown, because the search explored more than " +
                              std::to_string(options.max_states_per_key) + " states over " +
                              std::to_string(ops.size()) + " operations";
        }
        out.ops += rep.ops;
        out.states += rep.states;
        out.keys.push_back(std::move(rep));
    }
    for (const KeyReport& k : out.keys)
        if (k.verdict == Verdict::NotLinearizable) {
            out.verdict = Verdict::NotLinearizable;
            out.explanation = k.explanation;
            return out;
        }
    for (const KeyReport& k : out.keys)
        if (k.verdict == Verdict::Unknown) {
            out.verdict = Verdict::Unknown;
            out.explanation = k.explanation;
            return out;
        }
    return out;
}

}  // namespace quorum::kv
