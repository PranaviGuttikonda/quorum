#include "quorum/web/simulation.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <set>
#include <stdexcept>

#include "quorum/kv/command.h"
#include "quorum/raft/messages.h"

namespace quorum::web {

namespace {

kv::KvClusterConfig web_config(std::size_t servers) {
    if (servers < 1 || servers > 9) throw std::invalid_argument("Simulation: 1..9 servers");
    kv::KvClusterConfig c;
    c.servers = servers;
    c.clients = Simulation::kClients;
    c.net = sim::NetworkConfig{.min_delay = 8, .max_delay = 20};
    c.client = kv::ClientConfig{.retry_timeout = 300, .redirect_backoff = 20};
    return c;
}

// Minimal JSON output: the snapshot only needs objects, arrays, strings,
// integers, booleans and null.
class Json {
public:
    Json& raw(std::string_view s) {
        out_ += s;
        return *this;
    }
    Json& str(std::string_view s) {
        out_ += '"';
        for (unsigned char c : s) {
            switch (c) {
                case '"': out_ += "\\\""; break;
                case '\\': out_ += "\\\\"; break;
                case '\n': out_ += "\\n"; break;
                case '\r': out_ += "\\r"; break;
                case '\t': out_ += "\\t"; break;
                default:
                    if (c < 0x20) {
                        char buf[8];
                        std::snprintf(buf, sizeof buf, "\\u%04x", c);
                        out_ += buf;
                    } else {
                        out_ += static_cast<char>(c);
                    }
            }
        }
        out_ += '"';
        return *this;
    }
    Json& num(std::uint64_t v) { return raw(std::to_string(v)); }
    // A number with one decimal (formatted the same way on every build).
    Json& fixed1(double v) {
        char buf[32];
        std::snprintf(buf, sizeof buf, "%.1f", v);
        return raw(buf);
    }
    Json& boolean(bool b) { return raw(b ? "true" : "false"); }
    template <typename T>
    Json& opt(const std::optional<T>& v) {
        return v ? num(*v) : raw("null");
    }
    Json& opt_str(const std::optional<std::string>& v) { return v ? str(*v) : raw("null"); }
    // "key": (with a comma first unless it opens the object)
    Json& key(std::string_view k) {
        if (!out_.empty() && out_.back() != '{' && out_.back() != '[') out_ += ',';
        str(k);
        out_ += ':';
        return *this;
    }
    // Starts an array element.
    Json& item() {
        if (!out_.empty() && out_.back() != '[') out_ += ',';
        return *this;
    }
    std::string take() { return std::move(out_); }

private:
    std::string out_;
};

const char* drop_reason(sim::DropReason r) {
    switch (r) {
        case sim::DropReason::RandomLoss: return "loss";
        case sim::DropReason::Partitioned: return "partition";
        case sim::DropReason::SenderDown: return "sender-down";
        case sim::DropReason::ReceiverDown: return "receiver-down";
        case sim::DropReason::None: break;
    }
    return "none";
}

struct Described {
    const char* kind;
    std::string text;
};

std::string describe_command(const kv::Command& c) {
    std::string s = kv::to_string(c.op);
    s += ' ' + c.key;
    if (c.op == kv::Op::Put || c.op == kv::Op::Cas) s += " = " + c.value;
    return s;
}

// Given: a payload on the wire. Approach: decode it with the engine's own
//        codecs and name its kind for colouring. An AppendEntries reply counts
//        as replication when it moves the leader's matchIndex for that
//        follower, otherwise as a heartbeat reply.
Described describe(const raft::RaftCluster& c, const sim::Message& m) {
    if (c.is_server(m.from) && c.is_server(m.to)) {
        const auto msg = raft::decode(m.payload);
        if (!msg) return {"other", "?"};
        if (const auto* rv = std::get_if<raft::RequestVote>(&*msg))
            return {"vote", "RequestVote, term " + std::to_string(rv->term) + ", last log " +
                                std::to_string(rv->last_log_index) + "/t" + std::to_string(rv->last_log_term)};
        if (const auto* rr = std::get_if<raft::RequestVoteReply>(&*msg))
            return {"vote", std::string(rr->vote_granted ? "Vote granted" : "Vote refused") + ", term " +
                                std::to_string(rr->term)};
        if (const auto* ae = std::get_if<raft::AppendEntries>(&*msg)) {
            if (ae->entries.empty())
                return {"heartbeat", "Heartbeat, term " + std::to_string(ae->term) + ", commit " +
                                         std::to_string(ae->leader_commit)};
            const auto first = ae->prev_log_index + 1;
            const auto last = ae->prev_log_index + ae->entries.size();
            return {"append", "AppendEntries " + std::to_string(first) +
                                  (last > first ? ".." + std::to_string(last) : std::string()) + ", term " +
                                  std::to_string(ae->term) + ", commit " + std::to_string(ae->leader_commit)};
        }
        const auto& ar = std::get<raft::AppendEntriesReply>(*msg);
        bool advances = !ar.success;  // a rejection drives the backtracking
        if (const raft::RaftNode* leader = c.node(m.to);
            ar.success && leader && leader->role() == raft::Role::Leader && leader->current_term() == ar.term)
            advances = ar.match_index > leader->match_index(m.from);
        return {advances ? "append" : "heartbeat",
                std::string(ar.success ? "Ack" : "Reject") + ", match " + std::to_string(ar.match_index) +
                    ", term " + std::to_string(ar.term)};
    }
    if (c.is_server(m.to)) {
        if (const auto cmd = kv::decode_request(m.payload)) return {"request", describe_command(*cmd)};
        return {"request", "?"};
    }
    if (const auto rep = kv::decode_reply(m.payload)) {
        if (const auto* res = std::get_if<kv::Result>(&rep->body)) {
            std::string s = res->ok ? "ok" : "failed";
            if (res->value) s += ", value " + *res->value;
            return {"reply", s};
        }
        const auto& rd = std::get<kv::Redirect>(rep->body);
        return {"reply", rd.leader_hint ? "Not the leader: try " + std::to_string(*rd.leader_hint)
                                        : std::string("Not the leader: no leader known")};
    }
    return {"reply", "?"};
}

bool parse_id(std::string_view s, NodeId& out) {
    if (s.empty() || s.size() > 3) return false;
    NodeId v = 0;
    for (char ch : s) {
        if (ch < '0' || ch > '9') return false;
        v = v * 10 + static_cast<NodeId>(ch - '0');
    }
    out = v;
    return true;
}

// The last kLogTail entries. Entries up to `commit` are marked committed.
void write_log(Json& j, const std::vector<raft::LogEntry>& log, raft::LogIndex commit) {
    j.key("log").raw("[");
    const std::size_t from = log.size() > Simulation::kLogTail ? log.size() - Simulation::kLogTail : 0;
    for (std::size_t i = from; i < log.size(); ++i) {
        const raft::LogEntry& e = log[i];
        const raft::LogIndex index = i + 1;
        j.item().raw("{");
        j.key("i").num(index);
        j.key("t").num(e.term);
        j.key("committed").boolean(index <= commit);
        if (e.is_noop()) {
            j.key("noop").boolean(true);
        } else if (const auto cmd = kv::decode_command(e.command)) {
            j.key("client").num(cmd->client);
            j.key("seq").num(cmd->seq);
            j.key("op").str(kv::to_string(cmd->op));
            j.key("key").str(cmd->key);
            if (cmd->op == kv::Op::Put || cmd->op == kv::Op::Cas) j.key("value").str(cmd->value);
        } else {
            j.key("raw").str(e.command);
        }
        j.raw("}");
    }
    j.raw("]");
}

std::uint64_t fnv1a(std::string_view s) {
    std::uint64_t h = 0xcbf29ce484222325ULL;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x100000001b3ULL;
    }
    return h;
}

}  // namespace

Simulation::Simulation(std::uint64_t seed, std::size_t servers)
    : seed_(seed),
      kv_(seed, web_config(servers)),
      queued_(kClients),
      running_(kClients),
      load_rng_(seed ^ 0x10ad10ad10ad10adULL) {}

// One tick: chaos acts first (as a user would, between ticks), then the load
// generator, then queued operations start, then the engine ticks, and finally
// the results are collected and observed.
void Simulation::tick(std::uint32_t n) {
    for (std::uint32_t i = 0; i < n; ++i) {
        chaos_step();
        generate_load();
        start_queued();
        kv_.tick();
        collect();
        observe();
    }
}

void Simulation::start_queued() {
    for (std::size_t c = 0; c < kClients; ++c) {
        if (running_[c] || queued_[c].empty() || !kv_.client(c).idle()) continue;
        WebOp& op = ops_[queued_[c].front()];
        queued_[c].pop_front();
        op.history = kv_.client(c).submit(op.op, op.key, op.value);
        running_[c] = static_cast<std::size_t>(&op - ops_.data());
    }
}

void Simulation::generate_load() {
    if (!load_ || (chaos_ && chaos_->phase == Chaos::Phase::Settling)) return;
    for (std::size_t c = 0; c < kClients; ++c) {
        if (running_[c] || !queued_[c].empty() || !load_rng_.bernoulli(kLoadRate)) continue;
        WebOp w;
        w.client = static_cast<std::uint32_t>(c + 1);
        w.key = "k" + std::to_string(load_rng_.uniform(1, 3));
        w.op = load_rng_.bernoulli(0.5) ? kv::Op::Put : kv::Op::Get;
        if (w.op == kv::Op::Put) w.value = "c" + std::to_string(c + 1) + "-" + std::to_string(++load_ops_);
        w.generated = true;
        enqueue(std::move(w));
    }
}

void Simulation::chaos_step() {
    if (!chaos_ || chaos_->phase != Chaos::Phase::Running) return;
    Chaos& ch = *chaos_;
    auto& c = kv_.raft();
    in_chaos_ = true;
    if (now() >= ch.end) {
        heal();
        for (NodeId id : c.ids()) do_restart(id);
        ch.phase = Chaos::Phase::Settling;
        ch.settle_until = now() + kChaosSettle;
        add_event("chaos", 0, "Chaos over: healed, restarted, waiting for clients");
        in_chaos_ = false;
        return;
    }
    std::vector<NodeId> up, down;
    for (NodeId id : c.ids()) (c.is_up(id) ? up : down).push_back(id);
    if (down.size() < kMaxDown && up.size() > 1 && ch.rng.bernoulli(0.003))
        do_crash(up[ch.rng.uniform(0, up.size() - 1)]);
    for (NodeId id : down)
        if (ch.rng.bernoulli(0.008)) do_restart(id);
    bool split = false;
    for (NodeId id : c.ids()) split |= c.network().group_of(id) != 0;
    if (!split && ch.rng.bernoulli(0.002)) {
        std::vector<NodeId> ids = c.ids();
        for (std::size_t i = ids.size(); i > 1; --i) std::swap(ids[i - 1], ids[ch.rng.uniform(0, i - 1)]);
        const auto cut = static_cast<std::ptrdiff_t>(ch.rng.uniform(1, ids.size() - 1));
        std::string text;
        for (auto it = ids.begin(); it != ids.end(); ++it) {
            if (it != ids.begin()) text += it - ids.begin() == cut ? '|' : ',';
            text += std::to_string(*it);
        }
        partition(text);
    } else if (split && ch.rng.bernoulli(0.006)) {
        heal();
    }
    in_chaos_ = false;
}

void Simulation::collect() {
    for (std::size_t c = 0; c < kClients; ++c) {
        if (!running_[c]) continue;
        WebOp& op = ops_[*running_[c]];
        op.stats = kv_.client(c).op_stats();
        if (kv_.client(c).idle()) {
            op.server = kv_.client(c).answered_by();
            running_[c].reset();
            const kv::OpRecord& r = kv_.history().ops().at(*op.history);
            const Tick latency = *r.completed - r.invoked;
            completions_.push_back(Completion{*r.completed, latency, op.op != kv::Op::Get});
            while (completions_.size() > kLatencyWindow &&
                   completions_.front().at + 10 * kTicksPerSecond < now())
                completions_.pop_front();
            std::string text = describe_command(r.command) + " -> ";
            if (op.op == kv::Op::Get) {
                text += r.result->value ? "\"" + *r.result->value + "\"" : std::string("not found");
            } else {
                text += r.result->ok ? "ok" : "failed";
            }
            text += ", S" + std::to_string(op.server.value_or(0)) + ", " + std::to_string(latency) + " ticks";
            add_event("op", op.server.value_or(0), std::move(text));
        }
    }
    for (const sim::TraceEvent& e : kv_.raft().network().trace())
        if (e.kind == sim::EventKind::Drop) drops_.push_back(Drop{e.tick, e.from, e.to, e.reason});
    kv_.raft().clear_trace();
    while (!drops_.empty() && drops_.front().at + kDropMemory < now()) drops_.pop_front();
}

bool Simulation::has_working_leader() const {
    const auto& c = kv_.raft();
    for (NodeId l : c.leaders()) {
        std::size_t reach = 0;
        for (NodeId id : c.ids()) reach += c.is_up(id) && c.network().can_reach(l, id);
        if (reach * 2 > c.ids().size()) return true;
    }
    return false;
}

void Simulation::observe() {
    const auto& c = kv_.raft();
    for (NodeId l : c.leaders()) {
        const raft::Term t = c.node(l)->current_term();
        if (led_terms_.insert(t).second) {
            ++elections_;
            add_event("elect", l, "S" + std::to_string(l) + " elected leader, term " + std::to_string(t));
        }
    }
    if (!has_working_leader()) ++leaderless_;

    if (chaos_ && chaos_->phase == Chaos::Phase::Settling) {
        bool idle = true;
        for (std::size_t i = 0; i < kClients; ++i) idle &= !running_[i] && queued_[i].empty();
        if (idle || now() >= chaos_->settle_until) {
            chaos_->result = kv::check_linearizable(kv_.history().ops());
            chaos_->phase = Chaos::Phase::Done;
            load_ = chaos_->load_before;
            const auto& r = *chaos_->result;
            add_event("check", 0,
                      std::to_string(r.ops) + " operations, " +
                          (r.verdict == kv::Verdict::Linearizable ? std::string("linearizable")
                                                                  : std::string(kv::to_string(r.verdict))));
        }
    }
}

void Simulation::add_event(std::string kind, NodeId node, std::string text) {
    events_.push_back(Event{now(), std::move(kind), node, in_chaos_, std::move(text)});
    if (events_.size() > kEventCap) {
        events_.pop_front();
        ++first_event_;
    }
}

bool Simulation::do_crash(NodeId id) {
    auto& c = kv_.raft();
    if (!c.is_server(id) || !c.is_up(id)) return false;
    c.crash(id);
    add_event("crash", id, "S" + std::to_string(id) + " crashed");
    return true;
}

bool Simulation::do_restart(NodeId id) {
    auto& c = kv_.raft();
    if (!c.is_server(id) || c.is_up(id)) return false;
    c.restart(id);
    add_event("restart", id, "S" + std::to_string(id) + " restarted");
    return true;
}

bool Simulation::crash(NodeId id) {
    return do_crash(id);
}

bool Simulation::restart(NodeId id) {
    return do_restart(id);
}

bool Simulation::partition(const std::string& text) {
    std::vector<std::vector<NodeId>> groups;
    std::set<NodeId> seen;
    std::string_view rest(text);
    while (true) {
        const auto bar = rest.find('|');
        std::string_view g = rest.substr(0, bar);
        std::vector<NodeId> group;
        while (true) {
            const auto comma = g.find(',');
            NodeId id = 0;
            if (!parse_id(g.substr(0, comma), id) || !kv_.raft().is_server(id) || !seen.insert(id).second)
                return false;
            group.push_back(id);
            if (comma == std::string_view::npos) break;
            g.remove_prefix(comma + 1);
        }
        groups.push_back(std::move(group));
        if (bar == std::string_view::npos) break;
        rest.remove_prefix(bar + 1);
    }
    kv_.raft().partition(groups);
    std::string desc;
    for (const auto& g : groups) {
        if (!desc.empty()) desc += " | ";
        for (std::size_t i = 0; i < g.size(); ++i) desc += (i ? " " : "") + std::string("S") + std::to_string(g[i]);
    }
    add_event("split", 0, "Network split: " + desc);
    return true;
}

void Simulation::heal() {
    bool split = false;
    for (NodeId id : kv_.raft().ids()) split |= kv_.raft().network().group_of(id) != 0;
    kv_.raft().heal();
    if (split) add_event("heal", 0, "Network healed");
}

void Simulation::set_load(bool on) {
    load_ = on;
    if (chaos_ && chaos_->phase != Chaos::Phase::Done) chaos_->load_before = on;
}

bool Simulation::start_chaos() {
    if (chaos_ && chaos_->phase != Chaos::Phase::Done) return false;
    Chaos ch;
    ch.start = now();
    ch.end = now() + kChaosTicks;
    ch.load_before = load_;
    ch.rng = sim::Rng(seed_ ^ 0xc4a05c4a05c4a05cULL ^ (now() * 0x9e3779b97f4a7c15ULL));
    chaos_ = std::move(ch);
    load_ = true;
    in_chaos_ = true;
    add_event("chaos", 0, "Chaos started: 60 simulated seconds of crashes and splits");
    in_chaos_ = false;
    return true;
}

void Simulation::enqueue(WebOp w) {
    w.id = static_cast<std::int64_t>(ops_.size()) + 1;
    ops_.push_back(std::move(w));
    queued_[ops_.back().client - 1].push_back(ops_.size() - 1);
}

std::int64_t Simulation::submit(std::uint32_t client, const std::string& op, const std::string& key,
                                const std::string& value) {
    if (client < 1 || client > kClients || key.empty() || key.size() > 64 || value.size() > 64) return -1;
    WebOp w;
    if (op == "put") {
        w.op = kv::Op::Put;
    } else if (op == "get") {
        w.op = kv::Op::Get;
    } else if (op == "delete" || op == "del") {
        w.op = kv::Op::Delete;
    } else {
        return -1;
    }
    if (w.op == kv::Op::Put && value.empty()) return -1;
    w.client = client;
    w.key = key;
    if (w.op == kv::Op::Put) w.value = value;
    enqueue(std::move(w));
    start_queued();
    return ops_.back().id;
}

std::string Simulation::events_json(std::size_t from) const {
    Json j;
    j.raw("{");
    const std::size_t first = std::max(from, first_event_);
    j.key("first").num(first);
    j.key("total").num(first_event_ + events_.size());
    j.key("events").raw("[");
    for (std::size_t i = first - first_event_; i < events_.size(); ++i) {
        const Event& e = events_[i];
        j.item().raw("{");
        j.key("t").num(e.tick);
        j.key("kind").str(e.kind);
        if (e.node) j.key("node").num(e.node);
        if (e.chaos) j.key("chaos").boolean(true);
        j.key("text").str(e.text);
        j.raw("}");
    }
    j.raw("]}");
    return j.take();
}

std::string Simulation::snapshot() const {
    const raft::RaftCluster& c = kv_.raft();
    const auto& net = c.network();
    Json j;
    j.raw("{");
    j.key("seed").num(seed_);
    j.key("now").num(now());
    j.key("leader").opt(leader());

    j.key("nodes").raw("[");
    for (NodeId id : c.ids()) {
        const raft::RaftNode* n = c.node(id);
        j.item().raw("{");
        j.key("id").num(id);
        j.key("up").boolean(n != nullptr);
        j.key("group").num(net.group_of(id));
        j.key("boots").num(c.boot_count(id));
        if (n) {
            j.key("role").str(raft::to_string(n->role()));
            j.key("term").num(n->current_term());
            j.key("votedFor").opt(n->voted_for());
            j.key("leaderId").opt(n->leader_id());
            j.key("commit").num(n->commit_index());
            j.key("applied").num(n->last_applied());
            j.key("lastIndex").num(n->last_log_index());
            j.key("timer").num(n->role() == raft::Role::Leader ? 0 : n->election_elapsed());
            j.key("timeout").num(n->election_timeout());
            write_log(j, n->log(), n->commit_index());
        } else if (const auto* mem = dynamic_cast<const raft::MemoryStorageProvider*>(&c.storage())) {
            // Down: what survives is the persistent state (Figure 2). Commit
            // index is volatile, so nothing is marked committed.
            const raft::PersistentState p = mem->storage(id).load();
            j.key("term").num(p.current_term);
            j.key("votedFor").opt(p.voted_for);
            j.key("lastIndex").num(p.log.size());
            write_log(j, p.log, 0);
        }
        j.raw("}");
    }
    j.raw("]");

    j.key("messages").raw("[");
    for (const sim::InFlight& f : net.in_flight_messages()) {
        const Described d = describe(c, f.msg);
        j.item().raw("{");
        j.key("id").num(f.msg.id);
        j.key("from").num(f.msg.from);
        j.key("to").num(f.msg.to);
        j.key("sent").num(f.msg.sent_at);
        j.key("at").num(f.msg.deliver_at);
        j.key("copy").boolean(f.duplicate);
        j.key("doomed").boolean(f.doomed);
        j.key("kind").str(d.kind);
        j.key("text").str(d.text);
        j.raw("}");
    }
    j.raw("]");

    j.key("drops").raw("[");
    for (const Drop& d : drops_) {
        j.item().raw("{");
        j.key("at").num(d.at);
        j.key("from").num(d.from);
        j.key("to").num(d.to);
        j.key("reason").str(drop_reason(d.reason));
        j.raw("}");
    }
    j.raw("]");

    j.key("clients").raw("[");
    for (std::size_t i = 0; i < kv_.client_count(); ++i) {
        const kv::KvClient& cl = kv_.client(i);
        j.item().raw("{");
        j.key("id").num(cl.client_id());
        j.key("node").num(cl.node_id());
        j.key("idle").boolean(cl.idle());
        j.key("queued").num(queued_[i].size());
        j.key("hint").opt(cl.leader_hint());
        j.raw("}");
    }
    j.raw("]");

    j.key("ops").raw("[");
    const auto& history = kv_.history().ops();
    // Submitted operations only: the load generator's show up in metrics and
    // events, and would otherwise push typed commands out of the console.
    std::vector<std::size_t> shown;
    for (std::size_t i = ops_.size(); i-- > 0 && shown.size() < kOpTail;)
        if (!ops_[i].generated) shown.push_back(i);
    std::reverse(shown.begin(), shown.end());
    for (std::size_t i : shown) {
        const WebOp& w = ops_[i];
        j.item().raw("{");
        j.key("id").num(static_cast<std::uint64_t>(w.id));
        j.key("client").num(w.client);
        j.key("op").str(kv::to_string(w.op));
        j.key("key").str(w.key);
        if (w.op == kv::Op::Put) j.key("value").str(w.value);
        if (!w.history) {
            j.key("status").str("queued");
        } else {
            const kv::OpRecord& r = history.at(*w.history);
            j.key("invoked").num(r.invoked);
            j.key("status").str(r.completed ? "done" : "running");
            j.key("completed").opt(r.completed);
            if (r.result) {
                j.key("ok").boolean(r.result->ok);
                j.key("result").opt_str(r.result->value);
            }
            j.key("server").opt(w.server);
            j.key("sends").num(w.stats.sends);
            j.key("timeouts").num(w.stats.timeouts);
            j.key("redirects").num(w.stats.redirects);
        }
        j.raw("}");
    }
    j.raw("]");

    j.key("load").boolean(load_);
    j.key("eventCount").num(first_event_ + events_.size());

    // Metrics. Latency is over the last kLatencyWindow operations (nearest
    // rank), writes per second over the last 10 simulated seconds.
    {
        std::uint64_t writes = 0;
        for (const Completion& c : completions_)
            writes += c.write && c.at + 10 * kTicksPerSecond > now();
        std::vector<Tick> lat;
        const std::size_t n = std::min(completions_.size(), kLatencyWindow);
        for (std::size_t i = completions_.size() - n; i < completions_.size(); ++i) lat.push_back(completions_[i].latency);
        std::sort(lat.begin(), lat.end());
        auto rank = [&](double p) -> std::optional<Tick> {
            if (lat.empty()) return std::nullopt;
            const auto k = static_cast<std::size_t>(std::ceil(p * static_cast<double>(lat.size())));
            return lat[std::max<std::size_t>(k, 1) - 1];
        };
        j.key("metrics").raw("{");
        j.key("writesPerSec").fixed1(static_cast<double>(writes) / 10.0);
        j.key("latencyOps").num(lat.size());
        j.key("p50").opt(rank(0.50));
        j.key("p99").opt(rank(0.99));
        j.key("elections").num(elections_);
        j.key("leaderlessTicks").num(leaderless_);
        j.key("ticksPerSecond").num(kTicksPerSecond);
        j.raw("}");
    }

    if (!chaos_) {
        j.key("chaos").raw("null");
    } else {
        const Chaos& ch = *chaos_;
        j.key("chaos").raw("{");
        j.key("phase").str(ch.phase == Chaos::Phase::Running    ? "running"
                           : ch.phase == Chaos::Phase::Settling ? "settling"
                                                                : "done");
        j.key("start").num(ch.start);
        j.key("end").num(ch.end);
        if (ch.result) {
            j.key("verdict").str(kv::to_string(ch.result->verdict));
            j.key("ops").num(ch.result->ops);
            j.key("explanation").str(ch.result->explanation);
        }
        j.raw("}");
    }

    const auto& st = net.stats();
    j.key("stats").raw("{");
    j.key("sent").num(st.sent);
    j.key("delivered").num(st.delivered);
    j.key("dropped").num(st.dropped());
    j.raw("}");
    j.raw("}");
    return j.take();
}

std::string scenario_trace(std::uint64_t seed) {
    Simulation s(seed, 5);
    std::string out;
    auto step = [&](const std::string& name) {
        char buf[96];
        std::snprintf(buf, sizeof buf, " now=%llu hash=%016llx\n", static_cast<unsigned long long>(s.now()),
                      static_cast<unsigned long long>(fnv1a(s.snapshot())));
        out += name + buf;
    };
    s.tick(400);
    step("start");
    const NodeId first = s.leader().value_or(1);
    s.crash(first);
    s.tick(500);
    step("crash " + std::to_string(first));
    s.submit(1, "put", "a", "1");
    s.submit(2, "put", "b", "2");
    s.tick(300);
    step("put");
    s.submit(1, "get", "a", "");
    s.submit(2, "delete", "b", "");
    s.tick(300);
    step("get");
    const NodeId second = s.leader().value_or(2);
    s.partition(std::to_string(second) + "|" + std::to_string(first));  // leader alone; first is down
    s.submit(1, "put", "a", "3");
    s.tick(700);
    step("split " + std::to_string(second));
    s.heal();
    s.restart(first);
    s.tick(700);
    step("heal");
    for (int i = 0; i < 5; ++i) {
        s.tick(37);
        step("tick");
    }
    s.set_load(true);
    s.tick(500);
    step("load");
    s.start_chaos();
    s.tick(Simulation::kChaosTicks + 1000);
    step("chaos");
    s.set_load(false);
    s.tick(600);
    step("quiet");
    out += s.events_json(0);
    out += '\n';
    out += s.snapshot();
    out += '\n';
    return out;
}

}  // namespace quorum::web
