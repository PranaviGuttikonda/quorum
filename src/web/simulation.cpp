#include "quorum/web/simulation.h"

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
    : seed_(seed), kv_(seed, web_config(servers)), queued_(kClients), running_(kClients) {}

void Simulation::tick(std::uint32_t n) {
    for (std::uint32_t i = 0; i < n; ++i) {
        start_queued();
        kv_.tick();
        collect();
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

void Simulation::collect() {
    for (std::size_t c = 0; c < kClients; ++c) {
        if (!running_[c]) continue;
        WebOp& op = ops_[*running_[c]];
        op.stats = kv_.client(c).op_stats();
        if (kv_.client(c).idle()) {
            op.server = kv_.client(c).answered_by();
            running_[c].reset();
        }
    }
    for (const sim::TraceEvent& e : kv_.raft().network().trace())
        if (e.kind == sim::EventKind::Drop) drops_.push_back(Drop{e.tick, e.from, e.to, e.reason});
    kv_.raft().clear_trace();
    while (!drops_.empty() && drops_.front().at + kDropMemory < now()) drops_.pop_front();
}

bool Simulation::crash(NodeId id) {
    auto& c = kv_.raft();
    if (!c.is_server(id) || !c.is_up(id)) return false;
    c.crash(id);
    return true;
}

bool Simulation::restart(NodeId id) {
    auto& c = kv_.raft();
    if (!c.is_server(id) || c.is_up(id)) return false;
    c.restart(id);
    return true;
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
    return true;
}

void Simulation::heal() {
    kv_.raft().heal();
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
    w.id = static_cast<std::int64_t>(ops_.size()) + 1;
    w.client = client;
    w.key = key;
    if (w.op == kv::Op::Put) w.value = value;
    ops_.push_back(std::move(w));
    queued_[client - 1].push_back(ops_.size() - 1);
    start_queued();
    return ops_.back().id;
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
    const std::size_t from = ops_.size() > kOpTail ? ops_.size() - kOpTail : 0;
    for (std::size_t i = from; i < ops_.size(); ++i) {
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
    out += s.snapshot();
    out += '\n';
    return out;
}

}  // namespace quorum::web
