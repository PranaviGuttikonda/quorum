#include "quorum/kv/client.h"

#include <stdexcept>
#include <utility>

namespace quorum::kv {

KvClient::KvClient(NodeId node_id, ClientId client_id, std::vector<NodeId> servers,
                   ClientConfig config, sim::Rng rng, History& history, SendFn send, NowFn now)
    : node_id_(node_id),
      client_id_(client_id),
      servers_(std::move(servers)),
      config_(config),
      rng_(rng),
      history_(history),
      send_(std::move(send)),
      now_(std::move(now)) {
    if (servers_.empty()) throw std::invalid_argument("KvClient: no servers");
}

std::size_t KvClient::submit(Op op, std::string key, std::string value,
                             std::optional<std::string> expected) {
    if (!idle()) throw std::logic_error("KvClient::submit: an operation is outstanding");
    op_ = Command{client_id_, ++seq_, op, std::move(key), std::move(value), std::move(expected)};
    op_index_ = history_.invoke(*op_, now_());
    op_stats_ = {};
    attempts_.clear();
    send_now();
    return op_index_;
}

bool KvClient::avoided(NodeId server) const {
    const auto it = avoid_until_.find(server);
    return it != avoid_until_.end() && it->second > now_();
}

std::vector<NodeId> KvClient::avoided_servers() const {
    std::vector<NodeId> out;
    for (NodeId s : servers_)
        if (avoided(s)) out.push_back(s);
    return out;
}

// Given: some servers recently timed out (likely crashed or cut off).
// Approach: narrow the candidates first, then make exactly one draw, so the
//           choice stays a pure function of the seed and what the client saw.
NodeId KvClient::random_server(std::optional<NodeId> avoid) {
    std::vector<NodeId> pool;
    for (NodeId s : servers_)
        if (s != avoid && !avoided(s)) pool.push_back(s);
    if (pool.empty()) pool = servers_;  // everyone is excluded: any server will do
    return pool[rng_.uniform(0, pool.size() - 1)];
}

const char* to_string(AttemptOutcome o) {
    switch (o) {
        case AttemptOutcome::InFlight: return "in-flight";
        case AttemptOutcome::Reply: return "reply";
        case AttemptOutcome::Redirect: return "redirect";
        case AttemptOutcome::Timeout: return "timeout";
        case AttemptOutcome::Superseded: return "superseded";
    }
    return "?";
}

void KvClient::record(NodeId server, AttemptOutcome outcome, std::optional<NodeId> hint) {
    for (auto it = attempts_.rbegin(); it != attempts_.rend(); ++it) {
        if (it->server != server) continue;
        it->late = it->outcome != AttemptOutcome::InFlight;
        it->outcome = outcome;
        it->hint = hint;
        it->ended = now_();
        return;
    }
}

void KvClient::supersede() {
    for (Attempt& a : attempts_) {
        if (a.outcome != AttemptOutcome::InFlight) continue;
        a.outcome = AttemptOutcome::Superseded;
        a.ended = now_();
    }
}

void KvClient::send_now() {
    target_ = hint_ ? *hint_ : random_server();
    elapsed_ = 0;
    backing_off_ = false;
    repeating_ = retry_same_;
    retry_same_ = false;
    supersede();
    attempts_.push_back(Attempt{now_(), target_, AttemptOutcome::InFlight, std::nullopt, 0, false});
    ++stats_.sends;
    ++op_stats_.sends;
    send_(target_, encode_request(*op_));
}

void KvClient::tick() {
    if (!op_) return;
    ++elapsed_;
    if (backing_off_) {
        if (elapsed_ >= config_.redirect_backoff) send_now();
        return;
    }
    if (elapsed_ >= config_.retry_timeout) {
        ++stats_.timeouts;
        ++op_stats_.timeouts;
        record(target_, AttemptOutcome::Timeout);
        // The target failed us: avoid it for a while, and try elsewhere.
        avoid_until_[target_] = now_() + 2 * config_.retry_timeout;
        hint_ = random_server(target_);
        send_now();
    }
}

void KvClient::on_message(const sim::Message& m) {
    const auto rep = decode_reply(m.payload);
    if (!rep || !op_ || rep->client != client_id_ || rep->seq != op_->seq) return;  // stale or garbage
    if (const auto* res = std::get_if<Result>(&rep->body)) {
        record(m.from, AttemptOutcome::Reply);
        supersede();
        history_.complete(op_index_, now_(), *res);
        last_result_ = *res;
        op_.reset();
        // Whoever answered is a good guess for the next request.
        hint_ = m.from;
        answered_by_ = m.from;
        ++stats_.completed;
        ++op_stats_.completed;
        return;
    }
    ++stats_.redirects;
    ++op_stats_.redirects;
    const auto& hint = std::get<Redirect>(rep->body).leader_hint;
    record(m.from, AttemptOutcome::Redirect, hint);
    if (hint && *hint != target_) {
        hint_ = hint;
        send_now();
    } else {
        // No leader known yet. Ask the same server again after the backoff,
        // unless that was already a repeat: then try someone else.
        if (repeating_ && m.from == target_) {
            hint_ = random_server(m.from);
        } else {
            hint_ = m.from;
            retry_same_ = true;
        }
        backing_off_ = true;
        elapsed_ = 0;
    }
}

}  // namespace quorum::kv
