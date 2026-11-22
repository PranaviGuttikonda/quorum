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

NodeId KvClient::random_server_except(std::optional<NodeId> avoid) {
    if (servers_.size() == 1 || !avoid) return servers_[rng_.uniform(0, servers_.size() - 1)];
    for (;;) {
        const NodeId s = servers_[rng_.uniform(0, servers_.size() - 1)];
        if (s != *avoid) return s;
    }
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
    target_ = hint_ ? *hint_ : random_server_except(std::nullopt);
    elapsed_ = 0;
    backing_off_ = false;
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
        hint_ = random_server_except(target_);  // the target failed us: try elsewhere
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
        hint_.reset();
        backing_off_ = true;
        elapsed_ = 0;
    }
}

}  // namespace quorum::kv
