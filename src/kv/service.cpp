#include "quorum/kv/service.h"

#include <utility>

namespace quorum::kv {

KvService::KvService(NodeId self, raft::RaftNode& node, raft::ServiceSend send)
    : self_(self), node_(node), send_(std::move(send)) {}

raft::ServiceFactory KvService::factory() {
    return [](NodeId self, raft::RaftNode& node, raft::ServiceSend send) -> std::unique_ptr<raft::Service> {
        return std::make_unique<KvService>(self, node, std::move(send));
    };
}

void KvService::reply(NodeId to, const Reply& r) {
    send_(to, encode(r));
}

void KvService::on_client_message(NodeId from, std::string_view payload) {
    const auto cmd = decode_request(payload);
    if (!cmd) return;
    if (const auto cached = sm_.cached(cmd->client, cmd->seq)) {
        ++stats_.replied_cached;
        reply(from, Reply{cmd->client, cmd->seq, *cached});
        return;
    }
    if (sm_.is_stale(cmd->client, cmd->seq)) return;

    const auto r = node_.propose(encode(*cmd));
    if (const auto* p = std::get_if<raft::Proposed>(&r)) {
        // A retry to the same leader proposes again. The state machine
        // deduplicates whichever copy commits second.
        ++stats_.proposed;
        pending_[p->index] = Pending{from, cmd->client, cmd->seq};
        return;
    }
    ++stats_.redirected;
    reply(from, Reply{cmd->client, cmd->seq, Redirect{std::get<raft::NotLeader>(r).leader_hint}});
}

void KvService::on_apply(LogIndex index, const raft::LogEntry& entry) {
    const auto applied = sm_.apply(index, entry);
    const auto it = pending_.find(index);
    if (it == pending_.end()) return;
    const Pending p = it->second;
    pending_.erase(it);
    if (applied && applied->client == p.client && applied->seq == p.seq) {
        ++stats_.replied;
        reply(p.client_node, Reply{p.client, p.seq, applied->result});
    } else {
        ++stats_.abandoned;
    }
}

}  // namespace quorum::kv
