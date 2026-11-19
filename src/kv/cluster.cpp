#include "quorum/kv/cluster.h"

#include <utility>

namespace quorum::kv {

KvCluster::KvCluster(std::uint64_t seed, KvClusterConfig config)
    : raft_(seed, config.servers, config.raft, config.net, KvService::factory(), config.storage) {
    // A separate stream for clients, so their choices don't depend on
    // how many draws the cluster made.
    sim::Rng client_root(seed ^ 0xc11e47c11e47c11eULL);
    for (std::size_t i = 0; i < config.clients; ++i) {
        const NodeId node = kFirstClientNode + static_cast<NodeId>(i);
        auto client = std::make_unique<KvClient>(
            node, ClientId{i + 1}, raft_.ids(), config.client, client_root.fork(), history_,
            [this, node](NodeId to, std::string payload) { raft_.client_send(node, to, std::move(payload)); },
            [this] { return raft_.now(); });
        KvClient* raw = client.get();
        raft_.add_client(node, [raw](const sim::Message& m) { raw->on_message(m); });
        clients_.push_back(std::move(client));
    }
}

void KvCluster::tick() {
    for (auto& c : clients_) c->tick();
    raft_.tick();
}

void KvCluster::run_for(Tick ticks) {
    for (Tick i = 0; i < ticks; ++i) tick();
}

bool KvCluster::run_until(const std::function<bool(const KvCluster&)>& pred, Tick max_ticks) {
    for (Tick i = 0; i < max_ticks; ++i) {
        if (pred(*this)) return true;
        tick();
    }
    return pred(*this);
}

std::optional<Result> KvCluster::execute(std::size_t i, Op op, std::string key, std::string value,
                                         std::optional<std::string> expected, Tick max_ticks) {
    const std::size_t idx = client(i).submit(op, std::move(key), std::move(value), std::move(expected));
    run_until([i](const KvCluster& k) { return k.client(i).idle(); }, max_ticks);
    return history_.ops().at(idx).result;
}

bool KvCluster::clients_idle() const {
    for (const auto& c : clients_)
        if (!c->idle()) return false;
    return true;
}

const KvService* KvCluster::service(NodeId id) const {
    return dynamic_cast<const KvService*>(raft_.service(id));
}

}  // namespace quorum::kv
