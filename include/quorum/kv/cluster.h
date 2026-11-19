#pragma once

// Given: KV tests need servers, clients and a shared operation history, all
//        driven by one seed.
// Approach: a RaftCluster whose services are KvServices, plus simulated
//           clients on global network nodes 1000, 1001, ... (client ids 1, 2,
//           ...). Each client has its own Rng forked from the seed. One tick
//           ticks the clients (timeouts and resends), then the Raft cluster.
//           Faults are injected through raft().

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "quorum/kv/client.h"
#include "quorum/kv/history.h"
#include "quorum/kv/service.h"
#include "quorum/raft/cluster.h"

namespace quorum::kv {

struct KvClusterConfig {
    std::size_t servers = 5;
    std::size_t clients = 3;
    raft::RaftConfig raft{};
    sim::NetworkConfig net{};
    ClientConfig client{};
    // Null means MemoryStorage.
    std::shared_ptr<raft::StorageProvider> storage;
};

class KvCluster {
public:
    static constexpr NodeId kFirstClientNode = 1000;

    KvCluster(std::uint64_t seed, KvClusterConfig config = {});
    KvCluster(const KvCluster&) = delete;
    KvCluster& operator=(const KvCluster&) = delete;

    void tick();
    void run_for(Tick ticks);
    bool run_until(const std::function<bool(const KvCluster&)>& pred, Tick max_ticks);

    // Submits one operation on client `i` and runs until it completes.
    // Returns its result, or nullopt if it didn't complete within max_ticks.
    std::optional<Result> execute(std::size_t i, Op op, std::string key, std::string value = {},
                                  std::optional<std::string> expected = std::nullopt,
                                  Tick max_ticks = 5000);

    raft::RaftCluster& raft() { return raft_; }
    const raft::RaftCluster& raft() const { return raft_; }
    KvClient& client(std::size_t i) { return *clients_.at(i); }
    const KvClient& client(std::size_t i) const { return *clients_.at(i); }
    std::size_t client_count() const { return clients_.size(); }
    bool clients_idle() const;
    const History& history() const { return history_; }
    // Null while the server is crashed.
    const KvService* service(NodeId id) const;
    Tick now() const { return raft_.now(); }

private:
    raft::RaftCluster raft_;
    History history_;
    std::vector<std::unique_ptr<KvClient>> clients_;
};

}  // namespace quorum::kv
