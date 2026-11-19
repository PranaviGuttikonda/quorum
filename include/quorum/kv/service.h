#pragma once

// Given: a KV server runs beside each Raft node, and clients may contact any
//        server, retry anywhere, and get redirected.
// Approach: on a client request:
//   1. If the state machine already executed this (client, seq), reply with
//      the cached result. This is exactly-once, even across leader changes.
//   2. If it is stale (the client has moved on), drop it.
//   3. If this node isn't leader, reply with a Redirect carrying its hint.
//   4. Otherwise propose the command and remember (index -> client). When
//      that index is applied, reply with the state machine's result if the
//      entry there is still this (client, seq). If another entry replaced it,
//      just forget the request: the client times out and retries.
// Reads go through the log like writes, so a reply always reflects a
// committed position in the log (see the PR on ReadIndex).

#include <cstdint>
#include <map>
#include <memory>
#include <string_view>

#include "quorum/kv/state_machine.h"
#include "quorum/raft/node.h"
#include "quorum/raft/service.h"

namespace quorum::kv {

struct ServiceStats {
    std::uint64_t proposed = 0;
    std::uint64_t replied = 0;         // after applying the request's entry
    std::uint64_t replied_cached = 0;  // straight from the session table
    std::uint64_t redirected = 0;
    std::uint64_t abandoned = 0;       // another entry took the proposed index
};

class KvService final : public raft::Service {
public:
    KvService(NodeId self, raft::RaftNode& node, raft::ServiceSend send);

    void on_apply(LogIndex index, const raft::LogEntry& entry) override;
    void on_client_message(NodeId from, std::string_view payload) override;

    NodeId id() const { return self_; }
    const KvStateMachine& state() const { return sm_; }
    const ServiceStats& stats() const { return stats_; }

    static raft::ServiceFactory factory();

private:
    struct Pending {
        NodeId client_node = 0;
        ClientId client = 0;
        Seq seq = 0;
    };
    void reply(NodeId to, const Reply& r);

    NodeId self_;
    raft::RaftNode& node_;
    raft::ServiceSend send_;
    KvStateMachine sm_;
    std::map<LogIndex, Pending> pending_;
    ServiceStats stats_;
};

}  // namespace quorum::kv
