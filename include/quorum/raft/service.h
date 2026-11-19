#pragma once

// Given: an application (such as the KV store) needs to run beside each Raft
//        node: consume committed entries, talk to clients, and propose
//        commands.
// Approach: a per-incarnation Service object, created when its node boots and
//           destroyed when the node crashes, so it holds only volatile state.
//           Anything durable must be rebuilt from the log through on_apply.

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "quorum/raft/log.h"
#include "quorum/raft/types.h"

namespace quorum::raft {

class RaftNode;

class Service {
public:
    virtual ~Service() = default;
    // Called for every committed entry, in index order, no-ops included.
    virtual void on_apply(LogIndex index, const LogEntry& entry) = 0;
    // A message from a non-server node (a client).
    virtual void on_client_message(NodeId from, std::string_view payload) = 0;
};

using ServiceSend = std::function<void(NodeId to, std::string payload)>;
using ServiceFactory =
    std::function<std::unique_ptr<Service>(NodeId self, RaftNode& node, ServiceSend send)>;

}  // namespace quorum::raft
