#pragma once

// Given: a client can't tell a lost request from a lost reply or a crashed
//        server, and doesn't know who the leader is.
// Approach: one outstanding operation at a time, with a fixed (client, seq)
//           for its whole life.
//   - Send to the leader hint, or a random server if there is none.
//   - A Redirect with a hint: switch to that server and resend at once.
//   - A Redirect without a hint (an election is running): back off briefly,
//     then try a random server.
//   - No reply within retry_timeout: forget the hint and resend to another
//     random server.
//   The operation completes only when a Result for this exact (client, seq)
//   arrives. Every invoke and completion goes into the History.

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "quorum/kv/command.h"
#include "quorum/kv/history.h"
#include "quorum/sim/network.h"
#include "quorum/sim/rng.h"

namespace quorum::kv {

struct ClientConfig {
    Tick retry_timeout = 100;
    Tick redirect_backoff = 10;
};

struct ClientStats {
    std::uint64_t sends = 0;
    std::uint64_t timeouts = 0;
    std::uint64_t redirects = 0;
    std::uint64_t completed = 0;
};

class KvClient {
public:
    using SendFn = std::function<void(NodeId to, std::string payload)>;
    using NowFn = std::function<Tick()>;

    KvClient(NodeId node_id, ClientId client_id, std::vector<NodeId> servers, ClientConfig config,
             sim::Rng rng, History& history, SendFn send, NowFn now);

    // Starts an operation. The client fills in client id and seq. Throws if
    // not idle. Returns the operation's index in the History.
    std::size_t submit(Op op, std::string key, std::string value = {},
                       std::optional<std::string> expected = std::nullopt);

    void tick();
    void on_message(const sim::Message& m);

    bool idle() const { return !op_.has_value(); }
    NodeId node_id() const { return node_id_; }
    ClientId client_id() const { return client_id_; }
    std::optional<NodeId> leader_hint() const { return hint_; }
    void set_leader_hint(std::optional<NodeId> hint) { hint_ = hint; }
    // Result of the most recently completed operation.
    const std::optional<Result>& last_result() const { return last_result_; }
    const ClientStats& stats() const { return stats_; }

private:
    void send_now();
    NodeId random_server_except(std::optional<NodeId> avoid);

    const NodeId node_id_;
    const ClientId client_id_;
    const std::vector<NodeId> servers_;
    const ClientConfig config_;
    sim::Rng rng_;
    History& history_;
    const SendFn send_;
    const NowFn now_;

    Seq seq_ = 0;
    std::optional<Command> op_;
    std::size_t op_index_ = 0;
    std::optional<NodeId> hint_;
    NodeId target_ = 0;
    Tick elapsed_ = 0;
    bool backing_off_ = false;
    std::optional<Result> last_result_;
    ClientStats stats_;
};

}  // namespace quorum::kv
