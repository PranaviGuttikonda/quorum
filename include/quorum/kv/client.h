#pragma once

// Given: a client can't tell a lost request from a lost reply or a crashed
//        server, and doesn't know who the leader is.
// Approach: one outstanding operation at a time, with a fixed (client, seq)
//           for its whole life.
//   - Send to the leader hint, or a random server if there is none.
//   - A Redirect with a hint: switch to that server and resend at once.
//   - A Redirect without a hint (an election is running): back off briefly,
//     then ask the same server again. It is up and will learn the new leader
//     within a heartbeat or so. If it still knows no leader, try a random
//     server instead (a follower cut off in a minority never learns one, so
//     asking it forever would stall the client).
//   - No reply within retry_timeout: forget the hint and resend to another
//     random server.
//   - Random picks avoid any server that timed out within the last
//     2 x retry_timeout, unless that excludes every server (then any server
//     will do). A crashed server's requests are simply dropped, so without
//     this memory the client can pick it again and wait out another full
//     timeout.
//   The operation completes only when a Result for this exact (client, seq)
//   arrives. Every invoke and completion goes into the History.
//   Each send is also recorded as an Attempt (when, where, how it ended), for
//   display only: nothing the client decides depends on it.

#include <cstdint>
#include <functional>
#include <map>
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

// One send of the current operation, and how it ended. Recorded only: no
// decision of the client reads it.
enum class AttemptOutcome : std::uint8_t {
    InFlight,    // no answer yet, and no timeout yet
    Reply,       // a Result for this operation came back
    Redirect,    // the server said it isn't the leader (hint, if it knew one)
    Timeout,     // no answer within retry_timeout: the client moved on
    Superseded,  // abandoned while in flight: another answer arrived first
};

const char* to_string(AttemptOutcome o);

struct Attempt {
    Tick sent = 0;
    NodeId server = 0;
    AttemptOutcome outcome = AttemptOutcome::InFlight;
    std::optional<NodeId> hint;  // Redirect only
    Tick ended = 0;              // when the outcome was recorded
    // An answer that arrived after this attempt had already timed out or
    // been superseded. The outcome is then that answer.
    bool late = false;
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
    const ClientConfig& config() const { return config_; }
    // Servers avoided in random picks right now (after recent timeouts).
    std::vector<NodeId> avoided_servers() const;
    // The same counters for the current operation, or the last one once it
    // completes. `completed` is 1 once it has.
    const ClientStats& op_stats() const { return op_stats_; }
    // The server whose Result completed the last operation.
    std::optional<NodeId> answered_by() const { return answered_by_; }
    // Every send of the current operation, or the last one once it
    // completes, in order.
    const std::vector<Attempt>& attempts() const { return attempts_; }

private:
    void send_now();
    // A uniformly random server, skipping `avoid` and every server avoided
    // after a recent timeout. If that leaves none, any server. One draw.
    NodeId random_server(std::optional<NodeId> avoid = std::nullopt);
    bool avoided(NodeId server) const;

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
    std::map<NodeId, Tick> avoid_until_;  // server -> end of its timeout exclusion
    bool retry_same_ = false;             // the next send repeats a hint-less redirect's server
    bool repeating_ = false;              // the current send is that repeat
    std::optional<Result> last_result_;
    ClientStats stats_;
    ClientStats op_stats_;
    std::optional<NodeId> answered_by_;
    std::vector<Attempt> attempts_;
    // Records `outcome` on the latest attempt sent to `server`.
    void record(NodeId server, AttemptOutcome outcome, std::optional<NodeId> hint = std::nullopt);
    // Marks an attempt still in flight as superseded.
    void supersede();
};

}  // namespace quorum::kv
