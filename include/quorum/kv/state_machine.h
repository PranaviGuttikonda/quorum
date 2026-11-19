#pragma once

// Given: every replica applies the same committed log, and clients retry
//        requests, so the same (client, seq) can be committed more than once.
// Approach: a deterministic state machine. Besides the key-value map, it
//           keeps a session per client: the last seq executed and its result.
//   - A command with a newer seq executes and replaces the session.
//   - The same seq again is a duplicate: it doesn't execute, and the cached
//     result is returned.
//   - An older seq is stale (the client has already moved on): it is ignored.
//   Sessions are ordinary state-machine state, derived only from the log, so
//   a restarted replica rebuilds exactly the same table by replaying it.
//   Sessions never expire (see the PR).

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "quorum/kv/command.h"
#include "quorum/raft/log.h"

namespace quorum::kv {

using raft::LogIndex;

struct Session {
    Seq seq = 0;
    Result result;
    bool operator==(const Session&) const = default;
};

// What applying one entry produced, for the server to reply with.
struct Applied {
    ClientId client = 0;
    Seq seq = 0;
    Result result;
    bool duplicate = false;
};

// One command that actually executed, i.e. was not deduplicated.
struct Execution {
    LogIndex index = 0;
    Command command;
    Result result;
    bool operator==(const Execution&) const = default;
};

struct StateMachineStats {
    std::uint64_t executed = 0;
    std::uint64_t duplicates = 0;
    std::uint64_t stale = 0;
    std::uint64_t noops = 0;
    std::uint64_t malformed = 0;
};

class KvStateMachine {
public:
    // Applies the entry at `index`, which must be last_applied() + 1 (it
    // throws otherwise). Returns nullopt for no-ops, malformed commands and
    // stale requests.
    std::optional<Applied> apply(LogIndex index, const raft::LogEntry& entry);

    // The cached result if `seq` is the client's last executed seq.
    std::optional<Result> cached(ClientId client, Seq seq) const;
    // True if the client has already executed something newer than `seq`.
    bool is_stale(ClientId client, Seq seq) const;

    const std::map<std::string, std::string>& data() const { return data_; }
    const std::map<ClientId, Session>& sessions() const { return sessions_; }
    const std::vector<Execution>& executions() const { return executions_; }
    LogIndex last_applied() const { return last_applied_; }
    const StateMachineStats& stats() const { return stats_; }

private:
    Result execute(const Command& c);

    std::map<std::string, std::string> data_;
    std::map<ClientId, Session> sessions_;
    std::vector<Execution> executions_;
    LogIndex last_applied_ = 0;
    StateMachineStats stats_;
};

}  // namespace quorum::kv
