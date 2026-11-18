#pragma once

// Given: SimNetwork carries opaque string payloads, and a real transport later
//        will carry bytes.
// Approach: plain structs for the RPCs in Raft Figure 2, with a strict text
//           codec ("RV 3 1 0 0"). It is readable in traces, and anything
//           malformed decodes to nullopt instead of half-parsed state.

#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "quorum/raft/types.h"

namespace quorum::raft {

struct RequestVote {
    Term term = 0;
    NodeId candidate_id = 0;
    LogIndex last_log_index = 0;
    Term last_log_term = 0;
    bool operator==(const RequestVote&) const = default;
};

struct RequestVoteReply {
    Term term = 0;
    bool vote_granted = false;
    bool operator==(const RequestVoteReply&) const = default;
};

// No entries field yet: every AppendEntries is a heartbeat until step 3.
struct AppendEntries {
    Term term = 0;
    NodeId leader_id = 0;
    LogIndex prev_log_index = 0;
    Term prev_log_term = 0;
    LogIndex leader_commit = 0;
    bool operator==(const AppendEntries&) const = default;
};

struct AppendEntriesReply {
    Term term = 0;
    bool success = false;
    bool operator==(const AppendEntriesReply&) const = default;
};

using RaftMessage = std::variant<RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply>;

Term term_of(const RaftMessage& msg);

std::string encode(const RaftMessage& msg);

// Returns nullopt unless `payload` is exactly one well-formed message.
std::optional<RaftMessage> decode(std::string_view payload);

}  // namespace quorum::raft
