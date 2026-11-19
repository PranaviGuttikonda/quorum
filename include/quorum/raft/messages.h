#pragma once

// Given: SimNetwork carries opaque string payloads, and a real transport later
//        will carry bytes.
// Approach: plain structs for the RPCs in Raft Figure 2, with a strict text
//           codec. Numeric fields are separated by single spaces. A command
//           is written as its byte length, a space and then the raw bytes, so
//           a command can contain anything (spaces, newlines, NUL). Anything
//           malformed decodes to nullopt instead of half-parsed state.
//
//   RV  <term> <candidate> <lastLogIndex> <lastLogTerm>
//   RVR <term> <granted:0|1>
//   AE  <term> <leader> <prevLogIndex> <prevLogTerm> <leaderCommit> <n>
//       followed by n times: " <term> <kind:0|1> <len> <bytes>"
//   AER <term> <success:0|1> <matchIndex> <lastLogIndex>

#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "quorum/raft/log.h"
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

struct AppendEntries {
    Term term = 0;
    NodeId leader_id = 0;
    LogIndex prev_log_index = 0;
    Term prev_log_term = 0;
    LogIndex leader_commit = 0;
    std::vector<LogEntry> entries;  // empty for a heartbeat
    bool operator==(const AppendEntries&) const = default;
};

// Given: the paper's reply is just {term, success}, and the leader infers
//        what it refers to. With duplicated and reordered messages, the
//        leader can't know which request a reply answers.
// Approach: the reply states its own facts. On success, match_index is the
//           last index this request proved identical to the leader's log
//           (prevLogIndex + entries). last_log_index is the follower's log
//           length; on failure the leader uses it to skip back past a short
//           log in one round trip.
struct AppendEntriesReply {
    Term term = 0;
    bool success = false;
    LogIndex match_index = 0;
    LogIndex last_log_index = 0;
    bool operator==(const AppendEntriesReply&) const = default;
};

using RaftMessage = std::variant<RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply>;

Term term_of(const RaftMessage& msg);

std::string encode(const RaftMessage& msg);

// Returns nullopt unless `payload` is exactly one well-formed message.
std::optional<RaftMessage> decode(std::string_view payload);

}  // namespace quorum::raft
