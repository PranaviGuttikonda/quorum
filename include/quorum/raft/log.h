#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "quorum/raft/types.h"

namespace quorum::raft {

enum class EntryKind : std::uint8_t {
    Command = 0,  // a client command, handed to the state machine
    NoOp = 1,     // appended by a new leader (§8); the state machine skips it
};

// Indices are 1-based, as in the paper. Index 0 is the empty prefix with term 0.
struct LogEntry {
    LogEntry() = default;
    LogEntry(Term t, std::string cmd, EntryKind k = EntryKind::Command)
        : term(t), command(std::move(cmd)), kind(k) {}
    static LogEntry noop(Term t) { return LogEntry(t, {}, EntryKind::NoOp); }
    bool is_noop() const { return kind == EntryKind::NoOp; }

    Term term = 0;
    std::string command;
    EntryKind kind = EntryKind::Command;
    bool operator==(const LogEntry&) const = default;
};

}  // namespace quorum::raft
