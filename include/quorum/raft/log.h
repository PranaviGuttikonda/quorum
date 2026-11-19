#pragma once

#include <string>

#include "quorum/raft/types.h"

namespace quorum::raft {

// Indices are 1-based, as in the paper. Index 0 is the empty prefix with term 0.
struct LogEntry {
    Term term = 0;
    std::string command;
    bool operator==(const LogEntry&) const = default;
};

}  // namespace quorum::raft
