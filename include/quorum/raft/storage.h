#pragma once

// Given: Raft requires currentTerm, votedFor and the log to be on stable
//        storage before a node answers an RPC (Figure 2, "Persistent state on
//        all servers").
// Approach: load() returns everything for recovery. Writes are incremental:
//           term and vote together, and the log as "keep the first N entries,
//           then append these", which covers both append and conflict
//           truncation in one call. MemoryStorage outlives the RaftNode (the
//           cluster owns it), so a simulated crash keeps its state. A
//           disk-backed version comes in step 6.

#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>

#include "quorum/raft/log.h"
#include "quorum/raft/types.h"

namespace quorum::raft {

struct PersistentState {
    Term current_term = 0;
    std::optional<NodeId> voted_for;
    std::vector<LogEntry> log;  // log[i] has index i + 1
    bool operator==(const PersistentState&) const = default;
};

class Storage {
public:
    virtual ~Storage() = default;
    virtual PersistentState load() const = 0;
    // Each write must be durable when it returns.
    virtual void save_term_and_vote(Term term, std::optional<NodeId> voted_for) = 0;
    // Keeps entries 1..keep and appends `entries` after them. Throws if keep
    // is beyond the end of the log.
    virtual void write_log(LogIndex keep, std::span<const LogEntry> entries) = 0;
};

class MemoryStorage final : public Storage {
public:
    PersistentState load() const override { return state_; }

    void save_term_and_vote(Term term, std::optional<NodeId> voted_for) override {
        state_.current_term = term;
        state_.voted_for = voted_for;
        ++hard_state_writes_;
    }

    void write_log(LogIndex keep, std::span<const LogEntry> entries) override {
        if (keep > state_.log.size()) throw std::out_of_range("MemoryStorage::write_log: keep > size");
        if (keep < state_.log.size()) {
            truncated_entries_ += state_.log.size() - keep;
            state_.log.resize(keep);
        }
        state_.log.insert(state_.log.end(), entries.begin(), entries.end());
        ++log_writes_;
    }

    std::uint64_t hard_state_writes() const { return hard_state_writes_; }
    std::uint64_t log_writes() const { return log_writes_; }
    // Total entries ever removed by conflict truncation.
    std::uint64_t truncated_entries() const { return truncated_entries_; }

private:
    PersistentState state_;
    std::uint64_t hard_state_writes_ = 0;
    std::uint64_t log_writes_ = 0;
    std::uint64_t truncated_entries_ = 0;
};

}  // namespace quorum::raft
