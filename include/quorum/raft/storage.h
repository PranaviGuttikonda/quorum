#pragma once

// Given: Raft requires currentTerm, votedFor and the log to be on stable
//        storage before a node answers an RPC (Figure 2, "Persistent state on
//        all servers").
// Approach: load() returns everything for recovery. Writes are incremental:
//           term and vote together, and the log as "keep the first N entries,
//           then append these", which covers both append and conflict
//           truncation in one call.
//   - Durability: save_term_and_vote is durable when it returns. write_log
//     may be buffered, and is durable once sync() returns. The host calls
//     sync() before anything the node or its service produced leaves the
//     server (RaftCluster does it on every send), so all the log writes of
//     one step share one fsync.
//   - MemoryStorage outlives the RaftNode (the cluster's provider owns it),
//     so a simulated crash keeps all of it. FileStorage (file_storage.h) is
//     the disk-backed version.

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
    // Durable when it returns.
    virtual void save_term_and_vote(Term term, std::optional<NodeId> voted_for) = 0;
    // Keeps entries 1..keep and appends `entries` after them. Throws if keep
    // is beyond the end of the log. Durable after the next sync().
    virtual void write_log(LogIndex keep, std::span<const LogEntry> entries) = 0;
    // Makes every earlier write durable. Cheap when there is nothing to sync.
    virtual void sync() = 0;
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

    void sync() override {}

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
