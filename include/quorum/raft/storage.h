#pragma once

// Given: Raft requires currentTerm and votedFor to be on stable storage before
//        a node answers an RPC (Figure 2, "Persistent state on all servers").
// Approach: a minimal load/save interface. MemoryStorage outlives the RaftNode
//           (the cluster owns it), so a simulated crash destroys the node but
//           keeps its persisted state. A disk-backed version comes in step 6.

#include <cstdint>
#include <optional>

#include "quorum/raft/types.h"

namespace quorum::raft {

struct PersistentState {
    Term current_term = 0;
    std::optional<NodeId> voted_for;
    bool operator==(const PersistentState&) const = default;
};

class Storage {
public:
    virtual ~Storage() = default;
    virtual PersistentState load() const = 0;
    // Must be durable when it returns.
    virtual void save(const PersistentState& state) = 0;
};

class MemoryStorage final : public Storage {
public:
    PersistentState load() const override { return state_; }
    void save(const PersistentState& state) override {
        state_ = state;
        ++saves_;
    }
    std::uint64_t saves() const { return saves_; }

private:
    PersistentState state_;
    std::uint64_t saves_ = 0;
};

}  // namespace quorum::raft
