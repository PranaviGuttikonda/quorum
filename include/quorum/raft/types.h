#pragma once

#include <cstdint>

#include "quorum/sim/network.h"

namespace quorum::raft {

using sim::NodeId;
using sim::Tick;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

enum class Role : std::uint8_t { Follower, Candidate, Leader };

inline const char* to_string(Role role) {
    switch (role) {
        case Role::Follower: return "follower";
        case Role::Candidate: return "candidate";
        case Role::Leader: return "leader";
    }
    return "?";
}

}  // namespace quorum::raft
