#pragma once

// Given: protocol timing (timeouts, delays) must be reproducible, so wall-clock
//        time is never used.
// Approach: time is a monotonically increasing tick counter that only the
//           simulator advances.

#include <cstdint>
#include <stdexcept>

namespace quorum::sim {

using Tick = std::uint64_t;

class SimClock {
public:
    Tick now() const { return now_; }

    void advance(Tick delta) { now_ += delta; }

    // Throws if t is in the past: simulated time never runs backwards.
    void advance_to(Tick t) {
        if (t < now_) throw std::invalid_argument("SimClock::advance_to: time moved backwards");
        now_ = t;
    }

private:
    Tick now_ = 0;
};

}  // namespace quorum::sim
