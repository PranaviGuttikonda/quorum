// Given: simulated time must be monotonic and fully controlled by the caller.
// Approach: exercise advance/advance_to, and the rejection of backwards moves.

#include "quorum/sim/clock.h"

#include "harness.h"

using quorum::sim::SimClock;
using quorum::sim::Tick;

TEST(clock_starts_at_zero_and_advances) {
    SimClock clock;
    CHECK_EQ(clock.now(), Tick{0});
    clock.advance(5);
    CHECK_EQ(clock.now(), Tick{5});
    clock.advance_to(12);
    CHECK_EQ(clock.now(), Tick{12});
    clock.advance_to(12);  // same tick is allowed
    CHECK_EQ(clock.now(), Tick{12});
}

TEST(clock_rejects_moving_backwards) {
    SimClock clock;
    clock.advance(10);
    CHECK_THROWS(clock.advance_to(9));
    CHECK_EQ(clock.now(), Tick{10});
}
