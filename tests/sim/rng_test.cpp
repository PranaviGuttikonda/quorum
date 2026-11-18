// Given: reproducibility across platforms depends on the RNG producing the same
//        stream everywhere.
// Approach: golden values pin the exact output, so a divergence on any CI
//           platform fails here. Statistical checks use fixed seeds and loose
//           bounds, so they are deterministic and not flaky.

#include "quorum/sim/rng.h"

#include <array>
#include <cstdint>
#include <limits>
#include <vector>

#include "harness.h"

using quorum::sim::Rng;

TEST(rng_golden_u64_sequence) {
    Rng rng(42);
    const std::array<std::uint64_t, 5> expected = {
        0x15780b2e0c2ec716ULL, 0x6104d9866d113a7eULL, 0xae17533239e499a1ULL,
        0xecb8ad4703b360a1ULL, 0xfde6dc7fe2ec5e64ULL,
    };
    for (std::uint64_t want : expected) CHECK_EQ(rng.next_u64(), want);
}

TEST(rng_golden_uniform_and_double) {
    Rng dice(7);
    const std::array<std::uint64_t, 10> expected = {1, 3, 1, 5, 3, 6, 5, 5, 5, 2};
    for (std::uint64_t want : expected) CHECK_EQ(dice.uniform(1, 6), want);

    Rng d(99);
    CHECK_EQ(d.next_double(), 0x1.65129fad5723ap-2);
}

TEST(rng_same_seed_same_stream_different_seed_differs) {
    Rng a(123), b(123), c(124);
    bool any_diff = false;
    for (int i = 0; i < 1000; ++i) {
        const std::uint64_t va = a.next_u64();
        CHECK_EQ(va, b.next_u64());
        if (va != c.next_u64()) any_diff = true;
    }
    CHECK(any_diff);
}

TEST(rng_seed_zero_is_valid) {
    Rng rng(0);
    bool nonzero = false;
    for (int i = 0; i < 8; ++i) nonzero |= rng.next_u64() != 0;
    CHECK(nonzero);
}

TEST(rng_uniform_bounds_and_coverage) {
    Rng rng(1);
    std::vector<int> counts(6, 0);
    const int draws = 60000;
    for (int i = 0; i < draws; ++i) {
        const std::uint64_t v = rng.uniform(10, 15);
        CHECK(v >= 10 && v <= 15);
        ++counts[v - 10];
    }
    // Expect ~10000 per face; +/-5% is far outside sampling noise.
    for (int c : counts) CHECK(c > 9500 && c < 10500);
}

TEST(rng_uniform_edge_ranges) {
    Rng rng(5);
    CHECK_EQ(rng.uniform(9, 9), std::uint64_t{9});
    const std::uint64_t max = std::numeric_limits<std::uint64_t>::max();
    (void)rng.uniform(0, max);  // full range must not divide by zero
    CHECK_EQ(rng.uniform(max, max), max);
    CHECK_THROWS(rng.uniform(2, 1));
}

TEST(rng_double_in_unit_interval) {
    Rng rng(2);
    double sum = 0.0;
    for (int i = 0; i < 100000; ++i) {
        const double v = rng.next_double();
        CHECK(v >= 0.0 && v < 1.0);
        sum += v;
    }
    const double mean = sum / 100000.0;
    CHECK(mean > 0.49 && mean < 0.51);
}

TEST(rng_bernoulli_rate) {
    Rng rng(3);
    CHECK(!rng.bernoulli(0.0));
    CHECK(rng.bernoulli(1.0));
    CHECK(!rng.bernoulli(-0.5));
    CHECK(rng.bernoulli(1.5));

    int hits = 0;
    const int n = 100000;
    for (int i = 0; i < n; ++i) hits += rng.bernoulli(0.3) ? 1 : 0;
    const double rate = static_cast<double>(hits) / n;
    CHECK(rate > 0.29 && rate < 0.31);
}

TEST(rng_fork_is_deterministic_and_independent) {
    Rng p1(77), p2(77);
    Rng c1 = p1.fork();
    Rng c2 = p2.fork();
    for (int i = 0; i < 100; ++i) CHECK_EQ(c1.next_u64(), c2.next_u64());

    // Drawing from the child must not shift the parent's stream.
    Rng p3(77);
    (void)p3.fork();
    for (int i = 0; i < 100; ++i) CHECK_EQ(p1.next_u64(), p3.next_u64());
}
