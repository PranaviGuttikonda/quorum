#pragma once

// Given: std::uniform_int_distribution and friends are implementation-defined,
//        so the same seed gives different values on libstdc++ and libc++.
// Approach: xoshiro256** (Blackman & Vigna) seeded via SplitMix64. Every derived
//           draw (range, double, bernoulli) uses only exact integer arithmetic
//           and IEEE-exact conversions, so output depends only on the seed.

#include <array>
#include <cstdint>

namespace quorum::sim {

class Rng {
public:
    explicit Rng(std::uint64_t seed);

    // Next raw 64-bit value.
    std::uint64_t next_u64();

    // Uniform integer in [lo, hi], inclusive. Throws if lo > hi.
    // Approach: rejection sampling to remove modulo bias.
    std::uint64_t uniform(std::uint64_t lo, std::uint64_t hi);

    // Uniform double in [0, 1) with 53 random bits.
    double next_double();

    // True with probability p. p <= 0 is always false; p >= 1 is always true.
    bool bernoulli(double p);

    // Given: components (nodes, network) each need their own stream, so that
    //        adding draws in one doesn't shift another.
    // Approach: seed a child generator from this generator's next value.
    Rng fork();

private:
    std::array<std::uint64_t, 4> s_;
};

}  // namespace quorum::sim
