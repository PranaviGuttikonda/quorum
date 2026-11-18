#include "quorum/sim/rng.h"

#include <limits>
#include <stdexcept>

namespace quorum::sim {

namespace {

std::uint64_t splitmix64(std::uint64_t& state) {
    std::uint64_t z = (state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

constexpr std::uint64_t rotl(std::uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

}  // namespace

// Given: xoshiro must not start from the all-zero state.
// Approach: SplitMix64 expands any 64-bit seed, including 0, into four
//           well-mixed words. It can't produce all zeros from consecutive outputs.
Rng::Rng(std::uint64_t seed) {
    std::uint64_t sm = seed;
    for (auto& word : s_) word = splitmix64(sm);
}

std::uint64_t Rng::next_u64() {
    const std::uint64_t result = rotl(s_[1] * 5, 7) * 9;
    const std::uint64_t t = s_[1] << 17;
    s_[2] ^= s_[0];
    s_[3] ^= s_[1];
    s_[1] ^= s_[2];
    s_[0] ^= s_[3];
    s_[2] ^= t;
    s_[3] = rotl(s_[3], 45);
    return result;
}

// Given: r % n is biased unless n divides 2^64.
// Approach: reject draws below (2^64 mod n). The accepted region is then an
//           exact multiple of n. 0 - n wraps modulo 2^64, so (0 - n) % n equals 2^64 mod n.
std::uint64_t Rng::uniform(std::uint64_t lo, std::uint64_t hi) {
    if (lo > hi) throw std::invalid_argument("Rng::uniform: lo > hi");
    const std::uint64_t span = hi - lo;
    if (span == std::numeric_limits<std::uint64_t>::max()) return next_u64();
    const std::uint64_t n = span + 1;
    const std::uint64_t threshold = (0 - n) % n;
    for (;;) {
        const std::uint64_t r = next_u64();
        if (r >= threshold) return lo + r % n;
    }
}

// Approach: the top 53 bits scaled by 2^-53. Both the int->double conversion
//           and the multiply are exact, so the result is identical everywhere.
double Rng::next_double() {
    return static_cast<double>(next_u64() >> 11) * 0x1.0p-53;
}

bool Rng::bernoulli(double p) {
    if (p <= 0.0) return false;
    if (p >= 1.0) return true;
    return next_double() < p;
}

Rng Rng::fork() {
    return Rng(next_u64());
}

}  // namespace quorum::sim
