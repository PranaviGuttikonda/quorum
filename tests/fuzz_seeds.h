#pragma once

// Given: the fuzz tests should run 1,000 seeds by default, fewer in slow
//        (Debug) CI jobs.
// Approach: QUORUM_FUZZ_SEEDS overrides the count. An unset variable means
//           1000. An invalid value (not a positive integer) throws, so a
//           misconfigured job fails loudly instead of quietly testing
//           something else.

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace quorum::testing {

inline std::uint64_t parse_fuzz_seeds(const char* value) {
    if (value == nullptr) return 1000;
    const std::string_view s(value);
    std::uint64_t n = 0;
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), n);
    if (s.empty() || ec != std::errc{} || ptr != s.data() + s.size() || n == 0)
        throw std::invalid_argument("QUORUM_FUZZ_SEEDS must be a positive integer, got \"" +
                                    std::string(s) + "\"");
    return n;
}

inline std::uint64_t fuzz_seeds() {
    static const std::uint64_t n = parse_fuzz_seeds(std::getenv("QUORUM_FUZZ_SEEDS"));
    return n;
}

}  // namespace quorum::testing
