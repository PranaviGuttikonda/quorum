// Given: CRC-32 must match the standard (zlib/IEEE) definition.
// Approach: the published check value, the empty string, and sensitivity to
//           a single flipped bit.

#include <string>

#include "harness.h"
#include "quorum/storage/crc32.h"

using quorum::storage::crc32;

TEST(crc32_matches_standard_check_values) {
    CHECK_EQ(crc32("123456789"), std::uint32_t{0xCBF43926});
    CHECK_EQ(crc32(""), std::uint32_t{0});
    CHECK_EQ(crc32("The quick brown fox jumps over the lazy dog"), std::uint32_t{0x414FA339});
}

TEST(crc32_detects_every_single_bit_flip) {
    const std::string base = "quorum log record payload";
    const auto want = crc32(base);
    for (std::size_t i = 0; i < base.size(); ++i) {
        for (int b = 0; b < 8; ++b) {
            std::string s = base;
            s[i] = static_cast<char>(s[i] ^ (1 << b));
            CHECK(crc32(s) != want);
        }
    }
}
