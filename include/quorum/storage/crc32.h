#pragma once

// Given: log records need a checksum that catches torn writes and bit flips.
// Approach: CRC-32 (IEEE 802.3, reflected polynomial 0xEDB88320), as used by
//           zlib and Ethernet, with a 256-entry table built at compile time.
//           crc32("123456789") == 0xCBF43926.

#include <cstdint>
#include <string_view>

namespace quorum::storage {

std::uint32_t crc32(std::string_view data);

}  // namespace quorum::storage
