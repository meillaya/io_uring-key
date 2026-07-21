#pragma once
// CRC32C (Castagnoli) for WAL records and SSTable blocks.

#include <cstddef>
#include <cstdint>

namespace kv {

namespace detail {
    struct CRC32CTable {
        uint32_t entries[256]{};
        constexpr CRC32CTable() {
            for (uint32_t i = 0; i < 256; i++) {
                uint32_t crc = i;
                for (int j = 0; j < 8; j++) {
                    crc = (crc >> 1) ^ (0x82F63B78u & (~(crc & 1) + 1));
                }
                entries[i] = crc;
            }
        }
    };
    inline constexpr CRC32CTable kCRC32CTable{};
} // namespace detail

class CRC32 {
public:
    static uint32_t Compute(const void* data, size_t len) {
        auto* p = static_cast<const uint8_t*>(data);
        uint32_t crc = ~0u;
        while (len--) {
            crc = (crc >> 8) ^ detail::kCRC32CTable.entries[(crc ^ *p++) & 0xFF];
        }
        return ~crc;
    }

    static uint32_t Extend(uint32_t crc, const void* data, size_t len) {
        auto* p = static_cast<const uint8_t*>(data);
        crc = ~crc;
        while (len--) {
            crc = (crc >> 8) ^ detail::kCRC32CTable.entries[(crc ^ *p++) & 0xFF];
        }
        return ~crc;
    }
};

} // namespace kv
