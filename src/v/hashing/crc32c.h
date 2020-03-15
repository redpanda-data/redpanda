#pragma once
#include <crc32c/crc32c.h>

#include <type_traits>

class crc32 {
public:
    template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
    void extend(T num) noexcept {
        // NOLINTNEXTLINE
        extend(reinterpret_cast<const uint8_t*>(&num), sizeof(T));
    }
    void extend(const uint8_t* data, size_t size) {
        _crc = crc32c::Extend(_crc, data, size);
    }
    void extend(const char* data, size_t size) {
        extend(
          // NOLINTNEXTLINE
          reinterpret_cast<const uint8_t*>(data),
          size);
    }

    uint32_t value() const { return _crc; }

private:
    uint32_t _crc = 0;
};
