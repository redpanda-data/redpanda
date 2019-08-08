#pragma once

#include "bytes/bytes.h"

#include <cstdint>
#include <type_traits>

class vint final {
public:
    using value_type = int64_t;
    using size_type = bytes::size_type;
    static constexpr size_t max_length = sizeof(value_type) + 1;

    struct result {
        value_type value;
        size_type bytes_read;
    };

private:
    static constexpr uint8_t more_bytes = 128;

    using uvalue_type = uint64_t;

    static constexpr uvalue_type encode_zigzag(value_type n) noexcept {
        // The right shift has to be arithmetic and not logical.
        return (static_cast<uvalue_type>(n) << 1)
               ^ static_cast<uvalue_type>(
                 n >> std::numeric_limits<value_type>::digits);
    }

    static constexpr value_type decode_zigzag(uvalue_type n) noexcept {
        return static_cast<value_type>((n >> 1) ^ -(n & 1));
    }

public:
    static size_type
    serialize(value_type value, bytes::iterator out) noexcept {
        auto encode = encode_zigzag(value);
        size_type size = 1;
        while (encode >= more_bytes) {
            *out++ = static_cast<int8_t>(encode | more_bytes);
            encode >>= 7;
            ++size;
        }
        *out++ = static_cast<int8_t>(encode);
        return size;
    }

    template<typename Range>
    static result deserialize(Range&& r) noexcept {
        uvalue_type result = 0;
        uint8_t shift = 0;
        size_type bytes_read = 0;
        for (auto src = r.begin(); src != r.end(); ++src) {
            bytes_read++;
            uint64_t byte = *src;
            if (has_more_bytes(byte)) {
                result |= (byte & (more_bytes - 1)) << shift;
            } else {
                result |= byte << shift;
                break;
            }
            shift = std::min(std::numeric_limits<value_type>::digits, shift + 7);
        }
        return {decode_zigzag(result), bytes_read};
    }

    static bool has_more_bytes(int8_t byte) {
        return byte & more_bytes;
    }
};
