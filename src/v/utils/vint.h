#pragma once

#include "bytes/bytes.h"

#include <cstdint>
#include <type_traits>

using vint_size_type = bytes::size_type;

static constexpr size_t max_vint_length = 9;

namespace internal {

template<typename ValueType>
class vint_base final {
    static_assert(std::is_signed_v<ValueType>);
    using UValueType = std::make_unsigned_t<ValueType>;
    static constexpr uint8_t more_bytes = 128;

public:
    using value_type = ValueType;

    struct result {
        ValueType value;
        vint_size_type bytes_read;
    };

    static constexpr UValueType encode_zigzag(ValueType n) noexcept {
        // The right shift has to be arithmetic and not logical.
        return (static_cast<UValueType>(n) << 1)
               ^ static_cast<UValueType>(
                 n >> std::numeric_limits<ValueType>::digits);
    }

    static constexpr ValueType decode_zigzag(UValueType n) noexcept {
        return static_cast<std::make_signed_t<ValueType>>((n >> 1) ^ -(n & 1));
    }

    static vint_size_type
    serialize(ValueType value, bytes::iterator out) noexcept {
        auto encode = encode_zigzag(value);
        vint_size_type size = 1;
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
        UValueType result = 0;
        uint8_t shift = 0;
        vint_size_type bytes_read = 0;
        for (auto src = r.begin(); src != r.end(); ++src) {
            bytes_read++;
            uint64_t byte = *src;
            if (byte & more_bytes) {
                result |= (byte & (more_bytes - 1)) << shift;
            } else {
                result |= byte << shift;
                break;
            }
            shift = std::min(std::numeric_limits<ValueType>::digits, shift + 7);
        }
        return {decode_zigzag(result), bytes_read};
    }
};

} // namespace internal

using vint = internal::vint_base<int32_t>;
using vlong = internal::vint_base<int64_t>;
