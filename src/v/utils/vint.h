/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/bytes.h"

#include <cstdint>

namespace unsigned_vint {
/// At most 5 bytes are needed to encode a 32 bit value
inline constexpr size_t max_length = 5;

namespace detail {
struct var_decoder {
    uint64_t result = 0;
    size_t bytes_read = 0;
    uint64_t shift = 0;
    uint64_t limit;

    explicit var_decoder(uint64_t limit)
      : limit(limit) {}

    /**
     * @brief Incorporate a new byte into the state.
     * Returns true if the outer loop should break (last byte found).
     */
    bool accept(uint64_t byte) {
        /// \brief almost identical impl to leveldb, made generic for c++
        /// friendliness
        /// https://github.com/google/leveldb/blob/201f52201f/util/coding.cc#L116
        if (shift > limit) {
            return true;
        }
        bytes_read++;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= byte << shift;
            return true;
        }
        shift += 7;
        return false;
    }
};

template<typename Range>
inline std::pair<uint64_t, size_t>
deserialize(Range&& r, size_t max_bytes) noexcept {
    uint64_t limit = ((max_bytes - 1) * 7);
    var_decoder decoder(limit);
    for (auto byte : r) {
        if (decoder.accept(byte)) {
            break;
        }
    }
    return std::make_pair(decoder.result, decoder.bytes_read);
}

} // namespace detail

inline size_t serialize(uint64_t value, uint8_t* out) noexcept {
    size_t bytes_used = 0;
    if (value < 0x80) {
        out[bytes_used++] = static_cast<uint8_t>(value);
        return bytes_used;
    }
    out[bytes_used++] = static_cast<uint8_t>(value | 0x80);
    value >>= 7;
    if (value < 0x80) {
        out[bytes_used++] = static_cast<uint8_t>(value);
        return bytes_used;
    }
    do {
        out[bytes_used++] = static_cast<uint8_t>(value | 0x80);
        value >>= 7;
    } while (unlikely(value >= 0x80));
    out[bytes_used++] = static_cast<uint8_t>(value);
    return bytes_used;
}

template<typename Range>
inline std::pair<uint32_t, size_t> deserialize(Range&& r) noexcept {
    auto [result, bytes_read] = detail::deserialize(
      std::forward<Range>(r), max_length);
    return {static_cast<uint32_t>(result), bytes_read};
}

inline constexpr size_t size(uint64_t v) noexcept {
    size_t len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

inline bytes to_bytes(uint32_t value) noexcept {
    // our bytes uses a short-string optimization of 31 bytes, at most
    // unsigned_vint::max_length bytes will be used to allocate the encoded size
    // at the start of the returned buffer
    auto sz = size(static_cast<uint64_t>(value));
    bytes out(bytes::initialized_later{}, sz);
    serialize(value, out.begin());
    return out;
}

} // namespace unsigned_vint

// class is actually zigzag vint; always signed ints
// matches exactly the kafka encoding which uses protobuf
namespace vint {
// Go language implementation details it nicely:
// https://golang.org/src/encoding/binary/varint.go
//
// Design note: At most 10 bytes are needed for 64-bit values. The encoding
// could be more dense: a full
// 64-bit value needs an extra byte just to hold bit 63. Instead, the msb of the
// previous byte could be used to hold bit 63 since we know there can't be more
// than 64 bits. This is a trivial improvement and would reduce the maximum
// encoding length to 9 bytes. However, it breaks the invariant that the msb is
// always the "continuation bit" and thus makes the format incompatible with a
// varint encoding for larger numbers (say 128-bit).
inline constexpr size_t max_length = 10;
inline constexpr uint64_t encode_zigzag(int64_t v) noexcept {
    // Note:  the right-shift must be arithmetic
    // Note:  left shift must be unsigned because of overflow
    return ((uint64_t)(v) << 1) ^ (uint64_t)(v >> 63);
}
inline constexpr int64_t decode_zigzag(uint64_t v) noexcept {
    return (int64_t)((v >> 1) ^ (~(v & 1) + 1));
}
inline constexpr size_t vint_size(const int64_t value) noexcept {
    uint64_t v = encode_zigzag(value);
    return unsigned_vint::size(v);
}

inline size_t serialize(const int64_t x, uint8_t* out) noexcept {
    uint64_t value = encode_zigzag(x);
    return unsigned_vint::serialize(value, out);
}

/// \brief almost identical impl to leveldb, made generic for c++
/// friendliness
/// https://github.com/google/leveldb/blob/201f52201f/util/coding.cc#L116
template<typename Range>
inline std::pair<int64_t, size_t> deserialize(Range&& r) noexcept {
    auto [result, bytes_read] = unsigned_vint::detail::deserialize(
      std::forward<Range>(r), max_length);
    return {decode_zigzag(result), bytes_read};
}

inline bytes to_bytes(int64_t value) noexcept {
    // our bytes uses a short-string optimization of 31 bytes, at most
    // vint::max_length bytes will be used to allocate the encoded size at the
    // start of the returned buffer
    auto sz = vint_size(value);
    bytes out(bytes::initialized_later{}, sz);
    serialize(value, out.begin());
    return out;
}

} // namespace vint
