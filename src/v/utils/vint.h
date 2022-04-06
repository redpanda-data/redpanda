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
inline size_t serialize(const int64_t x, uint8_t* out) noexcept {
    // *must* be named differently from input
    uint64_t value = encode_zigzag(x);
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
inline constexpr size_t vint_size(const int64_t value) noexcept {
    uint64_t v = encode_zigzag(value);
    size_t len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}
inline bytes to_bytes(int64_t value) noexcept {
    // our bytes uses a short-string optimization of 31 bytes
    auto out = ss::uninitialized_string<bytes>(max_length);
    auto sz = serialize(value, out.data());
    out.resize(sz);
    return out;
}

/// \brief almost identical impl to leveldb, made generic for c++
/// friendliness
/// https://github.com/google/leveldb/blob/201f52201f/util/coding.cc#L116
template<typename Range>
inline std::pair<int64_t, size_t> deserialize(Range&& r) noexcept {
    uint64_t result = 0;
    size_t bytes_read = 0;
    uint64_t shift = 0;
    for (auto src = r.begin(); shift <= 63 && src != r.end(); ++src) {
        uint64_t byte = *src;
        bytes_read++;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= byte << shift;
            break;
        }
        shift += 7;
    }
    return {decode_zigzag(result), bytes_read};
}

} // namespace vint
