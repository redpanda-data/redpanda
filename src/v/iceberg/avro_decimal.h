/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

/**
 * @file
 * Helpers for encoding and decoding Avro `decimal` logical type values as
 * big-endian two's-complement integers.
 *
 * Precision is capped at 128 bits (i.e. up to the range of `absl::int128`).
 * Avro itself does not bound the precision of a `decimal`, but our
 * downstream consumer Iceberg restricts it to at most 38 digits, and
 * 10^38 - 1 fits comfortably within a signed 128-bit integer
 * (ceil(log2(10^38)) = 127 bits). No representable value is lost when data
 * destined for this downstream consumer passes through `absl::int128`.
 *
 * See the Iceberg spec: "decimal(P, S) ... precision must be 38 or less"
 * https://iceberg.apache.org/spec/#primitive-types
 *
 * The encoder always emits exactly 16 bytes (Java `BigInteger.toByteArray()`
 * style, suitable for Avro `fixed[16]`), while the decoder accepts payloads
 * of 0..16 bytes and sign-extends as required by Avro's variable-width
 * `bytes` representation of `decimal`.
 */

#include "absl/numeric/int128.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"

#include <array>
#include <cstring>

namespace iceberg {

/// Byte width of an Avro `decimal` value once represented as `absl::int128`:
/// 128 bits / 8 = 16 bytes. Used as both the fixed encoded width and the
/// upper bound on accepted decode payloads.
constexpr size_t max_decimal_bytes = 16;

/**
 * Converts a decimal into an array of bytes (big endian), this works the same
 * way as the Java's BigInteger.toByteArray() method.
 */
inline bytes encode_avro_decimal(absl::int128 decimal) {
    auto high_half = ss::cpu_to_be(absl::Uint128High64(decimal));
    auto low_half = ss::cpu_to_be(absl::Uint128Low64(decimal));

    bytes decimal_bytes(bytes::initialized_zero{}, max_decimal_bytes);

    for (int i = 0; i < 8; i++) {
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        decimal_bytes[i] = (high_half >> (i * 8)) & 0xFF;
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        decimal_bytes[i + 8] = (low_half >> (i * 8)) & 0xFF;
    }

    return decimal_bytes;
}
/**
 * Decodes a big-endian two's-complement byte sequence into an absl::int128,
 * matching Avro's `decimal` wire format.
 *
 * Avro uses the minimum number of bytes required to preserve the sign — e.g.
 * `0`, `1`, and `-1` each fit in a single byte, while `128` needs a leading
 * `0x00` to avoid being read as `-128`. The MSB of the first byte is
 * sign-extended into the high bits of the result.
 *
 * Accepts 0..16 byte payloads; longer inputs are rejected.
 */
inline absl::int128 decode_avro_decimal(bytes input) {
    if (input.size() > max_decimal_bytes) {
        throw std::invalid_argument(
          "Decimal bytes cannot be larger than 16 bytes");
    }
    if (input.empty()) {
        return 0;
    }
    // Right-align the payload in a 16-byte big-endian buffer and sign-extend
    // the leading bytes from the MSB of input[0], then read it as two
    // byteswapped 64-bit halves.
    std::array<uint8_t, max_decimal_bytes> buf{};
    buf.fill((input[0] & 0x80U) == 0 ? 0x00 : 0xFF);

    std::memcpy(
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
      buf.data() + (buf.size() - input.size()),
      input.data(),
      input.size());

    uint64_t hi = 0;
    uint64_t lo = 0;

    std::memcpy(&hi, buf.data(), sizeof(hi));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::memcpy(&lo, buf.data() + sizeof(hi), sizeof(lo));

    return absl::MakeInt128(
      static_cast<int64_t>(ss::be_to_cpu(hi)), ss::be_to_cpu(lo));
}

inline iobuf avro_decimal_to_iobuf(absl::int128 decimal, size_t max_size) {
    if (max_size > max_decimal_bytes) {
        throw std::invalid_argument(
          "Decimal iobuf can not be larger than 16 bytes");
    }
    return bytes_to_iobuf(encode_avro_decimal(decimal));
}

inline absl::int128 iobuf_to_avro_decimal(iobuf buf) {
    if (buf.size_bytes() > max_decimal_bytes) {
        throw std::invalid_argument(
          "Decimal iobuf can not be larger than 16 bytes");
    }

    return decode_avro_decimal(iobuf_to_bytes(buf));
}
} // namespace iceberg
