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
 * Iceberg's Avro mapping (https://iceberg.apache.org/spec/#avro) requires
 * `decimal(P, S)` to be stored as `fixed[minBytesRequired(P)]` — the
 * minimum number of bytes needed to represent the given precision. In
 * practice the same requirement applies to Iceberg manifest serialization.
 * The encoder produces exactly that many bytes, big-endian two's-complement,
 * with the value sign-extended on the left. The decoder accepts 0..16 byte
 * payloads, covering both the `fixed` and `bytes` Avro forms of `decimal`,
 * and sign-extends from the MSB of the first byte.
 */

#include "absl/numeric/int128.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"

#include <seastar/core/byteorder.hh>

#include <array>
#include <climits>
#include <cstring>
#include <stdexcept>

namespace iceberg {

/// Width of an `absl::int128`, in bytes — the upper bound on both encoded
/// payload sizes and accepted decode payloads.
constexpr size_t max_decimal_bytes = 16;

/**
 * Encode `decimal` as exactly `size` bytes of big-endian two's-complement —
 * the Iceberg / Avro wire format for `fixed[size]` decimals. The output
 * matches Java's reference encoder, which pads `BigInteger.toByteArray()`
 * on the left with the sign byte to reach `size`; we get there by
 * serializing the full 16-byte sign-extended representation and returning
 * its rightmost `size` bytes.
 *
 * Throws if `size` is outside `[1, max_decimal_bytes]` or if `decimal` does
 * not fit in a signed `size`-byte integer.
 */
inline bytes encode_avro_fixed_decimal(absl::int128 decimal, size_t size) {
    if (size == 0 || size > max_decimal_bytes) {
        throw std::invalid_argument(
          fmt::format(
            "Decimal size must be in [1, {}], got {}",
            max_decimal_bytes,
            size));
    }
    // Range check: a signed `size`-byte integer holds values in
    // [-2^(8*size-1), 2^(8*size-1) - 1]. At max_decimal_bytes the limit
    // would overflow int128 and every value fits anyway, so skip the check.
    if (size < max_decimal_bytes) {
        const auto shift = static_cast<int>(CHAR_BIT * size - 1);
        const absl::int128 limit = absl::int128{1} << shift;
        if (decimal < -limit || decimal >= limit) {
            throw std::invalid_argument(
              fmt::format("decimal value does not fit in {} bytes", size));
        }
    }
    // Serialize as a 16-byte big-endian two's-complement integer; the
    // sign-extending leading bytes are redundant once range-checked, so
    // return only the rightmost `size` bytes.
    const auto hi = ss::cpu_to_be(absl::Uint128High64(decimal));
    const auto lo = ss::cpu_to_be(absl::Uint128Low64(decimal));
    std::array<uint8_t, max_decimal_bytes> full{};
    std::memcpy(full.data(), &hi, sizeof(hi));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::memcpy(full.data() + sizeof(hi), &lo, sizeof(lo));
    return {full.begin() + (max_decimal_bytes - size), full.end()};
}

/**
 * Decode a big-endian two's-complement byte sequence into an absl::int128.
 *
 * Handles both Avro encodings of `decimal`: the fixed-width `fixed[N]` form
 * (value already sign-extended to fill N bytes) and the variable-width
 * `bytes` form (minimum bytes needed to preserve the sign — e.g. `0`, `1`,
 * and `-1` each fit in one byte, while `128` needs a leading `0x00` to
 * avoid being read as `-128`). The MSB of the first byte is sign-extended
 * into the high bits of the result.
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

inline iobuf avro_fixed_decimal_to_iobuf(absl::int128 decimal, size_t size) {
    return bytes_to_iobuf(encode_avro_fixed_decimal(decimal, size));
}

inline absl::int128 iobuf_to_avro_decimal(iobuf buf) {
    return decode_avro_decimal(iobuf_to_bytes(buf));
}
} // namespace iceberg
