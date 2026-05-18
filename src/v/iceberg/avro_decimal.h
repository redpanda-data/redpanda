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

#include "absl/numeric/int128.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"

#include <array>
namespace iceberg {
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

/**
 * Converts a decimal into an array of bytes (big endian), this works the same
 * way as the Java's BigInteger.toByteArray() method.
 */
inline bytes encode_avro_decimal(absl::int128 decimal) {
    auto high_half = ss::cpu_to_be(absl::Uint128High64(decimal));
    auto low_half = ss::cpu_to_be(absl::Uint128Low64(decimal));

    bytes decimal_bytes(bytes::initialized_zero{}, 16);

    for (int i = 0; i < 8; i++) {
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        decimal_bytes[i] = (high_half >> (i * 8)) & 0xFF;
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        decimal_bytes[i + 8] = (low_half >> (i * 8)) & 0xFF;
    }

    return decimal_bytes;
}
/**
 * Converts an array of big endian encoded bytes to an absl::int128
 */
inline absl::int128 decode_avro_decimal(bytes bytes) {
    int64_t high_half = 0;
    uint64_t low_half = 0;
    for (size_t i = 0; i < 8; i++) {
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        high_half |= static_cast<int64_t>(bytes[i]) << i * 8;
        low_half |= static_cast<uint64_t>(bytes[i + 8]) << i * 8;
    }

    return absl::MakeInt128(ss::be_to_cpu(high_half), ss::be_to_cpu(low_half));
}

inline iobuf avro_decimal_to_iobuf(absl::int128 decimal, size_t max_size) {
    if (max_size > 16) {
        throw std::invalid_argument(
          "Decimal iobuf can not be larger than 16 bytes");
    }
    return bytes_to_iobuf(encode_avro_decimal(decimal));
}

inline absl::int128 iobuf_to_avro_decimal(iobuf buf) {
    if (buf.size_bytes() > 16) {
        throw std::invalid_argument(
          "Decimal iobuf can not be larger than 16 bytes");
    }

    return decode_avro_decimal(iobuf_to_bytes(buf));
}
} // namespace iceberg
