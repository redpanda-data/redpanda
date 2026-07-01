/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/bucket_transform_hashing_visitor.h"

#include "hashing/murmur.h"

namespace iceberg {
namespace {
uint32_t hash_long(int64_t v) {
    const auto le = ss::cpu_to_le(v);
    return murmurhash3_x86_32(&le, sizeof(le), 0);
}
} // namespace

uint32_t bucket_transform_hashing_visitor::operator()(const int_value& value) {
    return hash_long(value.val);
}

uint32_t bucket_transform_hashing_visitor::operator()(const long_value& value) {
    return hash_long(value.val);
}

uint32_t bucket_transform_hashing_visitor::operator()(const time_value& value) {
    return hash_long(value.val);
}
uint32_t bucket_transform_hashing_visitor::operator()(const date_value& value) {
    return hash_long(value.val);
}
uint32_t
bucket_transform_hashing_visitor::operator()(const timestamp_value& value) {
    return hash_long(value.val);
}
uint32_t
bucket_transform_hashing_visitor::operator()(const timestamptz_value& value) {
    return hash_long(value.val);
}
uint32_t
bucket_transform_hashing_visitor::operator()(const decimal_value& value) {
    const auto low_val = Int128Low64(value.val);
    const auto high_val = Int128High64(value.val);

    std::array<uint8_t, 16> value_bytes{0};

    /**
     * Both Java and PyIceberg implementations encode the decimal in big endian
     * format and then they hash an array with the smallest size required to
     * represent the decimal.
     *
     * Extract the bytes most-significant first via arithmetic shifts, which is
     * independent of host byte order (don't byteswap into a native value and
     * then shift, which only yields big-endian bytes on little-endian hosts).
     */
    for (uint8_t i = 0; i < 8; i++) {
        const auto shift = (7 - i) * 8;
        value_bytes[i] = (high_val >> shift) & 0xFF;
        value_bytes[i + 8] = (low_val >> shift) & 0xFF;
    }
    /**
     * Limit the size of the array to the smallest size required to represent
     * the number
     */
    auto offset = 0;
    for (int i = 0; i < 15; ++i) {
        if (value_bytes[i] == 0x00 && (value_bytes[i + 1] & 0x80) == 0x00) {
            offset++;
        } else if (
          value_bytes[i] == 0xFF && (value_bytes[i + 1] & 0x80) == 0x80) {
            offset++;
        } else {
            break;
        }
    }

    return murmurhash3_x86_32(value_bytes.data() + offset, 16 - offset, 0);
}

uint32_t
bucket_transform_hashing_visitor::operator()(const string_value& value) {
    return murmurhash3_x86_32(value.val, 0);
}
uint32_t
bucket_transform_hashing_visitor::operator()(const fixed_value& value) {
    return murmurhash3_x86_32(value.val, 0);
}
uint32_t
bucket_transform_hashing_visitor::operator()(const binary_value& value) {
    return murmurhash3_x86_32(value.val, 0);
}
uint32_t bucket_transform_hashing_visitor::operator()(const uuid_value& v) {
    const auto& data = v.val.uuid().data;
    return murmurhash3_x86_32(
      static_cast<const uint8_t*>(data), sizeof(data), 0);
}

} // namespace iceberg
