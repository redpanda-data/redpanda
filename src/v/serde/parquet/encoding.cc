/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/parquet/encoding.h"

#include "utils/vint.h"

#include <bit>
#include <climits>
#include <type_traits>

namespace serde::parquet {

void plain_encoder<boolean_value>::add_value(boolean_value v) {
    bits |= static_cast<unsigned>(v.val) << shift;
    shift = ++shift % CHAR_BIT;
    if (shift == 0) {
        buf.append(&bits, 1);
        bits = 0;
    }
}

iobuf plain_encoder<boolean_value>::get_encoded_buf() {
    if (shift != 0) {
        buf.append(&bits, 1);
        bits = 0;
        shift = 0;
    }
    return std::exchange(buf, {});
}

size_t plain_encoder<boolean_value>::size_bytes() const {
    return buf.size_bytes();
}

template<typename value_type>
void numeric_plain_encoder<value_type>::add_value(value_type v) {
    if constexpr (std::is_integral_v<decltype(v.val)>) {
        v.val = ss::cpu_to_le(v.val);
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<const uint8_t*>(&v.val), sizeof(v.val));
    } else {
        // PLAIN encoding stores FLOAT/DOUBLE as little-endian IEEE bytes.
        // cpu_to_le only accepts integrals, so byteswap the bit pattern.
        using bits_type
          = std::conditional_t<sizeof(v.val) == 4, uint32_t, uint64_t>;
        auto bits = ss::cpu_to_le(std::bit_cast<bits_type>(v.val));
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<const uint8_t*>(&bits), sizeof(bits));
    }
}

template<typename value_type>
iobuf numeric_plain_encoder<value_type>::get_encoded_buf() {
    return std::exchange(buf, {});
}

template<typename value_type>
size_t numeric_plain_encoder<value_type>::size_bytes() const {
    return buf.size_bytes();
}

template class numeric_plain_encoder<int32_value>;
template class numeric_plain_encoder<int64_value>;
template class numeric_plain_encoder<float32_value>;
template class numeric_plain_encoder<float64_value>;

void plain_encoder<byte_array_value>::add_value(byte_array_value&& v) {
    int32_t i = ss::cpu_to_le(static_cast<int32_t>(v.val.size_bytes()));
    // NOLINTNEXTLINE(*reinterpret-cast*)
    buf.append(reinterpret_cast<const uint8_t*>(&i), sizeof(int32_t));
    buf.append(std::move(v.val));
}

iobuf plain_encoder<byte_array_value>::get_encoded_buf() {
    return std::exchange(buf, {});
}

size_t plain_encoder<byte_array_value>::size_bytes() const {
    return buf.size_bytes();
}

void plain_encoder<fixed_byte_array_value>::add_value(
  fixed_byte_array_value&& v) {
    buf.append(std::move(v.val));
}

iobuf plain_encoder<fixed_byte_array_value>::get_encoded_buf() {
    return std::exchange(buf, {});
}

size_t plain_encoder<fixed_byte_array_value>::size_bytes() const {
    return buf.size_bytes();
}

namespace {

template<typename level_type>
void write_level_value(iobuf& buf, size_t max_bit_width, level_type val) {
    if (max_bit_width > CHAR_WIDTH) {
        auto v = ss::cpu_to_le(val());
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<uint8_t*>(&v), 2);
    } else {
        auto v = static_cast<uint8_t>(val());
        buf.append(&v, 1);
    }
}

template<typename level_type>
iobuf encode_levels_impl(
  level_type max_value, const chunked_vector<level_type>& levels) {
    size_t bit_width = std::bit_width(static_cast<uint16_t>(max_value()));
    iobuf buf;
    if (bit_width == 0) {
        bytes len = unsigned_vint::to_bytes(levels.size() << 1U);
        buf.append(len.data(), len.size());
        write_level_value(buf, bit_width, max_value);
        return buf;
    }
    auto it = levels.begin();
    vassert(it != levels.end(), "bit width must be 0 if levels is empty");
    // NOTE: This implementation is simple, not optimal. A clean optimal
    // encoding can be found in parquet-go, which alternates between bitpacking
    // and run length encoding depending on which encodes better. This tradeoff
    // is also what duckdb does FWIW.
    level_type current_value = *it;
    uint32_t current_run = 1;
    auto flush = [&]() {
        bytes rl = unsigned_vint::to_bytes(current_run << 1U);
        buf.append(rl.data(), rl.size());
        write_level_value(buf, bit_width, current_value);
    };
    for (++it; it != levels.end(); ++it) {
        level_type v = *it;
        if (v == current_value) {
            ++current_run;
        } else {
            flush();
            current_value = v;
            current_run = 1;
        }
    }
    flush();
    return buf;
}
} // namespace

iobuf encode_levels(
  rep_level max_value, const chunked_vector<rep_level>& levels) {
    return encode_levels_impl(max_value, levels);
}

iobuf encode_levels(
  def_level max_value, const chunked_vector<def_level>& levels) {
    return encode_levels_impl(max_value, levels);
}

iobuf encode_for_stats(boolean_value v) {
    plain_encoder<boolean_value> e{};
    e.add_value(v);
    return e.get_encoded_buf();
}
iobuf encode_for_stats(int32_value v) {
    plain_encoder<int32_value> e{};
    e.add_value(v);
    return e.get_encoded_buf();
}
iobuf encode_for_stats(int64_value v) {
    plain_encoder<int64_value> e{};
    e.add_value(v);
    return e.get_encoded_buf();
}
iobuf encode_for_stats(float32_value v) {
    plain_encoder<float32_value> e{};
    e.add_value(v);
    return e.get_encoded_buf();
}
iobuf encode_for_stats(float64_value v) {
    plain_encoder<float64_value> e{};
    e.add_value(v);
    return e.get_encoded_buf();
}
iobuf encode_for_stats(const byte_array_value& v) { return v.val.copy(); }
iobuf encode_for_stats(const fixed_byte_array_value& v) { return v.val.copy(); }
} // namespace serde::parquet
