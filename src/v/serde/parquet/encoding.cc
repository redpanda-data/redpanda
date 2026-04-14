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

#include <algorithm>
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
    }
    // NOLINTNEXTLINE(*reinterpret-cast*)
    buf.append(reinterpret_cast<const uint8_t*>(&v.val), sizeof(v.val));
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

boolean_value decode_stats_boolean(const iobuf& data) {
    iobuf_const_parser parser(data);
    plain_decoder<boolean_value> dec(parser);
    return dec.read_value();
}
int32_value decode_stats_int32(const iobuf& data) {
    iobuf_const_parser parser(data);
    plain_decoder<int32_value> dec(parser);
    return dec.read_value();
}
int64_value decode_stats_int64(const iobuf& data) {
    iobuf_const_parser parser(data);
    plain_decoder<int64_value> dec(parser);
    return dec.read_value();
}
float32_value decode_stats_float32(const iobuf& data) {
    iobuf_const_parser parser(data);
    plain_decoder<float32_value> dec(parser);
    return dec.read_value();
}
float64_value decode_stats_float64(const iobuf& data) {
    iobuf_const_parser parser(data);
    plain_decoder<float64_value> dec(parser);
    return dec.read_value();
}
byte_array_value decode_stats_byte_array(const iobuf& data) {
    return byte_array_value{data.copy()};
}

namespace {

template<typename level_type>
level_type read_level_value(iobuf_parser_base& parser, size_t bit_width) {
    if (bit_width > CHAR_WIDTH) {
        auto v = parser.consume_type<int16_t>();
        return level_type(ss::le_to_cpu(v));
    }
    return level_type(parser.consume_type<uint8_t>());
}

template<typename level_type>
chunked_vector<level_type> decode_levels_impl(
  iobuf_parser_base& parser,
  int32_t num_values,
  int32_t byte_length,
  level_type max_value) {
    size_t bit_width = std::bit_width(static_cast<uint16_t>(max_value()));
    chunked_vector<level_type> result;
    result.reserve(num_values);
    size_t start_pos = parser.bytes_consumed();
    size_t end_pos = start_pos + byte_length;

    if (bit_width == 0) {
        // All values are 0. The encoder writes count << 1 and the value.
        auto [header, _] = parser.read_unsigned_varint();
        auto count = header >> 1U;
        auto val = read_level_value<level_type>(parser, bit_width);
        for (uint32_t i = 0;
             i < count && result.size() < static_cast<size_t>(num_values);
             ++i) {
            result.push_back(val);
        }
        // Skip any remaining bytes
        if (parser.bytes_consumed() < end_pos) {
            parser.skip(end_pos - parser.bytes_consumed());
        }
        return result;
    }

    while (parser.bytes_consumed() < end_pos
           && result.size() < static_cast<size_t>(num_values)) {
        auto [header, _] = parser.read_unsigned_varint();
        if ((header & 1U) == 0) {
            // RLE run: count = header >> 1, followed by value
            auto count = header >> 1U;
            auto val = read_level_value<level_type>(parser, bit_width);
            for (uint32_t i = 0;
                 i < count && result.size() < static_cast<size_t>(num_values);
                 ++i) {
                result.push_back(val);
            }
        } else {
            // Bitpack run: num_groups = header >> 1
            // Each group has 8 values packed in bit_width bits
            auto num_groups = header >> 1U;
            auto total_values = num_groups * 8;
            size_t bits_remaining = 0;
            uint64_t buffer = 0;
            uint64_t mask = (1ULL << bit_width) - 1;
            for (uint32_t i = 0; i < total_values; ++i) {
                while (bits_remaining < bit_width) {
                    buffer
                      |= static_cast<uint64_t>(parser.consume_type<uint8_t>())
                         << bits_remaining;
                    bits_remaining += 8;
                }
                auto val = static_cast<int16_t>(buffer & mask);
                buffer >>= bit_width;
                bits_remaining -= bit_width;
                if (result.size() < static_cast<size_t>(num_values)) {
                    result.push_back(level_type(val));
                }
            }
        }
    }
    // Ensure we consumed exactly byte_length bytes
    if (parser.bytes_consumed() < end_pos) {
        parser.skip(end_pos - parser.bytes_consumed());
    }
    return result;
}

} // namespace

chunked_vector<rep_level> decode_levels(
  iobuf_parser_base& parser,
  int32_t num_values,
  int32_t byte_length,
  rep_level max_value) {
    return decode_levels_impl(parser, num_values, byte_length, max_value);
}

chunked_vector<def_level> decode_levels(
  iobuf_parser_base& parser,
  int32_t num_values,
  int32_t byte_length,
  def_level max_value) {
    return decode_levels_impl(parser, num_values, byte_length, max_value);
}

chunked_vector<int32_t> decode_rle_bp_int32(
  iobuf_parser_base& parser,
  int32_t num_values,
  int32_t byte_length,
  int32_t bit_width) {
    chunked_vector<int32_t> result;
    result.reserve(num_values);
    size_t start_pos = parser.bytes_consumed();
    size_t end_pos = start_pos + byte_length;
    size_t bw = static_cast<size_t>(bit_width);

    if (bw == 0) {
        auto [header, _] = parser.read_unsigned_varint();
        auto count = header >> 1U;
        // Value bytes: ceil(bit_width/8) = 0, but encoder writes 1 byte.
        auto val = static_cast<int32_t>(parser.consume_type<uint8_t>());
        for (uint32_t i = 0;
             i < count && result.size() < static_cast<size_t>(num_values);
             ++i) {
            result.push_back(val);
        }
        if (parser.bytes_consumed() < end_pos) {
            parser.skip(end_pos - parser.bytes_consumed());
        }
        return result;
    }

    size_t value_bytes = (bw + 7) / 8;

    while (parser.bytes_consumed() < end_pos
           && result.size() < static_cast<size_t>(num_values)) {
        auto [header, _] = parser.read_unsigned_varint();
        if ((header & 1U) == 0) {
            // RLE run
            auto count = header >> 1U;
            int32_t val = 0;
            for (size_t b = 0; b < value_bytes; ++b) {
                val |= static_cast<int32_t>(parser.consume_type<uint8_t>())
                       << (b * 8);
            }
            for (uint32_t i = 0;
                 i < count && result.size() < static_cast<size_t>(num_values);
                 ++i) {
                result.push_back(val);
            }
        } else {
            // Bitpack run
            auto num_groups = header >> 1U;
            auto total_values = num_groups * 8;
            size_t bits_remaining = 0;
            uint64_t buffer = 0;
            uint64_t mask = (1ULL << bw) - 1;
            for (uint32_t i = 0; i < total_values; ++i) {
                while (bits_remaining < bw) {
                    buffer
                      |= static_cast<uint64_t>(parser.consume_type<uint8_t>())
                         << bits_remaining;
                    bits_remaining += 8;
                }
                auto val = static_cast<int32_t>(buffer & mask);
                buffer >>= bw;
                bits_remaining -= bw;
                if (result.size() < static_cast<size_t>(num_values)) {
                    result.push_back(val);
                }
            }
        }
    }
    if (parser.bytes_consumed() < end_pos) {
        parser.skip(end_pos - parser.bytes_consumed());
    }
    return result;
}

plain_decoder<boolean_value>::plain_decoder(iobuf_parser_base& parser)
  : _parser(parser) {}

boolean_value plain_decoder<boolean_value>::read_value() {
    if (_shift >= CHAR_BIT) {
        _bits = _parser.consume_type<uint8_t>();
        _shift = 0;
    }
    bool val = (_bits >> _shift) & 1;
    ++_shift;
    return boolean_value{val};
}

template<typename value_type>
numeric_plain_decoder<value_type>::numeric_plain_decoder(
  iobuf_parser_base& parser)
  : _parser(parser) {}

template<typename value_type>
value_type numeric_plain_decoder<value_type>::read_value() {
    auto raw = _parser.consume_type<decltype(value_type::val)>();
    if constexpr (std::is_integral_v<decltype(value_type::val)>) {
        raw = ss::le_to_cpu(raw);
    }
    return value_type{raw};
}

template class numeric_plain_decoder<int32_value>;
template class numeric_plain_decoder<int64_value>;
template class numeric_plain_decoder<float32_value>;
template class numeric_plain_decoder<float64_value>;

plain_decoder<byte_array_value>::plain_decoder(iobuf_parser_base& parser)
  : _parser(parser) {}

byte_array_value plain_decoder<byte_array_value>::read_value() {
    auto len = ss::le_to_cpu(_parser.consume_type<int32_t>());
    return byte_array_value{_parser.copy(len)};
}

plain_decoder<fixed_byte_array_value>::plain_decoder(
  iobuf_parser_base& parser, int32_t fixed_length)
  : _parser(parser)
  , _fixed_length(fixed_length) {}

fixed_byte_array_value plain_decoder<fixed_byte_array_value>::read_value() {
    return fixed_byte_array_value{_parser.copy(_fixed_length)};
}

} // namespace serde::parquet
