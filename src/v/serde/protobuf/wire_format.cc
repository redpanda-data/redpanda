/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "serde/protobuf/wire_format.h"

#include "bytes/iobuf_parser.h"
#include "utils/vint.h"

#include <array>

namespace serde::pb {

constexpr static size_t int32_limit = 5;
constexpr static size_t int64_limit = 10;

tag tag::read(iobuf_parser* parser) {
    auto tag_value = read_varint<uint64_t>(parser);
    constexpr uint64_t wire_mask = 0b111;
    uint64_t wtype = tag_value & wire_mask;
    if (wtype > static_cast<int64_t>(wire_type::max)) [[unlikely]] {
        throw std::runtime_error(fmt::format("invalid wire type: {}", wtype));
    }
    uint64_t field_number = tag_value >> 3ULL;
    constexpr auto max_field_number = static_cast<uint64_t>(
      std::numeric_limits<int32_t>::max());
    if (field_number > max_field_number || field_number == 0) [[unlikely]] {
        throw std::runtime_error(
          fmt::format("invalid field number: {}", field_number));
    }
    return {
      .wire_type = static_cast<enum wire_type>(wtype),
      .field_number = static_cast<int32_t>(field_number)};
}

void tag::write(tag t, iobuf* out) {
    auto v = (static_cast<uint32_t>(t.field_number) << 3U);
    v |= std::to_underlying(t.wire_type);
    write_varint<uint64_t>(v, out);
}

namespace {

int32_t decode_zigzag(uint32_t n) {
    // NOTE: unsigned types prevent undefined behavior
    return static_cast<int32_t>((n >> 1U) ^ (~(n & 1U) + 1));
}

uint32_t encode_zigzag(int32_t n) {
    // NOLINTNEXTLINE(*-magic-numbers,*-signed-bitwise)
    return (static_cast<uint32_t>(n) << 1U) ^ static_cast<uint32_t>(n >> 31U);
}

} // namespace

template<>
int32_t read_varint<int32_t, zigzag::yes>(iobuf_parser* parser) {
    uint32_t v = read_varint<uint32_t>(parser);
    return decode_zigzag(v);
}
template<>
int32_t read_varint<int32_t, zigzag::no>(iobuf_parser* parser) {
    // Negative values are sign extended to 64 bits, so we need to raise our
    // limit to 10 bytes.
    auto buf = parser->peek_bytes(std::min(int64_limit, parser->bytes_left()));
    auto [v, len] = unsigned_vint::detail::deserialize(buf, int64_limit);
    parser->skip(len);
    return static_cast<int32_t>(v);
}
template<>
uint32_t read_varint<uint32_t, zigzag::no>(iobuf_parser* parser) {
    auto buf = parser->peek_bytes(std::min(int32_limit, parser->bytes_left()));
    auto [v, len] = unsigned_vint::detail::deserialize(buf, int32_limit);
    parser->skip(len);
    return static_cast<uint32_t>(v);
}
template<>
int64_t read_varint<int64_t, zigzag::yes>(iobuf_parser* parser) {
    uint64_t v = read_varint<uint64_t>(parser);
    return vint::decode_zigzag(v);
}
template<>
int64_t read_varint<int64_t, zigzag::no>(iobuf_parser* parser) {
    uint64_t v = read_varint<uint64_t>(parser);
    return static_cast<int64_t>(v);
}
template<>
uint64_t read_varint<uint64_t, zigzag::no>(iobuf_parser* parser) {
    auto buf = parser->peek_bytes(std::min(int64_limit, parser->bytes_left()));
    auto [v, len] = unsigned_vint::detail::deserialize(buf, int64_limit);
    parser->skip(len);
    return v;
}

int32_t read_length(iobuf_parser* parser) {
    auto len = read_varint<int32_t, zigzag::no>(parser);
    if (len < 0 || std::cmp_greater(len, parser->bytes_left())) [[unlikely]] {
        throw std::runtime_error(fmt::format(
          "invalid length field: (length={}, bytes_left={})",
          len,
          parser->bytes_left()));
    }
    return len;
}

void skip_unknown_field(iobuf_parser* parser, wire_type type) {
    switch (type) {
    case wire_type::varint:
        std::ignore = read_varint<uint64_t>(parser);
        break;
    case wire_type::i64:
        parser->skip(sizeof(int64_t));
        break;
    case wire_type::length:
        parser->skip(read_length(parser));
        break;
    case wire_type::i32:
        parser->skip(sizeof(int32_t));
        break;
    case wire_type::group_start:
    case wire_type::group_end:
    default:
        throw std::runtime_error(
          fmt::format("unsupported wire type: {}", std::to_underlying(type)));
    }
}

template<>
void write_varint<int32_t, zigzag::yes>(int32_t v, iobuf* out) {
    write_varint<uint32_t>(encode_zigzag(v), out);
}
template<>
void write_varint<int32_t, zigzag::no>(int32_t v, iobuf* out) {
    write_varint(static_cast<uint32_t>(v), out);
}
template<>
void write_varint<uint32_t, zigzag::no>(uint32_t v, iobuf* out) {
    write_varint<uint64_t>(v, out);
}
template<>
void write_varint<int64_t, zigzag::yes>(int64_t v, iobuf* out) {
    write_varint<uint64_t>(vint::encode_zigzag(v), out);
}
template<>
void write_varint<int64_t, zigzag::no>(int64_t v, iobuf* out) {
    write_varint<uint64_t>(v, out);
}
template<>
void write_varint<uint64_t, zigzag::no>(uint64_t v, iobuf* out) {
    std::array<uint8_t, int64_limit> buf{};
    size_t len = unsigned_vint::serialize(v, buf.data());
    out->append(buf.data(), len);
}
void write_length(int32_t length, iobuf* out) {
    write_varint<int32_t, zigzag::no>(length, out);
}

} // namespace serde::pb

auto fmt::formatter<serde::pb::tag>::format(
  const serde::pb::tag& t, format_context& ctx) const
  -> format_context::iterator {
    return fmt::format_to(
      ctx.out(),
      "tag{{wire_type: {}, field_number: {}}}",
      std::to_underlying(t.wire_type),
      static_cast<int>(t.field_number));
}
