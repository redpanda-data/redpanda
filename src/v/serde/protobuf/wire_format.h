/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <fmt/core.h>

#include <cstdint>
#include <type_traits>

class iobuf;
class iobuf_parser;

namespace serde::pb {

// All valid protobuf wire types.
enum class wire_type : uint8_t {
    varint = 0,
    i64 = 1,
    length = 2,
    group_start = 3,
    group_end = 4,
    i32 = 5,
    max = 5,
};

// A tag is a field number and wire type pair. The wire type is the first 3 bits
// of the varint encoded field number. The field number is the rest of the
// varint.
struct tag {
    wire_type wire_type;
    int32_t field_number;

    // read a tag from the parser, if the tag is invalid (or there is not enough
    // bytes) throw an exception.
    static tag read(iobuf_parser* parser);

    // write a tag to the output buffer.
    static void write(tag t, iobuf* out);
};

// If the variant should be zigzag encoded/decoded or not.
enum class zigzag : uint8_t { yes, no };

// Read a varint from the parser, with optional zigzag decoding for signed
// types.
template<
  typename T,
  zigzag NeedsDecoding = std::is_signed_v<T> ? zigzag::yes : zigzag::no>
T read_varint(iobuf_parser* parser);

template<>
int32_t read_varint<int32_t, zigzag::yes>(iobuf_parser* parser);
template<>
int32_t read_varint<int32_t, zigzag::no>(iobuf_parser* parser);
template<>
uint32_t read_varint<uint32_t, zigzag::no>(iobuf_parser* parser);
template<>
int64_t read_varint<int64_t, zigzag::yes>(iobuf_parser* parser);
template<>
int64_t read_varint<int64_t, zigzag::no>(iobuf_parser* parser);
template<>
uint64_t read_varint<uint64_t, zigzag::no>(iobuf_parser* parser);

// Read a length from length wire format types (packed, string and
// message fields). It also validates that the length is within bounds of the
// remaining data inside the parser.
int32_t read_length(iobuf_parser* parser);

// Skip an unknown field that is encountered on the wire.
void skip_unknown_field(iobuf_parser* parser, wire_type type);

// Write a varint to the buffer, with optional zigzag encoding for signed
// types.
template<
  typename T,
  zigzag NeedsDecoding = std::is_signed_v<T> ? zigzag::yes : zigzag::no>
void write_varint(T v, iobuf* out);

template<>
void write_varint<int32_t, zigzag::yes>(int32_t v, iobuf* out);
template<>
void write_varint<int32_t, zigzag::no>(int32_t v, iobuf* out);
template<>
void write_varint<uint32_t, zigzag::no>(uint32_t v, iobuf* out);
template<>
void write_varint<int64_t, zigzag::yes>(int64_t v, iobuf* out);
template<>
void write_varint<int64_t, zigzag::no>(int64_t v, iobuf* out);
template<>
void write_varint<uint64_t, zigzag::no>(uint64_t v, iobuf* out);

// Write a length from length wire format types (packed, string and
// message fields).
void write_length(int32_t length, iobuf* out);

} // namespace serde::pb

template<>
struct fmt::formatter<serde::pb::tag> {
    constexpr auto parse(format_parse_context& ctx)
      -> format_parse_context::iterator {
        return ctx.begin();
    }
    auto format(const serde::pb::tag& value, format_context& ctx) const
      -> format_context::iterator;
};
