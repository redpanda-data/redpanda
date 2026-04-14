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

#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "utils/named_type.h"

#include <cstdint>
#include <optional>

namespace serde::thrift {

using field_id = named_type<int16_t, struct field_id_tag>;

/**
 * The field type is the encoded value for a field that is written into a
 * struct/list header so readers know the upcoming type.
 */
enum class field_type : uint8_t {
    boolean_true = 1,
    boolean_false = 2,
    i8 = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    f64 = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    structure = 12,
    uuid = 13,
};

// A Struct is a sequence of zero or more fields, followed by a stop field. Each
// field starts with a field header and is followed by the encoded field value.
// The encoding can be summarized by the following BNF:
//
// struct        ::= ( field-header field-value )* stop-field
// field-header  ::= field-type field-id
//
// Compact protocol field header (short form) and field value:
// +--------+--------+...+--------+
// |ddddtttt| field value         |
// +--------+--------+...+--------+
//
// Compact protocol field header (1 to 3 bytes, long form) and field value:
// +--------+--------+...+--------+--------+...+--------+
// |0000tttt| field id            | field value         |
// +--------+--------+...+--------+--------+...+--------+
//
// Compact protocol stop field:
// +--------+
// |00000000|
// +--------+
class struct_encoder {
public:
    static inline const bytes empty_struct = {0}; // NOLINT

    void write_field(field_id id, field_type type, iobuf val);
    void write_field(field_id id, field_type type, bytes val);

    iobuf write_stop() &&;

private:
    void write_field_header(field_id id, field_type type);

    void write_short_form_field_header(uint8_t delta, field_type type);

    void write_long_form_field_header(field_type type, field_id field_id);

    iobuf _buf;
    field_id _last_field_id = field_id(0);
};

// List and sets are encoded the same: a header indicating the size and the
// element-type of the elements, followed by the encoded elements.
//
// Compact protocol list header (1 byte, short form) and elements:
// +--------+--------+...+--------+
// |sssstttt| elements            |
// +--------+--------+...+--------+
//
// Compact protocol list header (2+ bytes, long form) and elements:
// +--------+--------+...+--------+--------+...+--------+
// |1111tttt| size                | elements            |
// +--------+--------+...+--------+--------+...+--------+
class list_encoder {
public:
    explicit list_encoder(size_t size, field_type type);

    void write_element(iobuf val);
    void write_element(bytes val);

    iobuf finish() &&;

private:
    void write_short_form_field_header(uint8_t size, field_type type);

    void write_long_form_field_header(field_type type, size_t size);

    iobuf _buf;
};

/**
 * Strings are length prefix encoded.
 *
 * First an unsigned varint for the length, then the string contents itself.
 *
 * Note that all strings passed to this method are expected to be small due to
 * types that use contiguous memory here.
 */
bytes encode_string(std::string_view str);

/**
 * Binary is length prefix encoded.
 *
 * First an unsigned varint for the length, then the binary contents itself.
 */
iobuf encode_binary(iobuf b);

// A Struct decoder reads fields from Thrift compact encoded data. Fields are
// decoded one at a time; the caller reads each field's value based on its type.
// Unknown fields can be skipped for forward compatibility.
//
// Usage:
//   struct_decoder dec(parser);
//   while (auto hdr = dec.read_field_header()) {
//       switch (hdr->id()) {
//       case 1: my_field = decode_i32(parser); break;
//       default: dec.skip_field(parser, hdr->type); break;
//       }
//   }
class struct_decoder {
public:
    struct field_header {
        field_id id;
        field_type type;
    };

    explicit struct_decoder(iobuf_parser_base& parser);

    // Read the next field header. Returns std::nullopt at the stop field.
    //
    // Booleans: the value is encoded in the type nibble itself
    // (boolean_true / boolean_false). No separate value read is needed.
    std::optional<field_header> read_field_header();

    // Skip a field value of the given type. Used for unknown fields.
    void skip_field(field_type type);

private:
    iobuf_parser_base& _parser;
    field_id _last_field_id = field_id(0);
};

// A List/Set decoder reads the header and provides element count and type.
// The caller reads each element's value based on the element type.
//
// Usage:
//   list_decoder dec(parser);
//   for (size_t i = 0; i < dec.size(); ++i) {
//       values.push_back(decode_i32(parser));
//   }
class list_decoder {
public:
    explicit list_decoder(iobuf_parser_base& parser);

    field_type element_type() const;
    size_t size() const;

private:
    field_type _type;
    size_t _size;
};

// Decode a zigzag-encoded varint as int16_t.
int16_t decode_i16(iobuf_parser_base& parser);
// Decode a zigzag-encoded varint as int32_t.
int32_t decode_i32(iobuf_parser_base& parser);
// Decode a zigzag-encoded varint as int64_t.
int64_t decode_i64(iobuf_parser_base& parser);
// Decode a length-prefixed UTF-8 string.
ss::sstring decode_string(iobuf_parser_base& parser);
// Decode a length-prefixed binary blob.
iobuf decode_binary(iobuf_parser_base& parser);

} // namespace serde::thrift
