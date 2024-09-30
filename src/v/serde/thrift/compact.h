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

#include <cstdint>

namespace serde::thrift {

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

    template<typename T>
    void write_field(int16_t field_id, field_type type, T val) {
        write_field_header(field_id, type);
        if constexpr (std::is_same_v<T, iobuf>) {
            _buf.append(std::move(val));
        } else {
            _buf.append(val.data(), val.size());
        }
    }

    iobuf write_stop() &&;

private:
    void write_field_header(int16_t field_id, field_type type);

    void write_short_form_field_header(uint8_t delta, field_type type);

    void write_long_form_field_header(field_type type, int16_t field_id);

    iobuf _buf;
    int16_t _current_field_id = 0;
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

    template<typename T>
    void write_element(T val) {
        if constexpr (std::is_same_v<T, iobuf>) {
            _buf.append(std::move(val));
        } else {
            _buf.append(val.data(), val.size());
        }
    }

    iobuf finish() &&;

private:
    void write_short_form_field_header(uint8_t size, field_type type);

    void write_long_form_field_header(field_type type, size_t field_id);

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

} // namespace serde::thrift
