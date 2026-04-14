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

#include "serde/thrift/compact.h"

#include "utils/vint.h"

#include <limits>
#include <stdexcept>

namespace serde::thrift {

namespace {

void write_byte(iobuf& buf, uint8_t v) { buf.append(&v, 1); }

template<typename IntType>
void write_uvint(iobuf& buf, IntType v) {
    bytes b = unsigned_vint::to_bytes(v);
    buf.append(b.data(), b.size());
}

template<typename IntType>
void write_svint(iobuf& buf, IntType v) {
    bytes b = vint::to_bytes(v);
    buf.append(b.data(), b.size());
}

} // namespace

void struct_encoder::write_field(field_id id, field_type type, iobuf val) {
    write_field_header(id, type);
    _buf.append(std::move(val));
}

void struct_encoder::write_field(field_id id, field_type type, bytes val) {
    write_field_header(id, type);
    _buf.append(val.data(), val.size());
}

iobuf struct_encoder::write_stop() && {
    write_byte(_buf, 0);
    return std::move(_buf);
}

void struct_encoder::write_field_header(field_id id, field_type type) {
    constexpr int16_t max_short_form_delta = 16;
    if (id > _last_field_id && id - _last_field_id < max_short_form_delta) {
        auto delta = id - _last_field_id;
        write_short_form_field_header(static_cast<uint8_t>(delta), type);
    } else {
        write_long_form_field_header(type, id);
    }
    _last_field_id = id;
}

void struct_encoder::write_short_form_field_header(
  uint8_t delta, field_type type) {
    auto header_byte = static_cast<uint8_t>(type);
    header_byte |= static_cast<uint8_t>(delta << 4U);
    write_byte(_buf, header_byte);
}

void struct_encoder::write_long_form_field_header(
  field_type type, field_id id) {
    write_byte(_buf, static_cast<uint8_t>(type));
    write_svint<int16_t>(_buf, id());
}

bytes encode_string(std::string_view str) {
    auto b = unsigned_vint::to_bytes(str.size());
    b.reserve(b.size() + str.size());
    for (const char c : str) {
        b.push_back(c);
    }
    return b;
}

iobuf encode_binary(iobuf b) {
    if (b.size_bytes() > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("encoded thrift binary value too big");
    }
    auto len = unsigned_vint::to_bytes(b.size_bytes());
    b.prepend(
      ss::temporary_buffer<char>(
        // NOLINTNEXTLINE(*reinterpret-cast*)
        reinterpret_cast<const char*>(len.data()),
        len.size()));
    return b;
}

list_encoder::list_encoder(size_t size, field_type type) {
    constexpr size_t max_short_form_size = 15;
    if (size < max_short_form_size) {
        write_short_form_field_header(static_cast<uint8_t>(size), type);
    } else {
        write_long_form_field_header(type, size);
    }
}

void list_encoder::write_element(iobuf val) { _buf.append(std::move(val)); }

void list_encoder::write_element(bytes val) {
    _buf.append(val.data(), val.size());
}

iobuf list_encoder::finish() && { return std::move(_buf); }

void list_encoder::write_short_form_field_header(
  uint8_t size, field_type type) {
    auto header_byte = static_cast<uint8_t>(type);
    header_byte |= static_cast<uint8_t>(size << 4U);
    write_byte(_buf, header_byte);
}

void list_encoder::write_long_form_field_header(
  field_type type, size_t field_id) {
    constexpr uint8_t long_form_marker = 0b11110000U;
    write_byte(_buf, static_cast<uint8_t>(type) | long_form_marker);
    write_uvint<int32_t>(_buf, static_cast<int32_t>(field_id));
}

struct_decoder::struct_decoder(iobuf_parser_base& parser)
  : _parser(parser) {}

std::optional<struct_decoder::field_header>
struct_decoder::read_field_header() {
    auto byte = _parser.consume_type<uint8_t>();
    if (byte == 0) {
        return std::nullopt;
    }
    auto type = static_cast<field_type>(byte & 0x0FU);
    auto delta = static_cast<uint8_t>(byte >> 4U);
    field_id id;
    if (delta != 0) {
        id = field_id(_last_field_id() + delta);
    } else {
        id = field_id(decode_i16(_parser));
    }
    _last_field_id = id;
    // Booleans are encoded in the type byte itself — no additional value bytes.
    // The caller should check the type for boolean_true/boolean_false rather
    // than reading a separate value.
    return field_header{id, type};
}

void struct_decoder::skip_field(field_type type) {
    switch (type) {
    case field_type::boolean_true:
    case field_type::boolean_false:
        break;
    case field_type::i8:
        _parser.skip(1);
        break;
    case field_type::i16:
        decode_i16(_parser);
        break;
    case field_type::i32:
        decode_i32(_parser);
        break;
    case field_type::i64:
        decode_i64(_parser);
        break;
    case field_type::f64:
        _parser.skip(8);
        break;
    case field_type::binary:
        decode_binary(_parser);
        break;
    case field_type::uuid:
        _parser.skip(16);
        break;
    case field_type::list:
    case field_type::set: {
        list_decoder list(_parser);
        for (size_t i = 0; i < list.size(); ++i) {
            skip_field(list.element_type());
        }
        break;
    }
    case field_type::map: {
        auto [count, _] = _parser.read_unsigned_varint();
        if (count == 0) {
            break;
        }
        auto kv_byte = _parser.consume_type<uint8_t>();
        auto key_type = static_cast<field_type>(kv_byte >> 4U);
        auto val_type = static_cast<field_type>(kv_byte & 0x0FU);
        for (size_t i = 0; i < count; ++i) {
            skip_field(key_type);
            skip_field(val_type);
        }
        break;
    }
    case field_type::structure: {
        struct_decoder nested(_parser);
        while (auto hdr = nested.read_field_header()) {
            nested.skip_field(hdr->type);
        }
        break;
    }
    }
}

list_decoder::list_decoder(iobuf_parser_base& parser) {
    auto byte = parser.consume_type<uint8_t>();
    _type = static_cast<field_type>(byte & 0x0FU);
    auto size_nibble = static_cast<uint8_t>(byte >> 4U);
    constexpr uint8_t long_form_marker = 0x0FU;
    if (size_nibble == long_form_marker) {
        auto [val, _] = parser.read_unsigned_varint();
        _size = val;
    } else {
        _size = size_nibble;
    }
}

field_type list_decoder::element_type() const { return _type; }

size_t list_decoder::size() const { return _size; }

int16_t decode_i16(iobuf_parser_base& parser) {
    auto [val, _] = parser.read_varlong();
    if (
      val < std::numeric_limits<int16_t>::min()
      || val > std::numeric_limits<int16_t>::max()) {
        throw std::out_of_range("thrift i16 value out of range");
    }
    return static_cast<int16_t>(val);
}

int32_t decode_i32(iobuf_parser_base& parser) {
    auto [val, _] = parser.read_varlong();
    if (
      val < std::numeric_limits<int32_t>::min()
      || val > std::numeric_limits<int32_t>::max()) {
        throw std::out_of_range("thrift i32 value out of range");
    }
    return static_cast<int32_t>(val);
}

int64_t decode_i64(iobuf_parser_base& parser) {
    auto [val, _] = parser.read_varlong();
    return val;
}

ss::sstring decode_string(iobuf_parser_base& parser) {
    auto [len, _] = parser.read_unsigned_varint();
    return parser.read_string_unsafe(len);
}

iobuf decode_binary(iobuf_parser_base& parser) {
    auto [len, _] = parser.read_unsigned_varint();
    return parser.copy(len);
}

} // namespace serde::thrift
