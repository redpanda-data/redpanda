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
    b.prepend(ss::temporary_buffer<char>(
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

} // namespace serde::thrift
