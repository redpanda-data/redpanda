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

namespace serde::thrift {

namespace {

template<typename IntType>
void write_be(iobuf& buf, IntType v) {
    IntType be_form = ss::cpu_to_be(v);
    // NOLINTNEXTLINE(*reinterpret-cast*)
    buf.append(reinterpret_cast<const char*>(&be_form), sizeof(v));
}

template<typename IntType>
void write_uvint(iobuf& buf, IntType v) {
    bytes b = unsigned_vint::to_bytes(v);
    buf.append(b.data(), b.size());
}

} // namespace

iobuf struct_encoder::write_stop() && {
    write_be<uint8_t>(_buf, 0);
    return std::move(_buf);
}

void struct_encoder::write_field_header(int16_t field_id, field_type type) {
    auto delta = field_id - _current_field_id;
    constexpr int16_t max_short_form_delta = 16;
    if (delta > 0 && delta < max_short_form_delta) {
        write_short_form_field_header(static_cast<uint8_t>(delta), type);
    } else {
        write_long_form_field_header(type, field_id);
    }
    _current_field_id = field_id;
}

void struct_encoder::write_short_form_field_header(
  uint8_t delta, field_type type) {
    auto header_byte = static_cast<uint8_t>(type);
    header_byte |= static_cast<uint8_t>(delta << 4U);
    write_be<uint8_t>(_buf, header_byte);
}

void struct_encoder::write_long_form_field_header(
  field_type type, int16_t field_id) {
    write_be<uint8_t>(_buf, static_cast<uint8_t>(type));
    write_uvint<int16_t>(_buf, field_id);
}

bytes encode_string(std::string_view str) {
    auto b = unsigned_vint::to_bytes(static_cast<int64_t>(str.size()));
    b.reserve(b.size() + str.size());
    for (const char c : str) {
        b.push_back(c);
    }
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

iobuf list_encoder::finish() && { return std::move(_buf); }

void list_encoder::write_short_form_field_header(
  uint8_t size, field_type type) {
    auto header_byte = static_cast<uint8_t>(type);
    header_byte |= static_cast<uint8_t>(size << 4U);
    write_be<uint8_t>(_buf, header_byte);
}

void list_encoder::write_long_form_field_header(
  field_type type, size_t field_id) {
    constexpr uint8_t long_form_marker = 0b11110000U;
    write_be<uint8_t>(_buf, static_cast<uint8_t>(type) | long_form_marker);
    write_uvint<int32_t>(_buf, static_cast<int32_t>(field_id));
}

} // namespace serde::thrift
