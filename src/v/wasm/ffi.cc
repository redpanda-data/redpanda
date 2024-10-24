/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "ffi.h"

#include "utils/vint.h"

#include <algorithm>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string_view>
#include <valarray>

namespace wasm::ffi {

std::string_view array_as_string_view(array<uint8_t> arr) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return {reinterpret_cast<char*>(arr.data()), arr.size()};
}

void sizer::append(std::string_view s) { _offset += s.size(); }
void sizer::append(bytes_view s) { _offset += s.size(); }
void sizer::append(const iobuf& b) { _offset += b.size_bytes(); }
void sizer::append_with_length(std::string_view s) {
    append(s.size());
    append(s);
}
void sizer::append_with_length(const iobuf& b) {
    append(b.size_bytes());
    append(b);
}
void sizer::append(uint32_t v) { _offset += vint::vint_size(v); }
void sizer::append(int32_t v) { _offset += vint::vint_size(v); }
void sizer::append(uint64_t v) { _offset += vint::vint_size(int64_t(v)); }
void sizer::append(int64_t v) { _offset += vint::vint_size(v); }
void sizer::append_byte(uint8_t) { ++_offset; }

writer::writer(array<uint8_t> buf)
  : _tmp(bytes::initialized_zero{}, vint::max_length)
  , _output(buf) {}

void writer::append_with_length(const iobuf& b) {
    append(b.size_bytes());
    append(b);
}
void writer::append_with_length(std::string_view s) {
    append(s.size());
    append(s);
}
void writer::append(std::string_view s) {
    ensure_size(s.size());
    std::copy(s.cbegin(), s.cend(), &_output[_offset]);
    _offset += s.size();
}
void writer::append(bytes_view s) {
    ensure_size(s.size());
    std::copy(s.cbegin(), s.cend(), &_output[_offset]);
    _offset += s.size();
}
void writer::append(const iobuf& b) {
    ensure_size(b.size_bytes());
    iobuf::iterator_consumer consumer(b.begin(), b.end());
    consumer.consume_to(b.size_bytes(), &_output[_offset]);
    _offset += b.size_bytes();
}

namespace {
template<typename T>
size_t append_integer(bytes tmp, T v, array<uint8_t> output) {
    // We create _tmp to be `vint::max_length` so this will always fit.
    size_t amt = vint::serialize(int64_t(v), tmp.data());
    if (amt > output.size()) {
        throw std::out_of_range(ss::format(
          "ffi::array buffer too small {} > {}", amt, output.size()));
    }
    std::copy_n(tmp.data(), amt, output.data());
    return amt;
}
} // namespace
void writer::append(uint32_t v) {
    _offset += append_integer(_tmp, v, slice_remainder());
}
void writer::append(int32_t v) {
    _offset += append_integer(_tmp, v, slice_remainder());
}
void writer::append(uint64_t v) {
    _offset += append_integer(_tmp, v, slice_remainder());
}
void writer::append(int64_t v) {
    _offset += append_integer(_tmp, v, slice_remainder());
}
void writer::append_byte(uint8_t v) {
    ensure_size(1);
    _output[_offset++] = v;
}

void writer::ensure_size(size_t size) {
    auto remainder = slice_remainder();
    if (size > remainder.size()) {
        throw std::out_of_range(ss::format(
          "ffi::array buffer too small {} > {}, total: {}",
          size,
          remainder.size(),
          _output.size()));
    }
}
array<uint8_t> writer::slice_remainder() { return _output.subspan(_offset); }

reader::reader(ffi::array<uint8_t> buf)
  : _input(buf) {}
ss::sstring reader::read_string(size_t size) {
    return ss::sstring(read_string_view(size));
}
std::string_view reader::read_string_view(size_t size) {
    auto r = slice_remainder();
    if (r.size() < size) {
        throw std::out_of_range(ss::format(
          "ffi::array buffer too small {} > {}, total: {}",
          size,
          r.size(),
          _input.size()));
    }
    auto sv = array_as_string_view(slice_remainder()).substr(0, size);
    _offset += size;
    return sv;
}
iobuf reader::read_iobuf(size_t size) {
    auto r = slice_remainder();
    if (r.size() < size) {
        throw std::out_of_range(ss::format(
          "ffi::array buffer too small {} > {}, total: {}",
          size,
          r.size(),
          _input.size()));
    }
    iobuf b;
    b.append(r.data(), size);
    _offset += size;
    return b;
}
iobuf reader::read_sized_iobuf() {
    int64_t size = read_varint();
    return read_iobuf(size);
}
ss::sstring reader::read_sized_string() {
    int64_t size = read_varint();
    return read_string(size);
}
std::string_view reader::read_sized_string_view() {
    int64_t size = read_varint();
    return read_string_view(size);
}
int64_t reader::read_varint() {
    auto r = slice_remainder();
    auto [v, sz] = vint::deserialize(std::span<uint8_t>{r.data(), r.size()});
    _offset += sz;
    return v;
}
uint8_t reader::read_byte() {
    auto r = slice_remainder();
    if (r.empty()) {
        throw std::out_of_range("ffi::array buffer empty, expected 1 byte");
    }
    ++_offset;
    return r.front();
}

size_t reader::remaining_bytes() const { return slice_remainder().size(); }

array<uint8_t> reader::slice_remainder() const {
    return _input.subspan(_offset);
}

std::ostream& operator<<(std::ostream& o, val_type vt) {
    switch (vt) {
    case val_type::i32:
        o << "i32";
        break;
    case val_type::i64:
        o << "i64";
        break;
    }
    return o;
}

} // namespace wasm::ffi
