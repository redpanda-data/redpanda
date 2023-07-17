/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "ffi.h"

#include "utils/vint.h"

#include <boost/fusion/container/list/cons.hpp>

#include <algorithm>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string_view>
#include <valarray>

namespace wasm::ffi {

std::string_view array_as_string_view(array<uint8_t> arr) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return {reinterpret_cast<char*>(arr.raw()), arr.size()};
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

writer::writer(array<uint8_t> buf)
  : _tmp(vint::max_length, 0)
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
    size_t amt = vint::serialize(int64_t(v), tmp.data());
    if (amt > output.size()) {
        throw std::out_of_range(ss::format(
          "ffi::array buffer too small {} > {}", amt, output.size()));
    }
    std::copy_n(tmp.data(), amt, output.raw());
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
array<uint8_t> writer::slice_remainder() {
    return _output.slice(_offset, _output.size() - _offset);
}

reader::reader(ffi::array<uint8_t> buf)
  : _input(buf) {}
ss::sstring reader::read_string(size_t size) {
    auto r = slice_remainder();
    if (r.size() < size) {
        throw std::out_of_range(ss::format(
          "ffi::array buffer too small {} > {}, total: {}",
          size,
          r.size(),
          _input.size()));
    }
    ss::sstring s;
    auto sv = array_as_string_view(slice_remainder());
    s.append(sv.data(), size);
    _offset += size;
    return s;
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
    b.append(r.raw(), size);
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
int64_t reader::read_varint() {
    auto r = slice_remainder();
    auto [v, sz] = vint::deserialize(std::span<uint8_t>{r.raw(), r.size()});
    _offset += sz;
    return v;
}
array<uint8_t> reader::slice_remainder() {
    return _input.slice(_offset, _input.size() - _offset);
}

std::ostream& operator<<(std::ostream& o, val_type vt) {
    switch (vt) {
    case val_type::i32:
        o << "i32";
        break;
    case val_type::i64:
        o << "i64";
        break;
    case val_type::f32:
        o << "f32";
        break;
    case val_type::f64:
        o << "f64";
        break;
    }
    return o;
}

} // namespace wasm::ffi
