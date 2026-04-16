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

#include "bytes/ioarray.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"

#include <seastar/core/align.hh>
#include <seastar/core/byteorder.hh>

#include <sys/uio.h>

#include <cstring>
#include <iterator>
#include <stdexcept>

ioarray::ioarray(size_t size)
  : ioarray(uninitialized_t{}, size) {
    for (size_t offset = 0, i = 0; offset < size;
         offset += max_chunk_size, ++i) {
        _buffers[i] = ss::temporary_buffer<char>(
          std::min(max_chunk_size, size - offset));
    }
}

ioarray::ioarray(ioarray::uninitialized_t, size_t size)
  : _offset(0)
  , _size(size)
  , _buffers((size + max_chunk_size - 1) / max_chunk_size) {}

ioarray& ioarray::operator=(ioarray&& o) noexcept {
    if (this != &o) {
        this->~ioarray();
        new (this) ioarray(std::move(o));
    }
    return *this;
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray ioarray::aligned(size_t alignment, size_t size) {
    if (size == 0) {
        return ioarray{0};
    }
    dassert(
      (alignment & (alignment - 1)) == 0,
      "alignment {} must be a power of two",
      alignment);
    dassert(
      size % alignment == 0,
      "size {} must be a multiple of alignment {}",
      size,
      alignment);
    ioarray c{size};
    for (size_t offset = 0, i = 0; offset < size;
         offset += max_chunk_size, ++i) {
        size_t len = std::min(max_chunk_size, size - offset);
        auto tmpbuf = ss::temporary_buffer<char>::aligned(
          alignment, ss::align_up(len, alignment));
        c._buffers[i] = std::move(tmpbuf);
    }
    return c;
}

ioarray::string_view::operator ss::sstring() const {
    ss::sstring out(ss::sstring::initialized_later{}, size());
    char* it = out.begin();
    for (const auto& view : _views) {
        it = std::ranges::copy(view, it).out;
    }
    return out;
}

bool ioarray::string_view::operator==(std::string_view other) const {
    if (!other.starts_with(_views[0])) {
        return false;
    }
    other.remove_prefix(_views[0].size());
    return other == _views[1];
}

std::strong_ordering
ioarray::string_view::operator<=>(std::string_view other) const {
    auto cmp = _views[0] <=> other.substr(0, _views[0].size());
    if (cmp != std::strong_ordering::equal) {
        return cmp;
    }
    other.remove_prefix(_views[0].size());
    return _views[1] <=> other;
}

ioarray ioarray::concat(ioarray a, ioarray b) {
    if (a.empty()) {
        return b;
    }
    if (b.empty()) {
        return a;
    }
    // Fast path: when a's data ends on a chunk boundary and b has no offset,
    // we can move buffers directly since the indexing math is preserved.
    bool fast = b._offset == 0 && (a._offset + a._size) % max_chunk_size == 0;
    if (fast) {
        ioarray out(
          uninitialized_t{},
          (a._buffers.size() + b._buffers.size()) * max_chunk_size);
        out._offset = a._offset;
        out._size = a._size + b._size;
        size_t i = 0;
        for (auto& buf : a._buffers) {
            out._buffers[i++] = std::move(buf);
        }
        for (auto& buf : b._buffers) {
            out._buffers[i++] = std::move(buf);
        }
        return out;
    }
    // Slow path: copy both into a fresh ioarray.
    size_t total = a._size + b._size;
    ioarray out(total);
    size_t pos = 0;
    for (auto c : a.as_range()) {
        out[pos++] = c;
    }
    for (auto c : b.as_range()) {
        out[pos++] = c;
    }
    return out;
}

ioarray ioarray::copy_from(const iobuf& buf) {
    ioarray c(uninitialized_t{}, buf.size_bytes());
    iobuf_const_parser parser(buf);
    size_t i = 0;
    for (size_t offset = 0; offset < buf.size_bytes();
         offset += max_chunk_size) {
        size_t len = std::min(max_chunk_size, buf.size_bytes() - offset);
        auto tmpbuf = ss::temporary_buffer<char>(len);
        parser.consume_to(len, tmpbuf.get_write());
        c._buffers[i++] = std::move(tmpbuf);
    }
    return c;
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray ioarray::share(size_t pos, size_t length) {
    if (length == 0) {
        return {};
    }
    dassert(
      (pos + length) <= _size,
      "pos {} + length {} must be <= size {}",
      pos,
      length,
      _size);
    pos += _offset;
    size_t start = pos / max_chunk_size;
    size_t end = (pos + length + max_chunk_size - 1) / max_chunk_size;
    ioarray out(uninitialized_t{}, (end - start) * max_chunk_size);
    size_t i = 0;
    out._offset = pos - (start * max_chunk_size);
    out._size = length;
    size_t remaining_length = out._offset + out._size;
    for (auto& buf : std::span(_buffers).subspan(start, end - start)) {
        size_t buf_size = std::min(max_chunk_size, remaining_length);
        remaining_length -= buf_size;
        out._buffers[i++] = buf.share(0, buf_size);
    }
    return out;
}

iobuf ioarray::as_iobuf() {
    iobuf out;
    for (auto& buf : _buffers) {
        size_t offset = out.empty() ? _offset : 0;
        out.append(
          std::make_unique<iobuf::fragment>(
            buf.share(offset, buf.size() - offset)));
    }
    dassert(
      out.size_bytes() == _size,
      "as_iobuf size mismatch: expected {}, got {}",
      _size,
      out.size_bytes());
    return out;
}

// NOLINTNEXTLINE(*swappable-parameters*)
ioarray::string_view ioarray::read_string(size_t pos, size_t length) const {
    dassert(
      length <= max_chunk_size, "length {} must be <= max_chunk_size", length);
    dassert(
      (pos + length) <= _size,
      "pos {} + length {} must be <= size {}",
      pos,
      length,
      _size);
    pos += _offset;
    auto& buf = _buffers[pos / max_chunk_size];
    const char* it = buf.begin();
    std::advance(it, pos % max_chunk_size);
    if (std::cmp_greater_equal(std::distance(it, buf.end()), length)) {
        return string_view{{it, length}};
    }
    auto& next = _buffers[(pos / max_chunk_size) + 1];
    return string_view{
      {it, buf.end()}, {next.begin(), length - std::distance(it, buf.end())}};
}

std::vector<::iovec> ioarray::as_iovec() {
    std::vector<::iovec> iov;
    iov.reserve(_buffers.size());
    for (auto& buf : _buffers) {
        char* start = buf.get_write();
        size_t offset = iov.empty() ? _offset : 0;
        std::advance(start, offset);
        iov.emplace_back(start, buf.size() - offset);
    }
    return iov;
}

char& ioarray::operator[](size_t i) {
    dassert(i < _size, "i {} must be < size {}", i, _size);
    i += _offset;
    char* it = _buffers[i / max_chunk_size].get_write();
    std::advance(it, i % max_chunk_size);
    return *it;
}

char ioarray::operator[](size_t i) const {
    dassert(i < _size, "i {} must be < size {}", i, _size);
    i += _offset;
    return _buffers[i / max_chunk_size][i % max_chunk_size];
}

uint32_t ioarray::read_fixed32(size_t i) const {
    dassert(
      i <= _size && i + sizeof(uint32_t) <= _size,
      "i {} + sizeof(uint32_t) {} must be <= size {}",
      i,
      sizeof(uint32_t),
      _size);
    i += _offset;

    // Calculate which chunk we're in and the offset within that chunk
    size_t chunk_idx = i / max_chunk_size;
    size_t chunk_offset = i % max_chunk_size;

    // Fast path: direct unaligned load
    if (chunk_offset + sizeof(uint32_t) <= max_chunk_size) {
        const char* ptr = _buffers[chunk_idx].begin();
        std::advance(ptr, chunk_offset);
        uint32_t v = 0;
        std::memcpy(&v, ptr, sizeof(uint32_t));
        return ss::le_to_cpu(v);
    }

    // Slow path: crosses chunk boundary, read byte by byte
    auto read = [this](size_t o) -> uint32_t {
        return static_cast<uint8_t>(
          _buffers[o / max_chunk_size][o % max_chunk_size]);
    };
    uint32_t v = read(i);
    v |= read(++i) << 8u;
    v |= read(++i) << 16u;
    v |= read(++i) << 24u;
    return ss::le_to_cpu(v);
}

void ioarray::trim_back(size_t n) {
    dassert(n <= _size, "n {} must be <= size {}", n, _size);
    _size -= n;
    size_t i = _buffers.size() - 1;
    while (n > 0) {
        auto& last_buf = _buffers[i];
        size_t amt = std::min(n, last_buf.size());
        n -= amt;
        last_buf.trim(last_buf.size() - amt);
        if (last_buf.empty()) {
            --i;
        }
    }
    auto end = _buffers.begin();
    std::advance(end, i + 1);
    if (end == _buffers.end()) {
        return;
    }
    // Absl very annoyingly does not support move operators, so we have to do it
    // the hard way by manually destructing and reconstructing in-place.
    decltype(_buffers) replacement(
      std::make_move_iterator(_buffers.begin()), std::make_move_iterator(end));
    auto* buffers_ptr = &_buffers;
    buffers_ptr->~FixedArray();
    new (buffers_ptr) absl::FixedArray(std::move(replacement));
}

ioarray
ioarray::from_sized_buffers(std::span<ss::temporary_buffer<char>> bufs) {
    if (bufs.empty()) {
        return {};
    }
    ioarray out(uninitialized_t{}, bufs.size() * max_chunk_size);
    size_t i = 0, size = 0;
    for (auto& buf : bufs.subspan(0, bufs.size() - 1)) {
        if (buf.size() != max_chunk_size) {
            throw std::invalid_argument(
              fmt::format(
                "inner chunk size must be {}, got {}",
                max_chunk_size,
                buf.size()));
        }
        size += buf.size();
        out._buffers[i++] = buf.share();
    }
    size += bufs.back().size();
    out._buffers[i] = bufs.back().share();
    out._size = size;
    return out;
}
