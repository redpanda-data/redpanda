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

#include "lsm/io/readahead_file_reader.h"

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>

#include <algorithm>

namespace lsm::io {

namespace {
constexpr size_t alignment = ioarray::max_chunk_size;

size_t align_down(size_t v) { return ss::align_down(v, alignment); }

size_t align_up(size_t v) { return ss::align_up(v, alignment); }
} // namespace

readahead_file_reader::readahead_file_reader(
  random_access_file_reader* inner, size_t file_size, size_t readahead_size)
  : _inner(inner)
  , _file_size(file_size)
  , _readahead(readahead_size) {}

ss::future<ioarray> readahead_file_reader::read(size_t offset, size_t n) {
    if (
      _buf.size() > 0 && offset >= _file_off
      && offset + n <= _file_off + _buf.size()) {
        // Serve from buffer if fully contained (nothing more to read)
    } else if (
      _buf.size() > 0 && offset >= _file_off
      && offset < _file_off + _buf.size()) {
        // Partial buffer hit: request starts in the buffer but extends past it
        size_t missing_off = _file_off + _buf.size();
        size_t missing_end = std::min(
          align_up(offset + n + _readahead), _file_size);
        auto fresh = co_await _inner->read(
          missing_off, missing_end - missing_off);
        _buf = ioarray::concat(std::move(_buf), std::move(fresh));
    } else {
        // Buffer miss: issue an aligned read
        size_t aligned_off = align_down(offset);
        size_t read_end = std::min(
          align_up(offset + n + _readahead), _file_size);
        _buf = co_await _inner->read(aligned_off, read_end - aligned_off);
        _file_off = aligned_off;
    }
    // We've done all the reading we need to serve the read, now we can share
    // out the data the caller asked for.
    auto output = _buf.share(offset - _file_off, n);
    // Snip off the beginning of our buffer so the next read is ready
    size_t consumed = offset + n - _file_off;
    _buf = _buf.share(consumed, _buf.size() - consumed);
    _file_off = offset + n;
    co_return output;
}

ss::future<> readahead_file_reader::close() { co_return; }

fmt::iterator readahead_file_reader::format_to(fmt::iterator it) const {
    return _inner->format_to(it);
}

} // namespace lsm::io
