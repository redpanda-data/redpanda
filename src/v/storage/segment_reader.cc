// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_reader.h"

#include "vassert.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

namespace storage {

segment_reader::segment_reader(
  ss::sstring filename,
  ss::file data_file,
  size_t file_size,
  size_t buffer_size,
  unsigned read_ahead) noexcept
  : _filename(std::move(filename))
  , _data_file(std::move(data_file))
  , _file_size(file_size)
  , _buffer_size(buffer_size)
  , _read_ahead(read_ahead) {}

ss::input_stream<char>
segment_reader::data_stream(size_t pos, const ss::io_priority_class& pc) {
    vassert(
      pos <= _file_size,
      "cannot read negative bytes. Asked to read at position: '{}' - {}",
      pos,
      *this);
    ss::file_input_stream_options options;
    options.buffer_size = _buffer_size;
    options.io_priority_class = pc;
    options.read_ahead = _read_ahead;
    return make_file_input_stream(
      _data_file, pos, _file_size - pos, std::move(options));
}

ss::input_stream<char> segment_reader::data_stream(
  size_t pos_begin, size_t pos_end, const ss::io_priority_class& pc) {
    vassert(
      pos_begin <= _file_size,
      "cannot read negative bytes. Asked to read at positions: '{}-{}' - {}",
      pos_begin,
      pos_end,
      *this);
    vassert(
      pos_end >= pos_begin,
      "cannot read backward. Asked to read at positions: '{}-{}' - {}",
      pos_begin,
      pos_end,
      *this);
    ss::file_input_stream_options options;
    options.buffer_size = _buffer_size;
    options.io_priority_class = pc;
    options.read_ahead = _read_ahead;
    return make_file_input_stream(
      _data_file, pos_begin, pos_end - pos_begin, std::move(options));
}

ss::future<> segment_reader::truncate(size_t n) {
    _file_size = n;
    return ss::open_file_dma(_filename, ss::open_flags::rw)
      .then([n](ss::file f) {
          return f.truncate(n)
            .then([f]() mutable { return f.close(); })
            .finally([f] {});
      });
}

std::ostream& operator<<(std::ostream& os, const segment_reader& seg) {
    return os << "{" << seg.filename() << ", (" << seg.file_size()
              << " bytes)}";
}

std::ostream& operator<<(std::ostream& os, const segment_reader_ptr& seg) {
    if (seg) {
        return os << *seg;
    }
    return os << "{{log_segment: null}}";
}
} // namespace storage
