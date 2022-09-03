// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_reader.h"

#include "ssx/future-util.h"
#include "storage/logger.h"
#include "storage/segment_utils.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

namespace storage {

segment_reader::segment_reader(
  ss::sstring filename,
  size_t buffer_size,
  unsigned read_ahead,
  debug_sanitize_files sanitize) noexcept
  : _filename(std::move(filename))
  , _buffer_size(buffer_size)
  , _read_ahead(read_ahead)
  , _sanitize(sanitize) {}

segment_reader::~segment_reader() noexcept {
    if (!_streams.empty() || _data_file_refcount > 0) {
        vlog(
          stlog.warn,
          "Dropping segment_reader while handles exist on file {}",
          _filename);
    }

    for (auto& i : _streams) {
        i.detach();
    }

    _streams.clear();
}

segment_reader::segment_reader(segment_reader&& rhs) noexcept
  : _filename(std::move(rhs._filename))
  , _data_file(std::move(rhs._data_file))
  , _data_file_refcount(rhs._data_file_refcount)
  , _file_size(rhs._file_size)
  , _buffer_size(rhs._buffer_size)
  , _read_ahead(rhs._read_ahead)
  , _sanitize(rhs._sanitize) {
    for (auto& i : rhs._streams) {
        i._parent = this;
        i._hook.unlink();
        _streams.push_back(i);
    }
}

ss::future<> segment_reader::load_size() {
    auto s = co_await stat();
    set_file_size(s.st_size);
};

ss::future<segment_reader_handle>
segment_reader::data_stream(size_t pos, const ss::io_priority_class pc) {
    vassert(
      pos <= _file_size,
      "cannot read negative bytes. Asked to read at position: '{}' - {}",
      pos,
      *this);

    // note: this file _must_ be open in `ro` mode only. Seastar uses dma
    // files with no shared buffer cache around them. When we use a writer
    // w/ dma at the same time as the reader, we need a way to synchronize
    // filesytem metadata. In order to prevent expensive synchronization
    // primitives fsyncing both *reads* and *writes* we open this file in ro
    // mode and if raft requires truncation, we open yet-another handle w/
    // rw mode just for the truncation which gives us all the benefits of
    // preventing x-file synchronization This is fine, because truncation to
    // sealed segments are supposed to be very rare events. The hotpath of
    // truncating the appender, is optimized.

    ss::file_input_stream_options options;
    options.buffer_size = _buffer_size;
    options.io_priority_class = pc;
    options.read_ahead = _read_ahead;

    auto handle = co_await get();
    handle.set_stream(make_file_input_stream(
      _data_file, pos, _file_size - pos, std::move(options)));
    co_return std::move(handle);
}

ss::future<segment_reader_handle> segment_reader::get() {
    vlog(
      stlog.trace,
      "::get segment file {}, refcount={}",
      _filename,
      _data_file_refcount);
    // Lock to prevent double-opens
    auto units = co_await _open_lock.get_units();
    if (!_data_file) {
        vlog(stlog.debug, "Opening segment file {}", _filename);
        _data_file = co_await internal::make_reader_handle(
          std::filesystem::path(_filename), _sanitize);
    }

    _data_file_refcount++;
    auto handle = segment_reader_handle(this);
    co_return handle;
}

/**
 * Release a reference to the file.
 *
 * This function may sleep, but will not access any memory
 * belonging to the segment_reader after that: i.e. it is safe
 * to deallocate or move the segment_reader while waiting for
 * the future from put() to complete.
 */
ss::future<> segment_reader::put() {
    vlog(
      stlog.trace,
      "::put segment file {}, refcount={}",
      _filename,
      _data_file_refcount);
    vassert(_data_file_refcount > 0, "bad put() on {}", _filename);
    _data_file_refcount--;
    if (_data_file && _data_file_refcount == 0) {
        vlog(stlog.debug, "Closing segment file {}", _filename);
        // Note: a get() can now come in and open a fresh file handle: this
        // means we strictly-speaking can consume >1 file descriptors from one
        // segment_reader, but it's a rare+transient state.
        auto data_file = std::exchange(_data_file, ss::file{});
        co_await data_file.close();
    }
}

ss::future<struct stat> segment_reader::stat() {
    auto handle = co_await get();
    auto r = co_await _data_file.stat();
    co_await handle.close();
    co_return r;
}

ss::future<segment_reader_handle> segment_reader::data_stream(
  size_t pos_begin, size_t pos_end, const ss::io_priority_class pc) {
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

    auto handle = co_await get();
    handle.set_stream(make_file_input_stream(
      _data_file, pos_begin, pos_end - pos_begin, std::move(options)));
    co_return handle;
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

ss::future<> segment_reader::close() {
    if (_data_file) {
        return _data_file.close();
    } else {
        return ss::now();
    }
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

segment_reader_handle::segment_reader_handle(segment_reader* parent)
  : _parent(parent) {
    _parent->_streams.push_back(*this);
}

segment_reader_handle::~segment_reader_handle() {
    vassert(!_stream.has_value(), "Must close before destroying");
    vassert(_parent == nullptr, "Must close before destroying");
}

ss::future<> segment_reader_handle::close() {
    if (_stream) {
        co_await _stream.value().close();
        _stream = std::nullopt;
    }
    _hook.unlink();

    if (_parent) {
        co_await _parent->put();
        _parent = nullptr;
    }
}

void segment_reader_handle::operator=(segment_reader_handle&& rhs) noexcept {
    assert(&rhs != this);

    if (_parent) {
        // Where we move-assign a handle into a handle that's already,
        // it's to reset a stream on the same underlying segment_reader,
        // so we can be certain that the put() will not reduce the
        // file handle refcount to zero, as `rhs` holds a reference.
        vlog(stlog.trace, "Backgrounding put to {}", _parent->_filename);
        ssx::background = _parent->put();
    }
    _stream = std::exchange(rhs._stream, std::nullopt);
    _parent = std::exchange(rhs._parent, nullptr);
    _hook.swap_nodes(rhs._hook);
}

} // namespace storage
