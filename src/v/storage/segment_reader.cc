// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_reader.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "ssx/future-util.h"
#include "storage/logger.h"
#include "storage/segment_utils.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

namespace storage {

segment_reader::segment_reader(
  segment_full_path path,
  size_t buffer_size,
  unsigned read_ahead,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) noexcept
  : _path(std::move(path))
  , _buffer_size(buffer_size)
  , _read_ahead(read_ahead)
  , _sanitizer_config(std::move(ntp_sanitizer_config)) {}

segment_reader::~segment_reader() noexcept {
    if (!_streams.empty() || _data_file_refcount > 0) {
        vlog(
          stlog.warn,
          "Dropping segment_reader while handles exist on file {}",
          _path);
    }

    for (auto& i : _streams) {
        i._parent = nullptr;
    }

    _streams.clear();
}

ss::future<> segment_reader::load_size() {
    ss::gate::holder guard{_gate};

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

    ss::gate::holder guard{_gate};

    auto handle = co_await get();
    handle.set_stream(make_file_input_stream(
      _data_file, pos, _file_size - pos, std::move(options)));
    co_return std::move(handle);
}

ss::future<segment_reader_handle> segment_reader::get() {
    vlog(
      stlog.trace,
      "::get segment file {}, refcount={}",
      _path,
      _data_file_refcount);
    // Lock to prevent double-opens
    auto units = co_await _open_lock.get_units();
    if (!_data_file) {
        vlog(stlog.debug, "Opening segment file {}", _path);
        _data_file = co_await internal::make_reader_handle(
          std::filesystem::path(_path), _sanitizer_config);
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
      _path,
      _data_file_refcount);
    vassert(_data_file_refcount > 0, "bad put() on {}", _path);
    _data_file_refcount--;
    if (_data_file && _data_file_refcount == 0) {
        vlog(stlog.debug, "Closing segment file {}", _path);
        // Note: a get() can now come in and open a fresh file handle: this
        // means we strictly-speaking can consume >1 file descriptors from one
        // segment_reader, but it's a rare+transient state.
        auto data_file = std::exchange(_data_file, ss::file{});
        co_await data_file.close();
    }
}

ss::future<struct stat> segment_reader::stat() {
    ss::gate::holder guard{_gate};

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

    ss::gate::holder guard{_gate};
    auto handle = co_await get();
    handle.set_stream(make_file_input_stream(
      _data_file, pos_begin, pos_end - pos_begin, std::move(options)));
    co_return handle;
}

ss::future<> segment_reader::truncate(size_t n) {
    ss::gate::holder guard{_gate};

    _file_size = n;
    return ss::open_file_dma(ss::sstring(_path), ss::open_flags::rw)
      .then([n](ss::file f) {
          return f.truncate(n)
            .then([f]() mutable { return f.close(); })
            .finally([f] {});
      });
}

ss::future<> segment_reader::close() {
    if (!_gate.is_closed()) {
        co_await _gate.close();
    }
    if (_data_file) {
        co_return co_await _data_file.close();
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
        vlog(stlog.trace, "Backgrounding put to {}", _parent->_path);
        ssx::background = _parent->put();
    }
    _stream = std::exchange(rhs._stream, std::nullopt);
    _parent = std::exchange(rhs._parent, nullptr);
    _hook.swap_nodes(rhs._hook);
}

concat_segment_data_source_impl::concat_segment_data_source_impl(
  std::vector<ss::lw_shared_ptr<segment>> segments,
  size_t start_pos,
  size_t end_pos,
  ss::io_priority_class priority_class)
  : _segments{std::move(segments)}
  , _current_pos{_segments.begin()}
  , _start_pos{start_pos}
  , _end_pos{end_pos}
  , _priority_class{priority_class}
  , _name{"uninitialized"} {}

ss::future<ss::temporary_buffer<char>> concat_segment_data_source_impl::get() {
    ss::gate::holder guard{_gate};
    if (!_current_stream) {
        co_await next_stream();
    }

    ss::temporary_buffer<char> buf = co_await _current_stream->read();
    while (buf.empty() && _current_pos != _segments.end()) {
        co_await next_stream();
        buf = co_await _current_stream->read();
    }

    co_return buf;
}

ss::future<> concat_segment_data_source_impl::next_stream() {
    if (_current_stream) {
        vlog(
          stlog.trace,
          "closing stream for current segment {} before switching to next "
          "segment",
          _name);
        co_await _current_stream->close();
    }

    if (_current_handle) {
        vlog(
          stlog.trace,
          "closing handle for current segment {} before switching to next "
          "segment",
          _name);
        co_await _current_handle->close();
    }

    vlog(
      stlog.trace,
      "opening stream for segment index {}, total segments: {}",
      std::distance(_segments.begin(), _current_pos),
      _segments.size());
    auto segment = *_current_pos;
    size_t start = 0;
    size_t end = segment->file_size();

    if (_current_pos == _segments.begin()) {
        start = _start_pos;
    }

    if (_current_pos == std::prev(_segments.end())) {
        end = _end_pos;
    }

    _name = segment->filename();
    _current_handle = co_await segment->reader().data_stream(
      start, end, _priority_class);
    _current_stream = _current_handle->take_stream();

    _current_pos++;

    vlog(stlog.trace, "opened segment {}", _name);
    co_return;
}

ss::future<> concat_segment_data_source_impl::close() {
    co_await _gate.close();
    if (_current_stream) {
        vlog(stlog.trace, "closing stream for segment {}", _name);
        co_await _current_stream->close();
    }

    if (_current_handle) {
        vlog(stlog.trace, "closing handle for segment {}", _name);
        co_await _current_handle->close();
    }
    co_return;
}

concat_segment_reader_view::concat_segment_reader_view(
  std::vector<ss::lw_shared_ptr<segment>> segments,
  size_t start_pos,
  size_t end_pos,
  ss::io_priority_class priority_class)
  : _stream(ss::data_source{std::make_unique<concat_segment_data_source_impl>(
      std::move(segments), start_pos, end_pos, priority_class)}) {}

ss::input_stream<char> concat_segment_reader_view::take_stream() {
    auto r = std::move(_stream.value());
    _stream.reset();
    return r;
}

ss::future<> concat_segment_reader_view::close() {
    if (_stream) {
        co_return co_await _stream->close();
    }
    co_return;
}

} // namespace storage
