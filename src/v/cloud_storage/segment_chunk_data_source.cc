
/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_chunk_data_source.h"

#include "cloud_storage/remote_segment.h"

namespace cloud_storage {

chunk_data_source_impl::chunk_data_source_impl(
  segment_chunks& chunks,
  remote_segment& segment,
  kafka::offset start,
  kafka::offset end,
  int64_t begin_stream_at,
  ss::file_input_stream_options stream_options,
  std::optional<uint16_t> prefetch_override)
  : _chunks(chunks)
  , _segment(segment)
  , _first_chunk_start(_segment.get_chunk_start_for_kafka_offset(start))
  , _last_chunk_start(_segment.get_chunk_start_for_kafka_offset(end))
  , _begin_stream_at{begin_stream_at - _first_chunk_start}
  , _current_chunk_start(_first_chunk_start)
  , _stream_options(std::move(stream_options))
  , _rtc{_as}
  , _ctxlog{cst_log, _rtc, _segment.get_segment_path()().native()}
  , _prefetch_override{prefetch_override} {
    vlog(
      _ctxlog.trace,
      "chunk data source initialized with file position {} to {}",
      _first_chunk_start,
      _last_chunk_start);
    _chunks.register_readers(_current_chunk_start, _last_chunk_start);
}

chunk_data_source_impl::~chunk_data_source_impl() {
    vassert(
      !_current_stream.has_value(),
      "stream not closed before destroying data source");
    vlog(_ctxlog.debug, "chunk data source destroyed");
}

ss::future<ss::temporary_buffer<char>> chunk_data_source_impl::get() {
    auto g = _gate.hold();

    if (!_current_stream) {
        co_await load_stream_for_chunk(_current_chunk_start);
        vassert(
          _current_stream.has_value(),
          "cannot read without stream for segment {}, current chunk id: {}",
          _segment.get_segment_path(),
          _current_chunk_start);
    }

    auto buf = co_await _current_stream->read();
    while (buf.empty() && _current_chunk_start < _last_chunk_start) {
        _current_chunk_start = _chunks.get_next_chunk_start(
          _current_chunk_start);
        co_await load_stream_for_chunk(_current_chunk_start);
        buf = co_await _current_stream->read();
    }

    co_return buf;
}

ss::future<>
chunk_data_source_impl::load_chunk_handle(chunk_start_offset_t chunk_start) {
    try {
        vlog(
          _ctxlog.debug,
          "Hydrating chunk {} with prefetch {}",
          chunk_start,
          _prefetch_override);
        _current_data_file = co_await _chunks.hydrate_chunk(
          chunk_start, _prefetch_override);
    } catch (const ss::abort_requested_exception& ex) {
        throw;
    } catch (const ss::gate_closed_exception& ex) {
        throw;
    } catch (const std::exception& ex) {
        vlog(
          _ctxlog.warn,
          "failed to hydrate chunk starting at {}, error: {}",
          chunk_start,
          ex);
        throw;
    }
}

ss::future<> chunk_data_source_impl::load_stream_for_chunk(
  chunk_start_offset_t chunk_start) {
    vlog(_ctxlog.debug, "loading stream for chunk starting at {}", chunk_start);

    std::exception_ptr eptr;

    try {
        co_await load_chunk_handle(chunk_start);
    } catch (...) {
        eptr = std::current_exception();
        vlog(
          _ctxlog.debug,
          "Hydrating chunk {} failed with error {}",
          chunk_start,
          eptr);
    }

    if (eptr) {
        co_await maybe_close_stream();
        vlog(
          _ctxlog.debug,
          "Closed stream after error {} while hydrating chunk start {}",
          eptr,
          chunk_start);
        std::rethrow_exception(eptr);
    }

    // Decrement the required_by_readers_in_future count by 1, we have acquired
    // the file handle here. Once we are done with this handle, if it is not
    // shared with any other data source, it should be eligible for trimming.
    _chunks.mark_acquired_and_update_stats(
      _current_chunk_start, _last_chunk_start);

    if (_current_stream) {
        co_await _current_stream->close();
    }

    // The first read of the data source begins at _begin_stream_at. This is
    // necessary because the remote segment reader which uses this data source
    // sets a delta before reading, and the remote segment consumer which
    // consumes data from this source expects all offsets to be below the delta.
    // Setting the appropriate start offset on the file stream makes sure that
    // we do not break that assertion.
    uint64_t begin = 0;
    if (_current_chunk_start == _first_chunk_start) {
        begin = _begin_stream_at;
    }
    vlog(
      _ctxlog.trace,
      "creating stream for chunk at {}, begin at {}",
      _current_chunk_start,
      begin);

    _current_stream = ss::make_file_input_stream(
      *_current_data_file, begin, _stream_options);
}

ss::future<> chunk_data_source_impl::close() {
    co_await _gate.close();
    co_await maybe_close_stream();
}

ss::future<> chunk_data_source_impl::maybe_close_stream() {
    if (_current_stream) {
        co_await _current_stream->close();
        _current_stream = std::nullopt;
    }
}

} // namespace cloud_storage
