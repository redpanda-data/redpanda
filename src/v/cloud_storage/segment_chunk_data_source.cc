
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
#include "utils/gate_guard.h"

#include <seastar/util/defer.hh>

#include <utility>

namespace cloud_storage {

chunk_data_source_impl::chunk_data_source_impl(
  segment_chunks& chunks,
  remote_segment& segment,
  kafka::offset start,
  kafka::offset end,
  int64_t begin_stream_at,
  ss::file_input_stream_options stream_options,
  std::optional<uint16_t> prefetch_override,
  std::optional<std::reference_wrapper<remote_segment_batch_reader>> reader)
  : _chunks(chunks)
  , _segment(segment)
  , _first_chunk_start(_segment.get_chunk_start_for_kafka_offset(start))
  , _last_chunk_start(_segment.get_chunk_start_for_kafka_offset(end))
  , _begin_stream_at{begin_stream_at - _first_chunk_start}
  , _current_chunk_start(_first_chunk_start)
  , _stream_options(std::move(stream_options))
  , _rtc{_as}
  , _ctxlog{cst_log, _rtc, _segment.get_segment_path()().native()}
  , _prefetch_override{prefetch_override}
  , _attached_reader{reader} {
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
    gate_guard g{_gate};

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
        switch (_current_stream_t) {
        case stream_type::disk:
            vlog(
              _ctxlog.trace,
              "advancing _current_chunk_start from {} to {}",
              _current_chunk_start,
              _chunks.get_next_chunk_start(_current_chunk_start));
            _current_chunk_start = _chunks.get_next_chunk_start(
              _current_chunk_start);
            break;
        case stream_type::download:
            vlog(
              _ctxlog.trace,
              "advancing _current_chunk_start from {} to {}",
              _current_chunk_start,
              _last_download_end + 1);
            _current_chunk_start = _last_download_end + 1;
            break;
        default:
            vassert(false, "Unexpected stream type");
        }
        co_await load_stream_for_chunk(_current_chunk_start);
        buf = co_await _current_stream->read();
    }

    co_return buf;
}

ss::future<> chunk_data_source_impl::load_chunk_handle(
  chunk_start_offset_t chunk_start, eager_stream_ptr eager_stream) {
    try {
        _current_data_file = co_await _chunks.hydrate_chunk(
          chunk_start, _prefetch_override, eager_stream);
        // Decrement the required_by_readers_in_future count by 1, we have
        // acquired
        // the file handle here. Once we are done with this handle, if it is not
        // shared with any other data source, it should be eligible for
        // trimming.
        _chunks.mark_acquired_and_update_stats(
          _current_chunk_start, _last_chunk_start);
        vlog(_ctxlog.trace, "chunk handle loaded for {}", chunk_start);
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

ss::future<eager_stream_ptr> chunk_data_source_impl::maybe_load_eager_stream(
  chunk_start_offset_t chunk_start) {
    std::exception_ptr eptr;
    try {
        bool should_load_eager_stream = true;
        if (should_load_eager_stream) {
            auto p = ss::make_lw_shared<eager_chunk_stream>();
            _download_task.emplace(*this, chunk_start, p);
            _download_task->start();

            try {
                co_await p->wait_for_stream();
            } catch (const ss::condition_variable_timed_out&) {
                vlog(_ctxlog.info, "timed out while waiting for eager stream");
                p->state = eager_chunk_stream::stream_state::
                  download_cancelled_timeout;
            }

            switch (p->state) {
            case eager_chunk_stream::stream_state::ready:
                vassert(
                  p->stream.has_value(),
                  "stream state: {} but stream not loaded for {}",
                  p->state,
                  chunk_start);
                vlog(
                  _ctxlog.trace,
                  "chunk download in background for {}",
                  chunk_start);
                co_return std::move(p);
                break;
            case eager_chunk_stream::stream_state::awaiting_hydration:
                vassert(
                  false,
                  "invalid state: awaiting_hydration for {}",
                  chunk_start);
                break;
            case eager_chunk_stream::stream_state::in_wait_queue:
            case eager_chunk_stream::stream_state::chunk_in_cache:
            case eager_chunk_stream::stream_state::download_cancelled_timeout:
                vlog(
                  _ctxlog.trace,
                  "waiting for chunk download, state: {}",
                  p->state);
                co_await wait_for_download();
            }
        } else {
            co_await load_chunk_handle(chunk_start);
        }
    } catch (...) {
        eptr = std::current_exception();
    }

    if (eptr) {
        vlog(
          _ctxlog.warn,
          "error during load_stream_for_chunk: {} - {}",
          chunk_start,
          eptr);
        co_await maybe_close_stream();
        std::rethrow_exception(eptr);
    }

    co_return nullptr;
}

ss::future<> chunk_data_source_impl::load_stream_for_chunk(
  chunk_start_offset_t chunk_start) {
    vlog(_ctxlog.debug, "loading stream for chunk starting at {}", chunk_start);

    if (_download_task) {
        vlog(_ctxlog.debug, "waiting for pending download: {}", _download_task);
        co_await wait_for_download();
    }

    co_await maybe_close_stream();

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

    auto maybe_stream = co_await maybe_load_eager_stream(chunk_start);
    co_await set_current_stream(begin, std::move(maybe_stream));
}

ss::future<> chunk_data_source_impl::set_current_stream(
  uint64_t begin_at, eager_stream_ptr ecs) {
    if (ecs) {
        // TODO - this should be flipped if we switch to a disk based stream, or
        // more specifically when we close a transient stream. This change would
        // then need to be communicated back up to the reader so it can be
        // cached.
        if (_attached_reader) {
            _attached_reader->get().mark_transient(true);
        }
        vlog(
          _ctxlog.trace,
          "creating eager stream for chunk starting at {}, begin offset in "
          "chunk {}, stream ends "
          "at {}",
          _current_chunk_start,
          begin_at,
          ecs->last_offset);
        _current_stream_t = stream_type::download;
        _last_download_end = ecs->last_offset;
        _current_stream = std::move(ecs->stream.value());
        co_await skip_stream_to(begin_at);
    } else {
        vlog(
          _ctxlog.trace,
          "creating file stream for chunk starting at {}, begin offset in "
          "chunk {}",
          _current_chunk_start,
          begin_at);
        _current_stream_t = stream_type::disk;
        _current_stream = ss::make_file_input_stream(
          *_current_data_file, begin_at, _stream_options);
    }
}

ss::future<> chunk_data_source_impl::skip_stream_to(uint64_t begin) {
    uint64_t to_read = begin;
    while (to_read > 0) {
        const auto buf = co_await _current_stream->read_up_to(to_read);
        to_read -= buf.size();
        vlog(
          _ctxlog.trace,
          "remaining to_read = {}, current read = {}",
          to_read,
          buf.size());
    }
}

ss::future<> chunk_data_source_impl::close() {
    vlog(_ctxlog.trace, "closing gate");
    co_await _gate.close();
    vlog(_ctxlog.trace, "closed gate");
    co_await maybe_close_stream();
    if (_download_task) {
        co_await wait_for_download();
    }
}

ss::future<> chunk_data_source_impl::maybe_close_stream() {
    if (_current_stream) {
        co_await _current_stream->close();
        _current_stream = std::nullopt;
    }
}

ss::future<> chunk_data_source_impl::wait_for_download() {
    vassert(
      _download_task.has_value(), "waiting for download but no download task");
    auto reset_download = ss::defer([this] { _download_task.reset(); });
    co_await _download_task->finish();
}

chunk_data_source_impl::download_task::download_task(
  chunk_data_source_impl& ds,
  chunk_start_offset_t chunk_start,
  eager_stream_ptr ecs)
  : _ds{ds}
  , _chunk_start{chunk_start}
  , _ecs{std::move(ecs)} {}

void chunk_data_source_impl::download_task::start() {
    _download.emplace(_ds.load_chunk_handle(_chunk_start, _ecs));
}

ss::future<> chunk_data_source_impl::download_task::finish() {
    if (_download) {
        auto reset = ss::defer([this] { _download.reset(); });
        co_await std::move(_download.value());
    }
}

} // namespace cloud_storage
