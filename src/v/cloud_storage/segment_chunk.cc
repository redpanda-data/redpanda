/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_chunk.h"

#include "cloud_storage/remote_segment.h"

namespace {
constexpr auto cache_backoff_duration = 5s;
constexpr auto eviction_duration = 10s;
} // namespace

namespace cloud_storage {

void expiry_handler_impl(ss::promise<segment_chunk::handle_t>& pr) {
    pr.set_exception(ss::timed_out_error());
}

segment_chunks::segment_chunks(remote_segment& segment)
  : _segment(segment)
  , _cache_backoff_jitter(cache_backoff_duration)
  , _eviction_jitter(eviction_duration)
  , _rtc{_as}
  , _ctxlog(cst_log, _rtc, _segment.get_segment_path()().native()) {}

ss::future<> segment_chunks::start() {
    if (_started) {
        return ss::now();
    }

    _started = true;

    const auto& ix = _segment.get_coarse_index();

    // The first chunk starts at offset 0, whereas the first entry in the coarse
    // index will be after that.
    _chunks[0] = segment_chunk{
      .current_state = chunk_state::not_available,
      .handle = std::nullopt,
      .required_by_readers_in_future = 0,
      .required_after_n_chunks = 0,
      .waiters = {expiry_handler_impl}};

    for (const auto& [koff, file_offset] : ix) {
        vlog(
          _ctxlog.trace,
          "adding chunk metadata at file offset: {} [kafka offset {}]",
          file_offset,
          koff);
        _chunks[file_offset] = segment_chunk{
          .current_state = chunk_state::not_available,
          .handle = std::nullopt,
          .required_by_readers_in_future = 0,
          .required_after_n_chunks = 0,
          .waiters = {expiry_handler_impl}};
    }

    _eviction_timer.set_callback(
      [this] { ssx::background = trim_chunk_files(); });
    _eviction_timer.rearm(_eviction_jitter());
    return ss::now();
}

ss::future<> segment_chunks::stop() {
    vlog(_ctxlog.debug, "stopping segment_chunks");
    _eviction_timer.cancel();
    if (!_as.abort_requested()) {
        _as.request_abort();
    }

    co_await _gate.close();
    vlog(_ctxlog.debug, "stopped segment_chunks");
}

bool segment_chunks::downloads_in_progress() const {
    return std::any_of(_chunks.begin(), _chunks.end(), [](const auto& entry) {
        return entry.second.current_state == chunk_state::download_in_progress;
    });
}

ss::future<bool> segment_chunks::do_hydrate_and_materialize(
  file_offset_t chunk_start, segment_chunk& chunk) {
    gate_guard g{_gate};
    vassert(_started, "chunk API is not started");

    auto it = _chunks.find(chunk_start);
    std::optional<file_offset_t> chunk_end = std::nullopt;
    if (auto next = std::next(it); next != _chunks.end()) {
        chunk_end = next->first - 1;
    }

    co_await _segment.hydrate_chunk(chunk_start, chunk_end);
    auto file_handle = co_await _segment.materialize_chunk(chunk_start);
    if (!file_handle) {
        co_return false;
    }

    chunk.handle = ss::make_lw_shared(std::move(file_handle));
    co_return true;
}

static ss::future<segment_chunk::handle_t>
add_waiter_to_chunk(file_offset_t chunk_start, segment_chunk& chunk) {
    ss::promise<segment_chunk::handle_t> p;
    auto f = p.get_future();
    chunk.waiters.push_back(std::move(p), ss::lowres_clock::time_point::max());
    vlog(
      cst_log.trace,
      "hydrate request added to waiters for chunk id {}, current waiter "
      "size: {}",
      chunk_start,
      chunk.waiters.size());
    return f;
}

ss::future<segment_chunk::handle_t>
segment_chunks::hydrate_chunk(file_offset_t chunk_start) {
    gate_guard g{_gate};
    vassert(_started, "chunk API is not started");

    vassert(
      _chunks.contains(chunk_start),
      "No chunk starting at offset {}, cannot hydrate",
      chunk_start);

    auto& chunk = _chunks[chunk_start];
    auto curr_state = chunk.current_state;
    if (curr_state == chunk_state::hydrated) {
        vassert(
          chunk.handle,
          "chunk state is hydrated without data file for id {}",
          chunk_start);
        co_return chunk.handle.value();
    }

    // If a download is already in progress, subsequent callers to hydrate are
    // added to a wait list, and notified when the download finishes.
    if (curr_state == chunk_state::download_in_progress) {
        co_return co_await add_waiter_to_chunk(chunk_start, chunk);
    }

    // Download is not in progress. Set the flag and begin download attempt.
    try {
        chunk.current_state = chunk_state::download_in_progress;

        // Keep retrying if materialization fails.
        while (!co_await do_hydrate_and_materialize(chunk_start, chunk)) {
            vlog(
              _ctxlog.trace,
              "do_hydrate_and_materialize failed for chunk id {}",
              chunk_start);
            co_await ss::sleep_abortable(
              _cache_backoff_jitter.next_jitter_duration(), _as);
        }
    } catch (const std::exception& ex) {
        while (!chunk.waiters.empty()) {
            chunk.waiters.front().set_to_current_exception();
            chunk.waiters.pop_front();
        }
        throw;
    }

    vassert(
      chunk.handle.has_value(),
      "hydrate loop ended without materializing chunk handle for id {}",
      chunk_start);
    auto handle = chunk.handle.value();

    while (!chunk.waiters.empty()) {
        chunk.waiters.front().set_value(handle);
        chunk.waiters.pop_front();
    }

    co_return handle;
}

ss::future<> segment_chunks::trim_chunk_files() {
    gate_guard g{_gate};
    vassert(_started, "chunk API is not started");

    vlog(_ctxlog.trace, "starting chunk trim");

    std::vector<chunk_map_t::iterator> to_release;
    uint64_t hydrated_chunks = 0;
    for (auto it = _chunks.begin(); it != _chunks.end(); ++it) {
        const auto& metadata = it->second;
        if (metadata.current_state == chunk_state::hydrated) {
            hydrated_chunks += 1;
            if (metadata.handle.has_value() && metadata.handle->owned()) {
                to_release.push_back(it);
            }
        }
    }

    bool need_trim = hydrated_chunks > _segment.max_hydrated_chunks();
    vlog(
      _ctxlog.trace,
      "{} hydrated chunks, need trim: {}",
      hydrated_chunks,
      need_trim);
    if (!need_trim) {
        co_return;
    }

    std::sort(
      to_release.begin(),
      to_release.end(),
      [](const auto& it_a, const auto& it_b) {
          if (
            it_a->second.required_by_readers_in_future
            != it_b->second.required_by_readers_in_future) {
              return it_a->second.required_by_readers_in_future
                     < it_b->second.required_by_readers_in_future;
          }

          return it_a->second.required_after_n_chunks
                 > it_a->second.required_after_n_chunks;
      });

    std::vector<ss::lw_shared_ptr<ss::file>> files_to_close;
    files_to_close.reserve(to_release.size());

    // The files to close are first moved out of chunks and into a vector. This
    // is necessary to make sure that the chunks are no longer able to use the
    // file handle about to be closed before a scheduling point. If the file
    // handles are closed in this loop, because the close operation is
    // a scheduling point, it is possible that a chunk may get its file handle
    // acquired by a reader while it is waiting to be closed.
    for (auto& it : to_release) {
        if (hydrated_chunks <= _segment.max_hydrated_chunks()) {
            break;
        }

        vlog(
          _ctxlog.trace,
          "marking chunk starting at offset {} for release, pending for "
          "release: {} chunks",
          it->first,
          hydrated_chunks - _segment.max_hydrated_chunks());
        files_to_close.push_back(std::move(it->second.handle.value()));
        it->second.handle = std::nullopt;
        it->second.current_state = chunk_state::not_available;
        hydrated_chunks -= 1;
    }

    std::vector<ss::future<>> fs;
    fs.reserve(files_to_close.size());

    for (auto& f : files_to_close) {
        fs.push_back(f->close());
    }

    auto close_results = co_await ss::when_all(fs.begin(), fs.end());
    for (const auto& result : close_results) {
        if (result.failed()) {
            vlog(
              _ctxlog.warn,
              "failed to close a chunk file handle during eviction");
        }
    }

    _eviction_timer.rearm(_eviction_jitter());
}

void segment_chunks::register_future_readers(
  file_offset_t first, file_offset_t last) {
    vassert(_started, "chunk API is not started");

    auto start = _chunks.find(first);
    auto end = _chunks.find(last);

    vassert(
      start != _chunks.end(), "No chunk found starting at first: {}", first);
    vassert(end != _chunks.end(), "No chunk found starting at last: {}", last);

    auto required_after = 0;
    for (auto it = start; it != std::next(end); ++required_after, ++it) {
        auto& chunk = it->second;
        chunk.required_by_readers_in_future += 1;
        chunk.required_after_n_chunks += required_after;
    }
}

void segment_chunks::mark_chunk_read(file_offset_t first, file_offset_t last) {
    vassert(_started, "chunk API is not started");

    auto start = _chunks.find(first);
    auto end = _chunks.find(last);

    vassert(
      start != _chunks.end(), "No chunk found starting at first: {}", first);
    vassert(end != _chunks.end(), "No chunk found starting at last: {}", last);

    start->second.required_by_readers_in_future -= 1;
    for (auto it = std::next(start); it != end; ++it) {
        it->second.required_after_n_chunks -= 1;
    }
}

segment_chunk& segment_chunks::get(file_offset_t chunk_start) {
    vassert(_started, "chunk API is not started");

    vassert(
      _chunks.contains(chunk_start),
      "chunk start {} is not present in metadata",
      chunk_start);
    return _chunks[chunk_start];
}

file_offset_t segment_chunks::get_next_chunk_start(file_offset_t f) const {
    vassert(_chunks.contains(f), "No chunk found starting at {}", f);
    auto it = _chunks.find(f);
    auto next = std::next(it);
    vassert(next != _chunks.end(), "No chunk found after {}", f);
    return next->first;
}

void segment_chunks::mark_hydrated(file_offset_t chunk_start) {
    vassert(
      _chunks.contains(chunk_start),
      "No chunk found starting at {}",
      chunk_start);
    _chunks[chunk_start].current_state = chunk_state::hydrated;
}

chunk_data_source_impl::chunk_data_source_impl(
  segment_chunks& chunks,
  remote_segment& segment,
  kafka::offset start,
  kafka::offset end,
  file_offset_t begin_stream_at,
  ss::file_input_stream_options stream_options)
  : _chunks(chunks)
  , _segment(segment)
  , _first_chunk_start(_segment.get_chunk_start_for_kafka_offset(start))
  , _last_chunk_start(_segment.get_chunk_start_for_kafka_offset(end))
  , _begin_stream_at{begin_stream_at - _first_chunk_start}
  , _current_chunk_start(_first_chunk_start)
  , _stream_options(std::move(stream_options))
  , _rtc{_as}
  , _ctxlog{cst_log, _rtc, _segment.get_segment_path()().native()} {
    vlog(
      _ctxlog.trace,
      "chunk data source initialized with file position {} to {}",
      _first_chunk_start,
      _last_chunk_start);
    _chunks.register_future_readers(_current_chunk_start, _last_chunk_start);
}

chunk_data_source_impl::~chunk_data_source_impl() {
    vassert(
      !_current_stream.has_value(),
      "stream not closed before destroying data source");
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
        _chunks.mark_chunk_read(_current_chunk_start, _last_chunk_start);
        _current_chunk_start = _chunks.get_next_chunk_start(
          _current_chunk_start);
        co_await load_stream_for_chunk(_current_chunk_start);
        buf = co_await _current_stream->read();
    }

    co_return buf;
}

ss::future<>
chunk_data_source_impl::load_stream_for_chunk(file_offset_t chunk_start) {
    vlog(_ctxlog.debug, "loading stream for chunk starting at {}", chunk_start);

    _current_data_file
      = co_await _chunks.hydrate_chunk(chunk_start)
          .handle_exception([this, chunk_start](const std::exception_ptr& ex) {
              vlog(
                _ctxlog.error,
                "failed to hydrate chunk starting at {}, error: {}",
                chunk_start,
                ex);

              auto maybe_close_stream = ss::now();
              if (_current_stream) {
                  maybe_close_stream = _current_stream->close().then(
                    [this] { _current_stream = std::nullopt; });
              }

              return maybe_close_stream.then([ex] {
                  return ss::make_exception_future<segment_chunk::handle_t>(ex);
              });
          });

    // Mark the chunk hydrated after it has been shared with the data source, so
    // that it does not get trimmed between hydration and copying the handle.
    // This may be called by all waiters waiting for the handle.
    _chunks.mark_hydrated(chunk_start);

    if (_current_stream) {
        co_await _current_stream->close();
    }

    // The first read of the data source begins at _begin_stream_at. This is
    // necessary because the remote segment reader which uses this data source
    // sets a delta before reading, and the remote segment consumer which
    // consumes data from this source expects all offsets to be below the delta.
    // Setting the appropriate start offset on the file stream makes sure that
    // we do not break that assertion.
    file_offset_t begin = 0;
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
    if (_current_stream) {
        co_await _current_stream->close();
        _current_stream = std::nullopt;
    }
}

} // namespace cloud_storage
