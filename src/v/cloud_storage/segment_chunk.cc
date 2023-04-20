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

segment_chunks::segment_chunks(remote_segment& segment)
  : _segment(segment)
  , _cache_backoff_jitter(cache_backoff_duration)
  , _eviction_jitter(eviction_duration) {
    for (segment_chunk_id_t id = 0; id < _segment.get_chunks_in_segment();
         ++id) {
        _chunks[id] = segment_chunk{
          .current_state = chunk_state::not_available,
          .handle = std::nullopt,
          .required_by_readers_in_future = 0,
          .required_after_n_chunks = 0,
          .waiters = {}};
    }
}

ss::future<> segment_chunks::start() {
    _eviction_timer.set_callback(
      [this] { ssx::background = trim_chunk_files(); });
    _eviction_timer.rearm(_eviction_jitter());
    return ss::now();
}

ss::future<> segment_chunks::stop() {
    _eviction_timer.cancel();
    if (!_as.abort_requested()) {
        _as.request_abort();
    }

    return _gate.close();
}

bool segment_chunks::downloads_in_progress() const {
    return std::any_of(_chunks.begin(), _chunks.end(), [](const auto& entry) {
        return entry.second.current_state == chunk_state::download_in_progress;
    });
}

ss::future<bool> segment_chunks::do_hydrate_and_materialize(
  segment_chunk_id_t chunk_id, segment_chunk& chunk) {
    co_await _segment.hydrate_segment_chunk(chunk_id);
    auto file_handle = co_await _segment.materialize_segment_chunk(chunk_id);
    if (!file_handle) {
        co_return false;
    }

    chunk.handle = ss::make_lw_shared(std::move(file_handle));
    chunk.current_state = chunk_state::hydrated;
    co_return true;
}

ss::future<> segment_chunks::hydrate_chunk_id(segment_chunk_id_t chunk_id) {
    gate_guard g{_gate};

    auto& chunk = _chunks[chunk_id];
    auto curr_state = chunk.current_state;
    if (curr_state == chunk_state::hydrated) {
        vassert(
          chunk.handle,
          "chunk state is hydrated without data file for id {}",
          chunk_id);
        co_return;
    }

    // If a download is already in progress, subsequent callers to hydrate are
    // added to a wait list, and notified when the download finishes.
    if (curr_state == chunk_state::download_in_progress) {
        ss::promise<> p;
        auto f = p.get_future();
        chunk.waiters.push_back(
          std::move(p), ss::lowres_clock::time_point::max());
        co_return co_await f.discard_result();
    }

    // Download is not in progress. Set the flag and begin download attempt.
    try {
        chunk.current_state = chunk_state::download_in_progress;

        // Keep retrying if materialization fails.
        while (!co_await do_hydrate_and_materialize(chunk_id, chunk)) {
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
      chunk.handle,
      "hydrate loop ended without materializing chunk handle for id {}",
      chunk_id);

    while (!chunk.waiters.empty()) {
        chunk.waiters.front().set_value();
        chunk.waiters.pop_front();
    }
}

ss::future<> segment_chunks::trim_chunk_files() {
    gate_guard g{_gate};

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
              cst_log.warn,
              "failed to close a chunk file handle during eviction");
        }
    }

    _eviction_timer.rearm(_eviction_jitter());
}

void segment_chunks::register_future_readers(
  segment_chunk_id_t first, segment_chunk_id_t last) {
    for (segment_chunk_id_t id = first, required_after = 0; id <= last;
         ++id, ++required_after) {
        auto& chunk = _chunks[id];
        chunk.required_by_readers_in_future += 1;
        chunk.required_after_n_chunks += required_after;
    }
}

void segment_chunks::mark_chunk_read(
  segment_chunk_id_t first, segment_chunk_id_t last) {
    _chunks[first].required_by_readers_in_future -= 1;
    for (auto id = first + 1; id <= last; ++id) {
        _chunks[id].required_after_n_chunks -= 1;
    }
}

segment_chunk& segment_chunks::get(segment_chunk_id_t chunk_id) {
    vassert(
      _chunks.contains(chunk_id),
      "chunk id {} is not present in metadata",
      chunk_id);
    return _chunks[chunk_id];
}

chunk_data_source_impl::chunk_data_source_impl(
  segment_chunks& chunks,
  remote_segment& segment,
  kafka::offset start,
  kafka::offset end,
  ss::file_input_stream_options stream_options)
  : _chunks(chunks)
  , _segment(segment)
  , _first(_segment.chunk_id_for_kafka_offset(start))
  , _last(_segment.chunk_id_for_kafka_offset(end))
  , _current_chunk_id(_first.chunk_id)
  , _stream_options(std::move(stream_options)) {
    _chunks.register_future_readers(_current_chunk_id, _last.chunk_id);
}

chunk_data_source_impl::~chunk_data_source_impl() {
    vassert(
      !_current_stream.has_value(),
      "stream not closed before destroying data source");
}

ss::future<ss::temporary_buffer<char>> chunk_data_source_impl::get() {
    gate_guard g{_gate};

    if (!_current_stream) {
        co_await load_stream_for_chunk(_current_chunk_id);
        vassert(
          _current_stream.has_value(),
          "cannot read without stream for segment {}, current chunk id: {}",
          _segment.get_segment_path(),
          _current_chunk_id);
    }

    auto buf = co_await _current_stream->read();
    while (buf.empty() && _current_chunk_id < _last.chunk_id) {
        _chunks.mark_chunk_read(_current_chunk_id, _last.chunk_id);
        _current_chunk_id += 1;
        co_await load_stream_for_chunk(_current_chunk_id);
        buf = co_await _current_stream->read();
    }

    co_return buf;
}

ss::future<>
chunk_data_source_impl::load_stream_for_chunk(segment_chunk_id_t chunk_id) {
    co_await _chunks.hydrate_chunk_id(chunk_id).handle_exception(
      [this, chunk_id](const std::exception_ptr& ex) {
          vlog(
            cst_log.error,
            "failed to hydrate chunk {} for segment {}, error: {}",
            chunk_id,
            _segment.get_segment_path(),
            ex);
          if (_current_stream) {
              return _current_stream->close().then(
                [this] { _current_stream = std::nullopt; });
          }
          rethrow_exception(ex);
      });

    auto& handle = _chunks.get(chunk_id).handle;
    vassert(
      handle.has_value(),
      "chunk handle not present after hydration for chunk id {}, segment {}",
      _current_chunk_id,
      _segment.get_segment_path());

    // Decrement the count of the file handle for chunk id we just read, while
    // incrementing the count for the current chunk id.
    _current_data_file = handle.value();

    uint64_t start = 0;
    if (chunk_id == _first.chunk_id) {
        start = _first.file_pos;
    }

    uint64_t len = co_await _current_data_file->size();
    if (chunk_id == _last.chunk_id) {
        len = _last.file_pos + 1;
    }

    if (_current_stream) {
        co_await _current_stream->close();
    }

    _current_stream = ss::make_file_input_stream(
      *_current_data_file, start, len, _stream_options);
}

ss::future<> chunk_data_source_impl::close() {
    co_await _gate.close();
    if (_current_stream) {
        co_await _current_stream->close();
        _current_stream = std::nullopt;
    }
}

} // namespace cloud_storage
