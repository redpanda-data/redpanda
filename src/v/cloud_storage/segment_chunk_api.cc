/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_chunk_api.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/remote_segment.h"
#include "utils/gate_guard.h"

namespace {
constexpr auto cache_backoff_duration = 5s;
constexpr auto eviction_duration = 10s;

ss::future<cloud_storage::segment_chunk::handle_t> add_waiter_to_chunk(
  cloud_storage::chunk_start_offset_t chunk_start,
  cloud_storage::segment_chunk& chunk) {
    ss::promise<cloud_storage::segment_chunk::handle_t> p;
    auto f = p.get_future();
    chunk.waiters.push_back(std::move(p), ss::lowres_clock::time_point::max());

    using cloud_storage::cst_log;
    vlog(
      cst_log.trace,
      "hydrate request added to waiters for chunk id {}, current waiter "
      "size: {}",
      chunk_start,
      chunk.waiters.size());
    return f;
}

} // namespace

namespace cloud_storage {

void expiry_handler_impl(ss::promise<segment_chunk::handle_t>& pr) {
    pr.set_exception(ss::timed_out_error());
}

segment_chunks::segment_chunks(
  remote_segment& segment, uint64_t max_hydrated_chunks)
  : _segment(segment)
  , _cache_backoff_jitter(cache_backoff_duration)
  , _eviction_jitter(eviction_duration)
  , _rtc{_as}
  , _ctxlog(cst_log, _rtc, _segment.get_segment_path()().native())
  , _max_hydrated_chunks{max_hydrated_chunks} {}

ss::future<> segment_chunks::start() {
    if (_started) {
        return ss::now();
    }

    _started = true;

    const auto& ix = _segment.get_coarse_index();

    // The first chunk starts at offset 0, whereas the first entry in the coarse
    // index will be after that. Adding the first chunk independently covers
    // corner cases such as where segment is smaller than chunk. Since chunk 0
    // is always populated, in these cases it covers the full segment, and
    // hydrating it hydrates the entire segment file.
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

ss::future<ss::file> segment_chunks::do_hydrate_and_materialize(
  chunk_start_offset_t chunk_start, std::optional<uint16_t> prefetch_override) {
    gate_guard g{_gate};
    vassert(_started, "chunk API is not started");

    auto it = _chunks.find(chunk_start);
    std::optional<chunk_start_offset_t> chunk_end = std::nullopt;
    if (auto next = std::next(it); next != _chunks.end()) {
        chunk_end = next->first - 1;
    }

    const auto prefetch = prefetch_override.value_or(
      config::shard_local_cfg().cloud_storage_chunk_prefetch);
    co_await _segment.hydrate_chunk(
      segment_chunk_range{_chunks, prefetch, chunk_start});
    co_return co_await _segment.materialize_chunk(chunk_start);
}

ss::future<segment_chunk::handle_t> segment_chunks::hydrate_chunk(
  chunk_start_offset_t chunk_start, std::optional<uint16_t> prefetch_override) {
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
        bool done = false;
        while (!done) {
            auto handle = co_await do_hydrate_and_materialize(
              chunk_start, prefetch_override);
            if (handle) {
                done = true;
                chunk.handle = ss::make_lw_shared(std::move(handle));
            } else {
                vlog(
                  _ctxlog.trace,
                  "do_hydrate_and_materialize failed for chunk start offset {}",
                  chunk_start);
                co_await ss::sleep_abortable(
                  _cache_backoff_jitter.next_jitter_duration(), _as);
            }
        }
    } catch (const std::exception& ex) {
        chunk.current_state = chunk_state::not_available;
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
    chunk.current_state = chunk_state::hydrated;

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

    auto eviction_strategy = make_eviction_strategy(
      config::shard_local_cfg().cloud_storage_chunk_eviction_strategy,
      _max_hydrated_chunks,
      hydrated_chunks);

    co_await eviction_strategy->evict(std::move(to_release), _ctxlog);
    _eviction_timer.rearm(_eviction_jitter());
}

void segment_chunks::register_readers(
  chunk_start_offset_t first, chunk_start_offset_t last) {
    vassert(_started, "chunk API is not started");

    if (last <= first) {
        // If working with a single chunk, there are no future readers.
        return;
    }

    auto start = _chunks.find(first);
    vassert(
      start != _chunks.end(), "No chunk found starting at first: {}", first);

    auto end = _chunks.find(last);
    vassert(end != _chunks.end(), "No chunk found starting at last: {}", last);

    auto required_after = 1;
    end = std::next(end);
    for (auto it = start; it != end; ++required_after, ++it) {
        auto& chunk = it->second;
        chunk.required_by_readers_in_future += 1;
        chunk.required_after_n_chunks += required_after;
    }
}

void segment_chunks::mark_acquired_and_update_stats(
  chunk_start_offset_t first, chunk_start_offset_t last) {
    vassert(_started, "chunk API is not started");

    auto start = _chunks.find(first);
    auto end = _chunks.find(last);

    vassert(
      start != _chunks.end(), "No chunk found starting at first: {}", first);
    vassert(end != _chunks.end(), "No chunk found starting at last: {}", last);

    start->second.required_by_readers_in_future -= 1;

    end = std::next(end);
    for (auto it = start; it != end; ++it) {
        it->second.required_after_n_chunks -= 1;
    }
}

segment_chunk& segment_chunks::get(chunk_start_offset_t chunk_start) {
    vassert(_started, "chunk API is not started");

    vassert(
      _chunks.contains(chunk_start),
      "chunk start {} is not present in metadata",
      chunk_start);
    return _chunks[chunk_start];
}

chunk_start_offset_t
segment_chunks::get_next_chunk_start(chunk_start_offset_t f) const {
    vassert(_chunks.contains(f), "No chunk found starting at {}", f);
    auto it = _chunks.find(f);
    auto next = std::next(it);
    vassert(next != _chunks.end(), "No chunk found after {}", f);
    return next->first;
}

segment_chunks::iterator_t segment_chunks::begin() { return _chunks.begin(); }

segment_chunks::iterator_t segment_chunks::end() { return _chunks.end(); }

ss::future<> chunk_eviction_strategy::close_files(
  std::vector<ss::lw_shared_ptr<ss::file>> files_to_close,
  retry_chain_logger& rtc) {
    std::vector<ss::future<>> fs;
    fs.reserve(files_to_close.size());

    for (auto& f : files_to_close) {
        fs.push_back(f->close());
    }

    auto close_results = co_await ss::when_all(fs.begin(), fs.end());
    for (const auto& result : close_results) {
        if (result.failed()) {
            vlog(
              rtc.warn, "failed to close a chunk file handle during eviction");
        }
    }
}

ss::future<> eager_chunk_eviction_strategy::evict(
  std::vector<segment_chunks::chunk_map_t::iterator> chunks,
  retry_chain_logger& rtc) {
    std::vector<ss::lw_shared_ptr<ss::file>> files_to_close;
    files_to_close.reserve(chunks.size());

    for (auto& it : chunks) {
        files_to_close.push_back(std::move(it->second.handle.value()));
        it->second.handle = std::nullopt;
        it->second.current_state = chunk_state::not_available;
    }

    co_return co_await close_files(std::move(files_to_close), rtc);
}

capped_chunk_eviction_strategy::capped_chunk_eviction_strategy(
  uint64_t max_chunks, uint64_t hydrated_chunks)
  : _max_chunks(max_chunks)
  , _hydrated_chunks(hydrated_chunks) {}

ss::future<> capped_chunk_eviction_strategy::evict(
  std::vector<segment_chunks::chunk_map_t::iterator> chunks,
  retry_chain_logger& rtc) {
    bool need_trim = _hydrated_chunks > _max_chunks;
    vlog(
      rtc.trace,
      "{} hydrated chunks, need trim: {}",
      _hydrated_chunks,
      need_trim);
    if (!need_trim) {
        co_return;
    }

    std::vector<ss::lw_shared_ptr<ss::file>> files_to_close;
    files_to_close.reserve(chunks.size());
    for (auto& it : chunks) {
        if (_hydrated_chunks <= _max_chunks) {
            break;
        }

        vlog(
          rtc.trace,
          "marking chunk starting at offset {} for release, pending for "
          "release: {} chunks",
          it->first,
          _hydrated_chunks - _max_chunks);
        files_to_close.push_back(std::move(it->second.handle.value()));
        it->second.handle = std::nullopt;
        it->second.current_state = chunk_state::not_available;
        _hydrated_chunks -= 1;
    }

    co_return co_await close_files(std::move(files_to_close), rtc);
}

predictive_chunk_eviction_strategy::predictive_chunk_eviction_strategy(
  uint64_t max_chunks, uint64_t hydrated_chunks)
  : _max_chunks(max_chunks)
  , _hydrated_chunks(hydrated_chunks) {}

ss::future<> predictive_chunk_eviction_strategy::evict(
  std::vector<segment_chunks::chunk_map_t::iterator> chunks,
  retry_chain_logger& rtc) {
    bool need_trim = _hydrated_chunks > _max_chunks;
    vlog(
      rtc.trace,
      "{} hydrated chunks, need trim: {}",
      _hydrated_chunks,
      need_trim);
    if (!need_trim) {
        co_return;
    }

    std::sort(
      chunks.begin(), chunks.end(), [](const auto& it_a, const auto& it_b) {
          return it_a->second <=> it_b->second == std::strong_ordering::less;
      });

    std::vector<ss::lw_shared_ptr<ss::file>> files_to_close;
    files_to_close.reserve(chunks.size());

    for (auto& it : chunks) {
        if (_hydrated_chunks <= _max_chunks) {
            break;
        }

        vlog(
          rtc.trace,
          "marking chunk starting at offset {} for release, pending for "
          "release: {} chunks",
          it->first,
          _hydrated_chunks - _max_chunks);
        files_to_close.push_back(std::move(it->second.handle.value()));
        it->second.handle = std::nullopt;
        it->second.current_state = chunk_state::not_available;
        _hydrated_chunks -= 1;
    }

    co_return co_await close_files(std::move(files_to_close), rtc);
}

std::unique_ptr<chunk_eviction_strategy> make_eviction_strategy(
  model::cloud_storage_chunk_eviction_strategy k,
  uint64_t max_chunks,
  uint64_t hydrated_chunks) {
    switch (k) {
    case model::cloud_storage_chunk_eviction_strategy::eager:
        return std::make_unique<eager_chunk_eviction_strategy>();
    case model::cloud_storage_chunk_eviction_strategy::capped:
        return std::make_unique<capped_chunk_eviction_strategy>(
          max_chunks, hydrated_chunks);
    case model::cloud_storage_chunk_eviction_strategy::predictive:
        return std::make_unique<predictive_chunk_eviction_strategy>(
          max_chunks, hydrated_chunks);
    }
}

segment_chunk_range::segment_chunk_range(
  const segment_chunks::chunk_map_t& chunks,
  size_t prefetch,
  chunk_start_offset_t start) {
    auto it = chunks.find(start);
    vassert(
      it != chunks.end(), "failed to find {} in chunk start offsets", start);
    auto n_it = std::next(it);

    // We need one chunk which will be downloaded for the current read, plus the
    // prefetch count
    size_t num_chunks_required = prefetch + 1;

    // Collects start and end file offsets to be hydrated for the given
    // prefetch by iterating over adjacent chunk start offsets. The chunk map
    // does not contain end offsets, so for a given chunk start offset in the
    // map, the corresponding end of chunk is the next entry in the map minus
    // one.
    for (size_t i = 0; i < num_chunks_required && it != chunks.end(); ++i) {
        auto start = it->first;

        // The last entry in the chunk map always represents data upto the
        // end of segment. A nullopt here is a signal to
        // split_segment_into_chunk_range_consumer (which does have access to
        // the segment size) to use the segment size as the end of the byte
        // range.
        std::optional<chunk_start_offset_t> end = std::nullopt;
        if (n_it != chunks.end()) {
            end = n_it->first - 1;
        }

        _chunks[start] = end;
        if (n_it == chunks.end()) {
            break;
        }
        it++;
        n_it++;
    }
}

std::optional<chunk_start_offset_t> segment_chunk_range::last_offset() const {
    auto it = _chunks.end();
    return std::prev(it)->second;
}

chunk_start_offset_t segment_chunk_range::first_offset() const {
    return _chunks.begin()->first;
}

size_t segment_chunk_range::chunk_count() const { return _chunks.size(); }

segment_chunk_range::map_t::iterator segment_chunk_range::begin() {
    return _chunks.begin();
}

segment_chunk_range::map_t::iterator segment_chunk_range::end() {
    return _chunks.end();
}

} // namespace cloud_storage
