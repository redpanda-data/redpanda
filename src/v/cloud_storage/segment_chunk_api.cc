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

#include <chrono>

using namespace std::chrono_literals;

namespace views = std::views;

namespace {
constexpr auto cache_backoff_duration = 5s;
constexpr auto eviction_duration = 10s;

void set_waiter_errors(
  ss::expiring_fifo<
    ss::promise<cloud_storage::segment_chunk::handle_t>,
    cloud_storage::segment_chunk::expiry_handler>& waiters,
  const std::exception_ptr& ex) {
    while (!waiters.empty()) {
        waiters.front().set_exception(ex);
        waiters.pop_front();
    }
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

    _eviction_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] { return trim_chunk_files(); });
    });
    _eviction_timer.rearm(_eviction_jitter());
    ssx::background = run_hydrate_bg();
    return ss::now();
}

ss::future<> segment_chunks::stop() {
    vlog(_ctxlog.debug, "stopping segment_chunks");
    _bg_cvar.broken();
    _eviction_timer.cancel();
    if (!_as.abort_requested()) {
        _as.request_abort();
    }

    co_await _gate.close();
    resolve_prefetch_futures();
    vlog(_ctxlog.debug, "stopped segment_chunks");
}

bool segment_chunks::downloads_in_progress() const {
    return std::ranges::any_of(
      _chunks, [](const auto& chunk) { return !chunk.second.waiters.empty(); });
}

ss::future<segment_chunk::handle_t> segment_chunks::hydrate_chunk(
  chunk_start_offset_t chunk_start, std::optional<uint16_t> prefetch_override) {
    auto g = _gate.hold();
    vassert(_started, "chunk API is not started");

    vassert(
      _chunks.contains(chunk_start),
      "No chunk starting at offset {}, cannot hydrate",
      chunk_start);

    auto& chunk = _chunks[chunk_start];
    auto curr_state = chunk.current_state;

    vlog(
      _ctxlog.debug,
      "hydrate_chunk for {}, current state: {}",
      chunk_start,
      curr_state);
    if (curr_state == chunk_state::hydrated) {
        vassert(
          chunk.handle,
          "chunk state is hydrated without data file for id {}",
          chunk_start);
        co_return chunk.handle.value();
    }

    chunk.current_state = chunk_state::download_in_progress;

    ss::promise<cloud_storage::segment_chunk::handle_t> p;
    auto f = p.get_future();
    chunk.waiters.push_back(std::move(p), ss::lowres_clock::time_point::max());

    vlog(
      _ctxlog.trace,
      "hydrate request added to waiters for chunk id {}, current waiter "
      "size: {}",
      chunk_start,
      chunk.waiters.size());

    auto n_chunks_to_prefetch = prefetch_override.value_or(
      config::shard_local_cfg().cloud_storage_chunk_prefetch);
    schedule_prefetches(chunk_start, n_chunks_to_prefetch);

    _bg_cvar.signal();
    co_return co_await std::move(f);
}

void segment_chunks::schedule_prefetches(
  chunk_start_offset_t start_offset, size_t n_chunks_to_prefetch) {
    vassert(
      _chunks.contains(start_offset),
      "No chunk starting at offset {}, cannot schedule prefetches",
      start_offset);
    for (auto it = std::next(_chunks.find(start_offset));
         it != _chunks.end() && n_chunks_to_prefetch > 0;
         ++it, --n_chunks_to_prefetch) {
        auto& [start, chunk] = *it;
        if (
          chunk.current_state == chunk_state::download_in_progress
          || chunk.current_state == chunk_state::hydrated) {
            continue;
        }

        chunk.current_state = chunk_state::download_in_progress;
        ss::promise<cloud_storage::segment_chunk::handle_t> p;
        _prefetches.emplace_back(p.get_future());
        chunk.waiters.push_back(
          std::move(p), ss::lowres_clock::time_point::max());
        vlog(
          _ctxlog.trace,
          "prefetch hydrate request added for chunk id {}, current "
          "waiter size: {}",
          start,
          chunk.waiters.size());
    }
}

void segment_chunks::resolve_prefetch_futures() {
    auto available_it = std::ranges::partition(
      _prefetches, [](const auto& f) { return !f.available(); });

    fragmented_vector<ss::future<segment_chunk::handle_t>> resolved;
    resolved.reserve(available_it.size());

    std::ranges::move(available_it, std::back_inserter(resolved));

    if (!resolved.empty()) {
        vlog(_ctxlog.trace, "{} completed prefetches", resolved.size());
        for (auto& f : resolved) {
            vassert(
              f.available(), "future not available when resolving prefetches");
            if (f.failed()) {
                vlog(
                  _ctxlog.trace,
                  "failed prefetch download: {}",
                  f.get_exception());
            }
        }
    }
}

ss::future<> segment_chunks::trim_chunk_files() {
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
    resolve_prefetch_futures();
    _eviction_timer.rearm(_eviction_jitter());
}

ss::future<> segment_chunks::run_hydrate_bg() {
    auto g = _gate.hold();
    vassert(_started, "chunk API is not started");

    const auto has_waiters = [](const auto& pair) {
        return !pair.second.waiters.empty();
    };

    while (!_gate.is_closed()) {
        try {
            co_await _bg_cvar.wait(
              [this] { return downloads_in_progress() || _gate.is_closed(); });
            // TODO prioritize "real" requests over prefetches
            co_await ss::max_concurrent_for_each(
              _chunks | views::filter(has_waiters) | views::keys,
              _segment.concurrency(),
              [this](auto start) { return do_hydrate_chunk(start); });
        } catch (...) {
            const auto& ex = std::current_exception();
            for (auto& chunk : views::values(_chunks)) {
                if (!chunk.waiters.empty()) {
                    set_waiter_errors(chunk.waiters, ex);
                }
            }
            if (ssx::is_shutdown_exception(ex)) {
                vlog(_ctxlog.debug, "Chunk API hydration loop shut down");
                break;
            } else {
                vlog(
                  _ctxlog.error, "Chunk API error in hydration loop: {}", ex);
            }
        }
    }
}

ss::future<>
segment_chunks::do_hydrate_chunk(chunk_start_offset_t start_offset) {
    auto g = _gate.hold();
    vassert(
      _chunks.contains(start_offset),
      "No chunk starting at offset {}, cannot hydrate",
      start_offset);

    auto& chunk = _chunks[start_offset];
    auto& waiters = chunk.waiters;
    if (waiters.empty()) {
        co_return;
    }

    try {
        auto handle = co_await _segment.download_chunk(start_offset);
        vassert(
          chunk.handle == std::nullopt,
          "attempt to set file handle to chunk {} which already has a file "
          "assigned, this will result in a file descriptor leak",
          start_offset);
        chunk.handle = ss::make_lw_shared(std::move(handle));
        chunk.current_state = chunk_state::hydrated;

        while (!waiters.empty()) {
            waiters.front().set_value(chunk.handle.value());
            waiters.pop_front();
        }
    } catch (...) {
        const auto ex = std::current_exception();
        chunk.current_state = chunk_state::not_available;
        set_waiter_errors(waiters, ex);
    }
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

std::pair<size_t, size_t> segment_chunks::get_byte_range_for_chunk(
  chunk_start_offset_t start_offset, size_t last_byte_in_segment) const {
    auto it = _chunks.find(start_offset);
    vassert(it != _chunks.end(), "No chunk found starting at {}", start_offset);
    if (auto next_ch = std::next(it); next_ch != _chunks.end()) {
        return {start_offset, next_ch->first - 1};
    }
    return {start_offset, last_byte_in_segment};
}

ss::future<> chunk_eviction_strategy::close_files(
  std::vector<ss::lw_shared_ptr<ss::file>> files_to_close,
  retry_chain_logger& rtc) {
    std::vector<ss::future<>> fs;
    fs.reserve(files_to_close.size());

    for (auto& f : files_to_close) {
        fs.push_back(f->close());
    }

    auto close_results = co_await ss::when_all(fs.begin(), fs.end());
    for (auto& result : close_results) {
        if (result.failed()) {
            vlog(
              rtc.warn,
              "failed to close a chunk file handle during eviction: {}",
              result.get_exception());
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

} // namespace cloud_storage
