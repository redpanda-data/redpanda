/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/memory_first_reader.h"

#include "cloud_topics/batch_cache/batch_cache.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/optimized_optional.hh>

#include <exception>

namespace cloud_topics::prefetch {

namespace {
ss::logger mfrlog("memory_first_reader");
} // namespace

memory_first_reader_impl::memory_first_reader_impl(
  fetch_stream* stream,
  cloud_topics::batch_cache* cache,
  cloud_topic_log_reader_config cfg)
  : _stream(stream)
  , _cache(cache)
  , _cfg(std::move(cfg))
  , _next(_cfg.start_offset) {
    _stream->ref();
}

memory_first_reader_impl::~memory_first_reader_impl() noexcept {
    _stream->unref();
}

bool memory_first_reader_impl::is_end_of_stream() const {
    return _end_of_stream;
}

void memory_first_reader_impl::set_end_of_stream() { _end_of_stream = true; }

bool memory_first_reader_impl::is_over_limit_with_bytes(size_t size) const {
    // Always accept the first batch to guarantee progress.
    if (_bytes_consumed == 0) {
        return false;
    }
    return (_bytes_consumed + size) > _cfg.max_bytes;
}

ss::future<model::record_batch_reader::storage_t>
memory_first_reader_impl::do_load_slice(
  model::timeout_clock::time_point deadline) {
    model::record_batch_reader::data_t result;

    // Check stream error before doing any work (floor check, pre-wait).
    if (auto err = _stream->error()) {
        set_end_of_stream();
        co_await ss::coroutine::return_exception_ptr(err);
    }

    if (_next > _cfg.max_offset) {
        set_end_of_stream();
        co_return result;
    }

    const auto& tidp = _stream->tidp();

    // Cache-walk loop: pull consecutive batches from the cache.
    while (_next <= _cfg.max_offset) {
        // Read only from this stream's own index: the current post-compaction
        // L1 data, with ghost batches marking gaps. The L1 reader serves its
        // range (up to LRO) and never consults the shared per-partition cache,
        // whose write-through entries are pre-compaction originals and would be
        // stale. Offsets this stream does not cover (recent data not yet in an
        // L1 object) are served on the consumer's retry by the L0 reader, which
        // owns the per-partition cache.
        auto batch_opt = _stream->cached_get(kafka::offset_cast(_next));
        if (!batch_opt.has_value()) {
            // Cache miss.
            break;
        }

        auto& batch = batch_opt.value();
        if (batch.header().type == model::record_batch_type::ghost_batch) {
            // Gap marker (compaction hole, removed tx control/aborted range, or
            // a leading offset below the first data batch). Skip past it
            // without returning it to the consumer. The producer ghost-fills
            // gaps so a real gap (skip) is distinguishable from an evicted
            // offset (a plain miss, handled by the break above).
            _next = kafka::next_offset(model::offset_cast(batch.last_offset()));
            continue;
        }
        auto batch_size = batch.size_bytes();
        if (is_over_limit_with_bytes(batch_size)) {
            set_end_of_stream();
            break;
        }

        _bytes_consumed += batch_size;
        _next = kafka::next_offset(model::offset_cast(batch.last_offset()));
        result.push_back(std::move(batch));
    }

    if (!result.empty()) {
        // Report consumption to the stream so the pacer can slide the window.
        // Pass the delta (bytes since last report), not the cumulative total.
        auto last_consumed = model::offset_cast(result.back().last_offset());
        size_t delta = _bytes_consumed - _reported_bytes;
        _reported_bytes = _bytes_consumed;
        _stream->on_consumed(last_consumed, delta, ss::lowres_clock::now());
        co_return result;
    }

    // We have nothing yet. If we're already at or past the deadline, return
    // the empty result without waiting.
    if (model::timeout_clock::now() >= deadline) {
        set_end_of_stream();
        co_return result;
    }

    // Signal demand so the service prioritises this stream.
    _stream->on_demand(_next);
    vlog(mfrlog.debug, "waiting for offset {}", _next);

    // Build a local abort_source that fires on: (a) fetch abort, (b) stream
    // error. We pass this to wait_for_offset so any of the four wakeup
    // conditions (data / stream-error / deadline / shutdown) resolves the wait.
    ss::abort_source local_as;

    // Subscribe to the fetch-level abort source (shutdown / client cancel).
    ss::optimized_optional<ss::abort_source::subscription> fetch_sub;
    if (_cfg.abort_source.has_value()) {
        fetch_sub = _cfg.abort_source.value().get().subscribe(
          [&local_as]() noexcept {
              if (!local_as.abort_requested()) {
                  local_as.request_abort();
              }
          });
    }

    // Subscribe to the stream's error abort source so a producer error fires
    // our local abort immediately.
    auto err_sub = _stream->error_abort_source().subscribe(
      [&local_as]() noexcept {
          if (!local_as.abort_requested()) {
              local_as.request_abort();
          }
      });

    try {
        co_await _cache->wait_for_offset(
          tidp,
          kafka::offset_cast(_next),
          model::prev_offset(kafka::offset_cast(_next)),
          deadline,
          std::ref(local_as));
    } catch (const ss::abort_requested_exception&) {
        // Woken by local_as: either stream error, fetch abort, or (rarely)
        // deadline handling inside wait_for_offset. Fall through to post-wake
        // checks below.
    } catch (const ss::timed_out_error&) {
        // Deadline expired during wait. Return whatever is in the cache now.
    }

    // Post-wake error check (floor: always check after waking).
    if (auto err = _stream->error()) {
        vlog(
          mfrlog.warn, "stream error woke reader at offset {}: {}", _next, err);
        set_end_of_stream();
        co_await ss::coroutine::return_exception_ptr(err);
    }
    vlog(mfrlog.debug, "woke for offset {} (or deadline/abort)", _next);

    // Check fetch abort.
    if (_cfg.abort_source.has_value()) {
        _cfg.abort_source.value().get().check();
    }

    // Re-walk the cache after waking.
    while (_next <= _cfg.max_offset) {
        // Read only from this stream's own index: the current post-compaction
        // L1 data, with ghost batches marking gaps. The L1 reader serves its
        // range (up to LRO) and never consults the shared per-partition cache,
        // whose write-through entries are pre-compaction originals and would be
        // stale. Offsets this stream does not cover (recent data not yet in an
        // L1 object) are served on the consumer's retry by the L0 reader, which
        // owns the per-partition cache.
        auto batch_opt = _stream->cached_get(kafka::offset_cast(_next));
        if (!batch_opt.has_value()) {
            break;
        }

        auto& batch = batch_opt.value();
        if (batch.header().type == model::record_batch_type::ghost_batch) {
            // Gap marker: skip past it (see the pre-wait walk for rationale).
            _next = kafka::next_offset(model::offset_cast(batch.last_offset()));
            continue;
        }
        auto batch_size = batch.size_bytes();
        if (is_over_limit_with_bytes(batch_size)) {
            set_end_of_stream();
            break;
        }

        _bytes_consumed += batch_size;
        _next = kafka::next_offset(model::offset_cast(batch.last_offset()));
        result.push_back(std::move(batch));
    }

    if (!result.empty()) {
        auto last_consumed = model::offset_cast(result.back().last_offset());
        size_t delta = _bytes_consumed - _reported_bytes;
        _reported_bytes = _bytes_consumed;
        _stream->on_consumed(last_consumed, delta, ss::lowres_clock::now());
    } else {
        // Nothing after waking: treat as end of stream for this slice.
        set_end_of_stream();
    }

    co_return result;
}

fmt::iterator memory_first_reader_impl::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "memory_first_reader");
}

model::record_batch_reader make_memory_first_reader(
  fetch_stream* stream,
  cloud_topics::batch_cache* cache,
  cloud_topic_log_reader_config cfg) {
    return model::make_record_batch_reader<memory_first_reader_impl>(
      stream, cache, std::move(cfg));
}

} // namespace cloud_topics::prefetch
