/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/fetch_stream.h"

#include "base/vassert.h"
#include "cloud_topics/batch_cache/batch_cache.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "cloud_topics/logger.h"
#include "model/batch_utils.h"
#include "model/fundamental.h"
#include "storage/batch_cache.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future-util.hh>
#include <seastar/coroutine/as_future.hh>

#include <algorithm>
#include <exception>
#include <limits>

namespace cloud_topics::prefetch {

namespace {
ss::logger fslog("fetch_stream");
} // namespace

fetch_stream::fetch_stream(
  model::topic_id_partition tidp,
  kafka::offset start,
  l1::metastore* metastore,
  l1::io* io,
  cloud_topics::batch_cache* cache,
  prefetch_pacer pacer,
  cloud_topics::level_one_reader_probe* l1_reader_probe)
  : _tidp(tidp)
  , _next_offset(start)
  , _produced_through(kafka::prev_offset(start))
  , _metastore(metastore)
  , _io(io)
  , _cache(cache)
  , _l1_index(cache != nullptr ? cache->create_index() : nullptr)
  , _pacer(pacer)
  , _l1_reader_probe(l1_reader_probe)
  , _log(fslog, fmt::format("[{}/{}]", fmt::ptr(this), _tidp))
  , _demand_watermark(start) {
    vlog(_log.debug, "New fetch_stream created at offset {}", start);
}

fetch_stream::~fetch_stream() = default;

ss::future<bool> fetch_stream::ensure_lookahead() {
    // Discard stale entries entirely before the requested offset.
    while (!_lookahead.empty()
           && _lookahead.front().last_offset < _next_offset) {
        _lookahead.pop_front();
    }
    if (!_lookahead.empty()) {
        co_return true;
    }
    if (_lookahead_exhausted) {
        co_return false;
    }

    retry_chain_node rtc = l1::make_default_metastore_rtc(_abort);
    auto offset = _next_offset;
    auto response = co_await l1::retry_metastore_op(
      [this, offset] -> ss::future<std::expected<
                       l1::metastore::extent_metadata_response,
                       l1::metastore::errc>> {
          return _metastore->get_extent_metadata_forwards(
            _tidp,
            offset,
            kafka::offset::max(),
            /*max_num_extents=*/1,
            l1::metastore::include_object_metadata::yes);
      },
      rtc);

    if (!response.has_value()) {
        switch (response.error()) {
        case l1::metastore::errc::out_of_range:
        case l1::metastore::errc::missing_ntp:
            // No data at or beyond this offset: treat as end of stream. Not a
            // terminal error - the reader observes this as no further data.
            vlog(
              _log.debug,
              "No L1 objects at offset {} ({})",
              offset,
              response.error());
            _lookahead_exhausted = true;
            co_return false;
        default:
            record_error(
              std::make_exception_ptr(
                std::runtime_error(_log.format(
                  "Metastore query failed at offset {}: {}",
                  offset,
                  response.error()))));
            co_return false;
        }
    }

    for (auto& em : response.value().extents) {
        vassert(
          em.object_info.has_value(),
          "extent metadata missing object_info for offsets ({}~{})",
          em.base_offset,
          em.last_offset);
        _lookahead.push_back(
          l1::metastore::object_response{
            .oid = em.object_info->oid,
            .footer_pos = em.object_info->footer_pos,
            .object_size = em.object_info->object_size,
            .first_offset = em.base_offset,
            .last_offset = em.last_offset,
          });
    }
    // A non-empty response leaves end_of_stream meaningful only when we hit
    // the extent limit; we requested a single extent so more may follow. An
    // empty response with end_of_stream means nothing beyond.
    if (response.value().extents.empty()) {
        _lookahead_exhausted = response.value().end_of_stream;
        co_return false;
    }
    co_return true;
}

ss::future<bool> fetch_stream::ensure_current_object() {
    if (_current.has_value()) {
        co_return true;
    }

    auto& obj = _lookahead.front();
    auto downloader = std::make_unique<chunk_downloader>(
      _io, obj.oid, cloud_io::group_id::default_group, std::ref(_abort));

    auto footer_fut = co_await ss::coroutine::as_future(
      downloader->fetch_footer(obj.footer_pos, obj.object_size));
    if (footer_fut.failed()) {
        auto ex = footer_fut.get_exception();
        vlog(_log.error, "Footer fetch failed for object {}: {}", obj.oid, ex);
        co_await downloader->close();
        record_error(ex);
        co_return false;
    }
    auto footer = std::move(footer_fut).get();
    if (_l1_reader_probe != nullptr) {
        _l1_reader_probe->register_footer_read(
          obj.object_size - obj.footer_pos);
    }

    auto seek = footer.file_position_before_kafka_offset(_tidp, _next_offset);
    auto oid = obj.oid;
    auto last_offset = obj.last_offset;
    if (seek == l1::footer::npos) {
        // The object spans this offset in the metastore but has no data for it
        // (e.g. compaction). Skip past the object and move on.
        vlog(
          _log.debug, "No data for offset {} in object {}", _next_offset, oid);
        co_await downloader->close();
        _next_offset = kafka::next_offset(last_offset);
        _lookahead.pop_front();
        co_return false;
    }

    // Recreate the downloader with an explicit run start so concurrent,
    // out-of-order dispatch within the run cannot move the decode cursor. The
    // first downloader was only used to fetch the footer (no chunk was
    // dispatched), so dropping it here is safe and cheap.
    co_await downloader->close();
    auto run_downloader = std::make_unique<chunk_downloader>(
      _io,
      oid,
      cloud_io::group_id::default_group,
      std::ref(_abort),
      /*start_position=*/seek.file_position);

    _current = current_object{
      .oid = oid,
      .last_offset = last_offset,
      .downloader = std::move(run_downloader),
      .run_cursor = seek.file_position,
      .run_end = seek.file_position + seek.length,
    };
    _lookahead.pop_front();
    co_return true;
}

ss::future<>
fetch_stream::produce(size_t window_budget, size_t max_concurrent) {
    _last_active = ss::lowres_clock::now();
    if (_error) {
        co_return;
    }
    if (window_budget == 0 || max_concurrent == 0) {
        co_return;
    }

    if (!_current.has_value()) {
        if (!co_await ensure_lookahead()) {
            co_return;
        }
        if (!co_await ensure_current_object()) {
            // Either an error was recorded, the object was skipped, or no data
            // exists. The scheduler calls produce() again to make progress.
            co_return;
        }
    }

    auto& cur = *_current;

    // Dispatch up to `window_budget` bytes of chunks from the current run,
    // CONCURRENTLY, with at most `max_concurrent` GETs in flight at once. All
    // chunks belong to the same run, so the contiguous decode and slack carry
    // stay well defined. The dispatch futures resolve once the GET has landed
    // and been (attempted to be) fed to the reassembler.
    chunked_vector<ss::future<>> dispatches;
    size_t dispatched_bytes = 0;
    while (cur.run_cursor < cur.run_end && dispatched_bytes < window_budget
           && dispatches.size() < max_concurrent) {
        size_t run_remaining = cur.run_end - cur.run_cursor;
        // Each chunk is pacer-sized (clamped to the run remaining). The total
        // window budget caps a single chunk so it never overshoots, but it does
        // NOT shrink per chunk — that is what lets the window split into
        // several pacer-sized chunks dispatched concurrently rather than one
        // chunk that swallows the whole window.
        size_t size = _pacer.next_chunk_size(run_remaining, window_budget);
        if (size == 0) {
            break;
        }
        // Do not overshoot the window with the final chunk.
        size = std::min(size, window_budget - dispatched_bytes);
        if (size == 0) {
            break;
        }
        size_t pos = cur.run_cursor;
        cur.run_cursor += size;
        dispatched_bytes += size;
        _in_flight_bytes += size;
        dispatches.push_back(cur.downloader->dispatch(pos, size));
    }

    // Wait for every dispatched GET to land before draining and before any
    // run-boundary handling, so the reassembler sees the full contiguous run.
    auto results = co_await ss::when_all(dispatches.begin(), dispatches.end());
    // All dispatched GETs have resolved: they are no longer in-flight buffers.
    _in_flight_bytes -= std::min(_in_flight_bytes, dispatched_bytes);

    for (auto& r : results) {
        if (r.failed()) {
            auto ex = r.get_exception();
            vlog(_log.error, "Dispatch failed for object {}: {}", cur.oid, ex);
            record_error(ex);
            co_return;
        }
    }

    // All dispatched chunk GETs landed: count the bytes read from the L1
    // object against the reused L1 reader read_bytes metric.
    if (_l1_reader_probe != nullptr && dispatched_bytes > 0) {
        _l1_reader_probe->register_bytes_read(dispatched_bytes);
    }

    chunked_vector<model::record_batch> batches;
    try {
        batches = cur.downloader->take_ready();
    } catch (...) {
        auto ex = std::current_exception();
        vlog(_log.error, "Decode failed for object {}: {}", cur.oid, ex);
        record_error(ex);
        co_return;
    }

    size_t skipped_bytes = 0;
    std::optional<model::term_id> last_term;
    for (auto& b : batches) {
        auto last = model::offset_cast(b.last_offset());
        if (last < _next_offset) {
            // The footer index seek is coarse (~4 MiB granularity) and may
            // land before _next_offset. Skip pre-position batches rather than
            // caching them and pulling produced_through below start.
            skipped_bytes += b.size_bytes();
            continue;
        }
        // Fill any gap between the current position and this batch with ghost
        // batches so the cache has contiguous coverage. Gaps are kafka offsets
        // with no data batch in the reconciled L1 stream: a leading transaction
        // fence/control batch, an aborted-transaction range removed during
        // reconciliation, or a compaction hole. The reader skips ghost batches;
        // their presence lets it tell a real gap (skip and advance) apart from
        // an evicted offset (a plain miss it must wait on / re-produce).
        auto gap_start = kafka::offset_cast(_next_offset);
        if (b.base_offset() > gap_start) {
            for (auto& g : model::make_ghost_batches(
                   gap_start, model::prev_offset(b.base_offset()), b.term())) {
                put_and_pin(g);
            }
            _produced_through = std::max(
              _produced_through,
              model::offset_cast(model::prev_offset(b.base_offset())));
            _next_offset = model::offset_cast(b.base_offset());
        }
        _cached_ahead_bytes += b.size_bytes();
        // Land the batch in this stream's own index and pin it (see
        // put_and_pin: the pin keeps the reclaimer from dropping un-consumed
        // prefetch mid-read, which would force a cold-miss re-seek and collapse
        // throughput). The pin is released as the reader consumes
        // (unpin_through).
        put_and_pin(b);
        _produced_through = std::max(_produced_through, last);
        _next_offset = kafka::next_offset(last);
        last_term = b.term();
    }
    if (_l1_reader_probe != nullptr && skipped_bytes > 0) {
        _l1_reader_probe->register_bytes_skipped(skipped_bytes);
    }

    if (cur.run_cursor >= cur.run_end) {
        // Run/object complete. The writer emits whole batches so the run ends
        // on a batch boundary; the reassembler must carry no slack.
        vassert(
          cur.downloader->reassembler_slack_bytes() == 0,
          "non-empty reassembler slack ({} bytes) at run boundary for object "
          "{}",
          cur.downloader->reassembler_slack_bytes(),
          cur.oid);
        // Fill a trailing gap: the object's logical range can extend past the
        // last data batch (e.g. it ends with an aborted-transaction range that
        // reconciliation removed). Ghost-fill it so the reader skips past it
        // instead of waiting on offsets that will never arrive.
        if (last_term.has_value() && _next_offset <= cur.last_offset) {
            for (auto& g : model::make_ghost_batches(
                   kafka::offset_cast(_next_offset),
                   kafka::offset_cast(cur.last_offset),
                   last_term.value())) {
                put_and_pin(g);
            }
        }
        // Advance position past the object even if the last batches were
        // already cached by another stream (idempotent put).
        _next_offset = std::max(
          _next_offset, kafka::next_offset(cur.last_offset));
        _produced_through = std::max(_produced_through, cur.last_offset);
        co_await close_current_object();
    }

    // This stream landed data in its own index; the shared per-partition
    // monitor's put-path notify does not cover that, so wake any reader parked
    // on it to re-check (it consults this stream's index first).
    if (_l1_index != nullptr && _cache != nullptr) {
        _cache->notify_produced(_tidp, kafka::offset_cast(_produced_through));
    }
}

bool fetch_stream::needs_data() const {
    if (_error) {
        return false;
    }
    if (_lookahead_exhausted && !_current.has_value() && _lookahead.empty()) {
        return false;
    }
    auto window = _pacer.window_target(std::numeric_limits<size_t>::max());
    return _cached_ahead_bytes < window;
}

void fetch_stream::on_consumed(
  kafka::offset up_to, size_t bytes, ss::lowres_clock::time_point now) {
    _last_active = now;
    _pacer.observe_consumed(bytes, now);
    // Clamp the released amount to what the stream actually held ahead of the
    // consumer so the broker reservation is never released below zero.
    size_t released = std::min(bytes, _cached_ahead_bytes);
    _cached_ahead_bytes -= released;
    if (up_to >= _demand_watermark) {
        _demand_watermark = kafka::next_offset(up_to);
    }
    // The reader has drained through `up_to`; release the pins on those offsets
    // so the shared cache can reclaim them again (consume -> unpin -> window
    // slides).
    unpin_through(up_to);
    _consumed_observer(released);
}

void fetch_stream::put_and_pin(const model::record_batch& b) {
    if (_l1_index == nullptr) {
        return;
    }
    _l1_index->put(b, storage::batch_cache::is_dirty_entry::no);
    if (_l1_index->pin(b.base_offset())) {
        _pinned_offsets.insert(b.base_offset());
    } else {
        vlog(
          _log.debug,
          "pin missed for offset {} (entry already evicted)",
          b.base_offset());
    }
}

std::optional<model::record_batch> fetch_stream::cached_get(model::offset o) {
    if (_l1_index == nullptr) {
        return std::nullopt;
    }
    return _l1_index->get(o);
}

bool fetch_stream::cached_contains(model::offset o) {
    return _l1_index != nullptr && _l1_index->has_contiguous_coverage(o, o);
}

void fetch_stream::testing_reclaim_cache(size_t size) {
    if (_l1_index != nullptr) {
        _l1_index->testing_reclaim_from_cache(size);
    }
}

void fetch_stream::unpin_through(kafka::offset up_to) {
    if (_l1_index == nullptr) {
        return;
    }
    // Offsets are pinned by base offset; unpin every pinned offset whose base
    // is at or before `up_to`. The reader reports consumption by the consumed
    // batch's last offset, so any batch whose base <= up_to is fully consumed.
    auto end = _pinned_offsets.upper_bound(kafka::offset_cast(up_to));
    for (auto it = _pinned_offsets.begin(); it != end; ++it) {
        // unpin() returning false is a harmless no-op (the offset may have been
        // naturally evicted after consumption); nothing to balance.
        _l1_index->unpin(*it);
    }
    _pinned_offsets.erase(_pinned_offsets.begin(), end);
}

void fetch_stream::unpin_all() {
    if (_l1_index != nullptr) {
        for (auto o : _pinned_offsets) {
            _l1_index->unpin(o);
        }
    }
    _pinned_offsets.clear();
}

void fetch_stream::on_demand(kafka::offset blocked_at) {
    _last_active = ss::lowres_clock::now();
    _demand_watermark = std::max(_demand_watermark, blocked_at);
    _demand_observer();
}

void fetch_stream::record_error(std::exception_ptr ex) {
    if (_error) {
        return;
    }
    // Record the error so memory_first_reader::error() is non-null.
    _error = ex;
    // Wake any blocked memory_first_reader immediately rather than letting it
    // hang until the fetch deadline.
    if (!_error_abort.abort_requested()) {
        _error_abort.request_abort();
    }
}

ss::future<> fetch_stream::close_current_object() {
    if (!_current.has_value()) {
        co_return;
    }
    co_await _current->downloader->close();
    _current.reset();
}

void fetch_stream::request_abort() {
    if (!_abort.abort_requested()) {
        _abort.request_abort();
    }
    if (!_error_abort.abort_requested()) {
        _error_abort.request_abort();
    }
}

ss::future<> fetch_stream::close() {
    _abort.request_abort();
    // Release every pin still held so no pin is leaked into the shared cache on
    // teardown (service stop / partition stop / LRU reclaim all funnel here).
    unpin_all();
    co_await close_current_object();
    _lookahead.clear();
}

} // namespace cloud_topics::prefetch
