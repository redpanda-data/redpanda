/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/l1_fetch_service.h"

#include "cloud_topics/batch_cache/batch_cache.h"
#include "cloud_topics/level_one/prefetch/memory_first_reader.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <algorithm>
#include <limits>

namespace cloud_topics::prefetch {

namespace {
ss::logger fslog("l1_fetch_service");

// When reserved bytes reach this percent of budget, only blocked-reader streams
// keep dispatching (shrink-coldest pressure response).
constexpr uint64_t k_pressure_pct = 90;

// How long get_reader gives the producer a head start before handing back the
// reader. The reader then performs the real, deadline-bounded wait in
// do_load_slice. Short so warm hits and empty fetches return promptly.
constexpr auto k_fill_timeout = std::chrono::milliseconds(50);
constexpr auto k_fill_poll = std::chrono::milliseconds(1);
} // namespace

l1_fetch_service::l1_fetch_service(
  l1::metastore* metastore,
  l1::io* io,
  cloud_topics::batch_cache* cache,
  config::binding<size_t> mem_budget,
  config::binding<size_t> max_streams,
  config::binding<std::chrono::milliseconds> idle_timeout,
  config::binding<size_t> max_in_flight,
  config::binding<size_t> fill_watermark,
  pacer_config pacer,
  cloud_topics::level_one_reader_probe* l1_reader_probe)
  : _metastore(metastore)
  , _io(io)
  , _cache(cache)
  , _l1_reader_probe(l1_reader_probe)
  , _mem_budget(std::move(mem_budget))
  , _max_streams(std::move(max_streams))
  , _idle_timeout(std::move(idle_timeout))
  , _max_in_flight(std::move(max_in_flight))
  , _fill_watermark(std::move(fill_watermark))
  , _pacer_config(pacer)
  , _broker(_mem_budget) {}

l1_fetch_service::~l1_fetch_service() = default;

ss::future<> l1_fetch_service::start() {
    ssx::spawn_with_gate(_gate, [this] { return dispatch_loop(); });
    co_return;
}

ss::future<> l1_fetch_service::stop() {
    _stopped = true;
    _as.request_abort();
    _dispatch_cv.broken();

    // Abort every stream's in-flight download and wake every attached reader so
    // nothing hangs. request_abort() is synchronous and safe to call alongside
    // an in-flight produce() (it only fires the abort sources).
    for (auto& [tidp, entries] : _registry) {
        for (auto& e : entries) {
            e->stream->request_abort();
        }
    }
    for (auto& e : _dying) {
        e->stream->request_abort();
    }

    // Drain the dispatch loop, all in-flight produce() tasks, and any pending
    // background close tasks. Entries stay alive (referenced by those tasks)
    // until this completes.
    co_await _gate.close();

    // Nothing references the entries anymore: reset chain state and drop them.
    for (auto& [tidp, entries] : _registry) {
        for (auto& e : entries) {
            co_await e->stream->close();
        }
    }
    for (auto& e : _dying) {
        co_await e->stream->close();
    }
    _registry.clear();
    _dying.clear();
}

l1_fetch_service::stream_entry* l1_fetch_service::find_reusable(
  const model::topic_id_partition& tidp, kafka::offset start) {
    auto it = _registry.find(tidp);
    if (it == _registry.end()) {
        return nullptr;
    }
    // A reader starting at `start` can reuse a forward-scanning stream when
    // `start` falls within the range the stream has already produced or is
    // about to produce next: [read_start, position()]. This covers the warm
    // consecutive-fetch case where the producer has read ahead of the consumer
    // (position() > start) as well as an exact-position resume. A `drained`
    // (exhausted) stream is still reusable for offsets it already produced into
    // the cache; only an errored stream is excluded.
    for (auto& e : it->second) {
        if (e->stream->error() != nullptr) {
            continue;
        }
        if (start >= e->read_start && start <= e->stream->position()) {
            return e.get();
        }
    }
    return nullptr;
}

bool l1_fetch_service::reclaim_one_idle() {
    // Find the LRU zero-ref idle stream across all partitions and evict it.
    entry_list* victim_list = nullptr;
    size_t victim_idx = 0;
    ss::lowres_clock::time_point victim_active{};
    bool found = false;
    auto now = ss::lowres_clock::now();
    auto timeout = _idle_timeout();
    for (auto& [tidp, entries] : _registry) {
        for (size_t i = 0; i < entries.size(); ++i) {
            auto& e = entries[i];
            if (e->in_flight) {
                continue;
            }
            if (e->stream->refs() != 0) {
                continue;
            }
            if (now - e->stream->last_active() < timeout) {
                continue;
            }
            if (!found || e->stream->last_active() < victim_active) {
                victim_list = &entries;
                victim_idx = i;
                victim_active = e->stream->last_active();
                found = true;
            }
        }
    }
    if (!found) {
        return false;
    }
    // Swap the victim to the back and pop it (chunked_vector has no middle
    // erase; element order in the per-partition list is not significant).
    auto& list = *victim_list;
    if (victim_idx != list.size() - 1) {
        std::swap(list[victim_idx], list.back());
    }
    auto victim = std::move(list.back());
    list.pop_back();

    if (victim->reserved > 0) {
        _broker.release(victim->reserved);
        victim->reserved = 0;
    }
    ssx::spawn_with_gate(
      _gate, [s = std::move(victim)](this auto) -> ss::future<> {
          co_await s->stream->close();
      });
    _probe.register_eviction();
    _probe.set_reserved_bytes(_broker.reserved());
    vlog(fslog.debug, "Reclaimed LRU idle stream");
    return true;
}

l1_fetch_service::stream_entry* l1_fetch_service::create_entry(
  model::topic_id_partition tidp, kafka::offset start) {
    // Enforce the cap, evicting LRU idle streams when possible.
    while (stream_count() >= _max_streams() && reclaim_one_idle()) {
    }

    auto e = std::make_unique<stream_entry>();
    e->read_start = start;
    e->stream = std::make_unique<fetch_stream>(
      tidp,
      start,
      _metastore,
      _io,
      _cache,
      prefetch_pacer(_pacer_config),
      _l1_reader_probe);
    auto* raw = e.get();
    raw->stream->set_consumed_observer(
      [this, raw](size_t bytes) { on_stream_consumed(raw, bytes); });
    raw->stream->set_demand_observer([this, raw] { on_stream_demand(raw); });
    raw->stream->set_detach_observer([this] { reap_dying(); });
    _registry[tidp].push_back(std::move(e));
    return raw;
}

void l1_fetch_service::reconcile_reservation(
  stream_entry* e, bool allow_borrow) {
    if (e->retired) {
        // A retired entry's reservation was already released in full; never
        // grow or shrink it again.
        return;
    }
    size_t target = e->stream->cached_ahead_bytes() + e->pending_chunk;
    if (target > e->reserved) {
        size_t delta = target - e->reserved;
        if (!_broker.try_reserve(delta)) {
            if (allow_borrow) {
                _broker.borrow(delta);
                _probe.register_borrow(delta);
            } else {
                // Cannot grow the reservation now; leave it as-is. The next
                // reconcile (on freed budget) will catch up.
                _probe.set_reserved_bytes(_broker.reserved());
                return;
            }
        }
        e->reserved += delta;
    } else if (target < e->reserved) {
        _broker.release(e->reserved - target);
        e->reserved = target;
    }
    _probe.set_reserved_bytes(_broker.reserved());
}

void l1_fetch_service::on_stream_consumed(stream_entry* e, size_t /*bytes*/) {
    // cached_ahead_bytes has already dropped inside the stream; reconcile the
    // broker reservation down to match it.
    reconcile_reservation(e, false);
    // Freed budget -> the dispatch loop may now admit more work.
    wake_dispatch();
}

void l1_fetch_service::on_stream_demand(stream_entry* e) {
    e->reader_blocked = true;
    _probe.register_demand_wait();
    wake_dispatch();
}

bool l1_fetch_service::is_blocked(const stream_entry& e) {
    // A reader is blocked while it has signalled demand and the producer has
    // not yet landed the demanded offset in the cache.
    return e.reader_blocked
           && e.stream->produced_through() < e.stream->demand_watermark();
}

ss::future<model::record_batch_reader> l1_fetch_service::get_reader(
  model::topic_id_partition tidp, cloud_topic_log_reader_config cfg) {
    // By-design limitation: the L1 read path serves data exclusively out of the
    // shared batch cache (the producer decodes batches into it and the reader
    // reads them back). If the batch cache is disabled on this shard
    // (disable_batch_cache=true) the produced batches are dropped and the
    // reader will not return data. This is an accepted product limitation, not
    // a fallback to direct object storage; surface it as a rate-limited warning
    // so operators can diagnose "L1 reads return nothing" rather than failing
    // silently.
    if (_cache != nullptr && !_cache->caching_enabled()) {
        thread_local static ss::logger::rate_limit rate(
          std::chrono::minutes(1));
        vloglr(
          fslog,
          ss::log_level::warn,
          rate,
          "L1 read for {} requires the batch cache, which is disabled; reads "
          "will not return data (set disable_batch_cache=false to enable L1 "
          "reads)",
          tidp);
    }

    auto start = cfg.start_offset;
    auto* e = find_reusable(tidp, start);
    // Reject a reused stream only when it has ALREADY produced past `start` but
    // `start` is no longer resident (consumed, then evicted): the producer
    // advanced past it and never re-produces, so spin a fresh stream to
    // re-download it (self-heal). Do NOT reject when the stream has not yet
    // reached `start` (start > produced_through) — it is still seeking toward
    // it and will land it in the cache; rejecting there would spawn a redundant
    // stream that re-seeks the same coarse-index pre-roll, churning streams and
    // exhausting the memory budget. The producer pins un-consumed prefetch, so
    // a produced-and-still-needed offset is always resident and reused here.
    // Residency is checked against the candidate stream's OWN index (each
    // stream caches the L1 objects it downloaded), not the shared per-partition
    // cache.
    if (
      e != nullptr && _cache != nullptr && _cache->caching_enabled()
      && start <= e->stream->produced_through()
      && !e->stream->cached_contains(kafka::offset_cast(start))) {
        e = nullptr;
    }
    if (e != nullptr) {
        _probe.register_cache_hit();
        vlog(fslog.debug, "get_reader warm hit {} @ {}", tidp, start);
    } else {
        _probe.register_cache_miss();
        vlog(fslog.debug, "get_reader cold miss {} @ {}", tidp, start);
        e = create_entry(tidp, start);
    }

    refresh_stream_gauges();

    // Kick off prefetch immediately.
    wake_dispatch();

    // Fill gate: give the producer a head start, then hand back the reader.
    // The reader performs the real, deadline-bounded wait in do_load_slice; a
    // timed-out / empty fetch still returns a reader so empty/EOS surfaces
    // through the normal reader path.
    //
    // Pin the stream for the entire duration of get_reader so that concurrent
    // notify_partition_stopped / reclaim_one_idle cannot free it while the
    // fill-gate poll loop is suspended. Both reap_dying() and
    // reclaim_one_idle() skip entries whose stream has refs() > 0. The pin is
    // released via defer on every exit path; make_memory_first_reader takes its
    // own independent ref in its ctor, so there is continuous ref coverage from
    // here to handback.
    auto* stream = e->stream.get();
    stream->ref();
    auto pin_guard = ss::defer([stream] { stream->unref(); });

    // The gate gives the producer a fixed ~50ms head start. It is NOT the
    // precise fetch deadline: honoring the Kafka request's max_wait_ms here
    // would require deadline plumbing from the Kafka fetch layer down through
    // cloud_topic_log_reader_config (which has no deadline field today); the
    // real deadline-bounded wait happens later in
    // memory_first_reader::do_load_slice. The gate is, however, abort-aware: an
    // aborted fetch (client cancel / shutdown) must not be made to wait the
    // full head start. Honoring a caller-supplied deadline at the gate is a
    // roadmap follow-up requiring that cross-layer plumbing.
    //
    // The fill watermark is the volume of decoded data that must be cached
    // ahead of `start` before handing the reader back. At the default of 1
    // byte this is "hand back as soon as the first batch covering `start` has
    // landed". A larger watermark trades a little first-byte latency for a
    // warmer reader (more data ready before the fetch path drains it).
    auto fill_watermark = _fill_watermark();
    auto deadline = model::timeout_clock::now() + k_fill_timeout;
    while (model::timeout_clock::now() < deadline) {
        if (stream->error() != nullptr) {
            break;
        }
        if (
          stream->produced_through() >= start
          && stream->cached_ahead_bytes() >= fill_watermark) {
            break;
        }
        if (e->drained) {
            break;
        }
        // Return promptly on an aborted fetch rather than burning the rest of
        // the head start; the reader handback path below still surfaces the
        // abort through the normal reader.
        if (
          cfg.abort_source.has_value()
          && cfg.abort_source.value().get().abort_requested()) {
            break;
        }
        try {
            co_await ss::sleep_abortable(k_fill_poll, _as);
        } catch (const ss::sleep_aborted&) {
            break;
        }
    }

    co_return make_memory_first_reader(stream, _cache, std::move(cfg));
}

void l1_fetch_service::retire_entry(entry_ptr e, std::exception_ptr err) {
    // Record the terminal error (so an attached reader throws) and abort the
    // in-flight downloader so it does not hang. request_abort() is safe to call
    // while a produce() is in flight.
    e->stream->set_terminal_error(err);
    e->stream->request_abort();
    if (e->reserved > 0) {
        _broker.release(e->reserved);
        e->reserved = 0;
    }
    e->pending_chunk = 0;
    e->retired = true;
    e->drained = true;
    _probe.register_eviction();
    _probe.set_reserved_bytes(_broker.reserved());

    auto* raw = e.get();
    raw->closing = true;
    // Hold the entry alive across the (async) close. A reader may still hold a
    // raw pointer to the stream, so the stream must outlive all its readers.
    _dying.push_back(std::move(e));
    ssx::spawn_with_gate(_gate, [this, raw](this auto) -> ss::future<> {
        // Wait for any in-flight produce() to finish before close() resets
        // the chain cursor (close_current_object must not race produce()).
        // The download was already aborted by request_abort() above, so this is
        // bounded.
        while (raw->in_flight && !_as.abort_requested()) {
            co_await ss::sleep(std::chrono::milliseconds(1));
        }
        co_await raw->stream->close();
        raw->closing = false;
        reap_dying();
    });
}

void l1_fetch_service::reap_dying() {
    // Reap entries with no attached reader, no in-flight produce(), and no
    // pending close task. chunked_vector has no middle erase, so compact
    // survivors into a new list.
    entry_list survivors;
    for (auto& e : _dying) {
        if (e->stream->refs() == 0 && !e->in_flight && !e->closing) {
            continue;
        }
        survivors.push_back(std::move(e));
    }
    _dying = std::move(survivors);
}

void l1_fetch_service::notify_partition_stopped(
  const model::topic_id_partition& tidp) {
    auto it = _registry.find(tidp);
    if (it == _registry.end()) {
        return;
    }
    auto entries = std::move(it->second);
    _registry.erase(it);
    auto err = std::make_exception_ptr(ss::abort_requested_exception());
    for (auto& e : entries) {
        retire_entry(std::move(e), err);
    }
    refresh_stream_gauges();
    vlog(
      fslog.debug, "notify_partition_stopped tore down streams for {}", tidp);
}

ss::future<> l1_fetch_service::dispatch_loop() {
    while (!_as.abort_requested()) {
        try {
            co_await _dispatch_cv.wait();
        } catch (const ss::broken_condition_variable&) {
            co_return;
        }
        reap_dying();
        // Drain as many dispatch passes as keep producing work.
        while (!_as.abort_requested() && dispatch_pass() > 0) {
            co_await ss::maybe_yield();
        }
    }
}

size_t l1_fetch_service::dispatch_pass() {
    size_t spawned = 0;
    while (_in_flight < _max_in_flight()) {
        auto* e = pick_stream();
        if (e == nullptr) {
            break;
        }
        bool blocked = is_blocked(*e);
        // Per-stream pacing cap: how much more this stream wants prefetched to
        // reach its adaptive window target. Derived from pacing, NOT the global
        // budget — the broker (try_reserve / borrow) is the budget gate, so the
        // window is not artificially shrunk by a tight budget.
        size_t window_remaining = e->stream->window_deficit(
          _pacer_config.max_window);
        // A blocked reader must always make progress even when already at its
        // window target, so floor the request at one min_chunk for it.
        if (window_remaining == 0 && blocked) {
            window_remaining = _pacer_config.min_chunk;
        }
        // Pass a large run-remaining hint; produce() re-clamps to the true run
        // remaining internally.
        size_t chunk = e->stream->next_chunk_size(
          std::numeric_limits<size_t>::max(), window_remaining);
        if (chunk == 0) {
            // Window full and reader not blocked: nothing to dispatch now.
            break;
        }
        // WITHIN-stream concurrency: split this stream's window deficit into as
        // many concurrent chunk GETs as fit, bounded by the remaining global
        // download slots AND the budget the broker can afford. The whole window
        // is reserved up front so the broker covers ALL the concurrent chunks'
        // in-flight buffers. A blocked stream is floored at one slot/chunk so
        // it always makes progress (borrowing beyond budget if needed).
        size_t slots_left = _max_in_flight() - _in_flight;
        size_t want_slots = std::max<size_t>(1, window_remaining / chunk);
        size_t slots = std::min(want_slots, slots_left);
        // Trim the request to what the budget can afford so a tight (but
        // non-empty) budget still dispatches a smaller window instead of
        // nothing.
        size_t affordable_slots = _broker.available() / chunk;
        slots = std::min(slots, std::max<size_t>(affordable_slots, 1));
        size_t window = chunk * slots;

        // Admission control: try to reserve the whole window against the
        // budget. A blocked reader may borrow beyond budget (anti-starvation).
        if (!_broker.try_reserve(window)) {
            if (blocked) {
                // For a starving stream borrow just one chunk to unblock it,
                // rather than borrowing a whole speculative window.
                slots = 1;
                window = chunk;
                _broker.borrow(window);
                _probe.register_borrow(window);
            } else {
                // Out of budget for non-starving streams: stop dispatching.
                break;
            }
        }
        e->reserved += window;
        e->pending_chunk = window;
        e->pending_slots = slots;
        _in_flight += slots;
        _probe.set_reserved_bytes(_broker.reserved());
        dispatch_one(e, window, slots, blocked);
        ++spawned;
    }
    return spawned;
}

l1_fetch_service::stream_entry* l1_fetch_service::pick_stream() {
    // Pressure threshold: when reserved is near budget, only blocked-reader
    // streams keep dispatching (shrink-coldest: cold streams stop growing).
    bool pressure = _broker.reserved() * 100
                    >= _broker.budget() * k_pressure_pct;

    stream_entry* best = nullptr;
    double best_score = -1.0;
    stream_entry* best_blocked = nullptr;

    for (auto& [tidp, entries] : _registry) {
        for (auto& e : entries) {
            if (e->in_flight || e->drained) {
                continue;
            }
            if (e->stream->error() != nullptr) {
                continue;
            }
            if (is_blocked(*e)) {
                // First priority: blocked-reader streams (anti-starvation),
                // regardless of window state. Pick the one blocked longest ago
                // (smallest last_active) for fairness.
                if (
                  best_blocked == nullptr
                  || e->stream->last_active()
                       < best_blocked->stream->last_active()) {
                    best_blocked = e.get();
                }
                continue;
            }
            if (!e->stream->needs_data()) {
                continue;
            }
            if (pressure) {
                // Under memory pressure cold (non-blocked) streams stop.
                continue;
            }
            // Deficit-weighted score: how far behind the window target this
            // stream is, weighted by its drain rate.
            double rate = e->stream->consume_rate_bps();
            double deficit = static_cast<double>(
              e->stream->window_deficit(std::numeric_limits<size_t>::max()));
            double score = deficit * (rate + 1.0);
            if (score > best_score) {
                best_score = score;
                best = e.get();
            }
        }
    }
    if (best_blocked != nullptr) {
        return best_blocked;
    }
    return best;
}

void l1_fetch_service::dispatch_one(
  stream_entry* e, size_t window, size_t slots, bool borrow) {
    e->in_flight = true;
    // _in_flight slots were already committed in dispatch_pass. Reflect each
    // concurrent download slot in the probe's in-flight gauge so peak_in_flight
    // captures within-stream concurrency.
    for (size_t i = 0; i < slots; ++i) {
        _probe.inc_in_flight();
        _probe.register_download();
    }

    ssx::spawn_with_gate(
      _gate, [this, e, window, slots, borrow](this auto) -> ss::future<> {
          auto fut = co_await ss::coroutine::as_future(
            e->stream->produce(window, slots));
          if (fut.failed()) {
              auto ex = fut.get_exception();
              vlog(fslog.warn, "produce failed: {}", ex);
          }
          // The in-flight window is done. Reconcile the reservation down to the
          // bytes actually landed in the cache (cached_ahead_bytes); the unused
          // remainder of the speculative window reservation is released. A
          // borrow-backed dispatch keeps its reservation if data landed (repaid
          // later on consume).
          e->pending_chunk = 0;
          e->pending_slots = 0;
          reconcile_reservation(e, borrow);
          if (e->stream->error() != nullptr || e->stream->is_exhausted()) {
              e->drained = true;
          }
          // Clear the blocked flag once the producer has caught up to (or
          // exhausted past) the demanded offset; the reader is woken by the
          // cache's offset monitor.
          if (
            e->stream->produced_through() >= e->stream->demand_watermark()
            || e->drained) {
              e->reader_blocked = false;
          }
          e->in_flight = false;
          _in_flight -= slots;
          for (size_t i = 0; i < slots; ++i) {
              _probe.dec_in_flight();
          }
          wake_dispatch();
      });
}

void l1_fetch_service::wake_dispatch() {
    if (!_stopped) {
        _dispatch_cv.signal();
    }
}

void l1_fetch_service::refresh_stream_gauges() {
    uint64_t active = 0;
    uint64_t idle = 0;
    for (auto& [tidp, entries] : _registry) {
        for (auto& e : entries) {
            if (e->stream->refs() > 0) {
                ++active;
            } else {
                ++idle;
            }
        }
    }
    _probe.set_active_streams(active);
    _probe.set_idle_streams(idle);
}

size_t l1_fetch_service::stream_count() const {
    size_t n = 0;
    for (auto& [tidp, entries] : _registry) {
        n += entries.size();
    }
    return n;
}

size_t
l1_fetch_service::stream_count(const model::topic_id_partition& tidp) const {
    auto it = _registry.find(tidp);
    if (it == _registry.end()) {
        return 0;
    }
    return it->second.size();
}

fetch_stream*
l1_fetch_service::stream_for_test(const model::topic_id_partition& tidp) {
    auto it = _registry.find(tidp);
    if (it == _registry.end() || it->second.empty()) {
        return nullptr;
    }
    return it->second.front()->stream.get();
}

size_t l1_fetch_service::dispatch_once_for_test() { return dispatch_pass(); }

ss::future<> l1_fetch_service::drain_in_flight_for_test() {
    while (_in_flight > 0) {
        co_await ss::sleep(std::chrono::milliseconds(1));
    }
}

} // namespace cloud_topics::prefetch
