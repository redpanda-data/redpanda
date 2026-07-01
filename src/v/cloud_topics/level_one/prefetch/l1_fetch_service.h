/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/prefetch/fetch_stream.h"
#include "cloud_topics/level_one/prefetch/l1_memory_broker.h"
#include "cloud_topics/level_one/prefetch/prefetch_pacer.h"
#include "cloud_topics/level_one/prefetch/prefetch_probe.h"
#include "cloud_topics/log_reader_config.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <memory>

namespace cloud_topics {
class batch_cache;
class level_one_reader_probe;
} // namespace cloud_topics

namespace cloud_topics::prefetch {

/// Per-shard scheduler + registry + memory-broker owner for the L1 prefetch
/// service. This is the only global decision-maker on the shard: it owns the
/// lifetime of every `fetch_stream`, arbitrates the per-shard memory budget
/// across them, and drives many concurrent downloads to hide fetch latency.
///
/// Concurrency model (spec §7): a single background dispatch fiber selects
/// streams by priority and spawns each `produce()` as a gated background task.
/// Concurrency is measured in download SLOTS — one slot per in-flight chunk
/// GET. A single `produce()` call may consume several slots: it dispatches that
/// many chunk GETs of its window CONCURRENTLY within its current run. Total
/// slots across all streams are bounded by `max_in_flight`; the per-stream slot
/// allocation gives WITHIN-stream concurrency (one consumer scanning one
/// partition gets throughput scaling with concurrency/bandwidth). The memory
/// budget bounds the bytes reserved for all concurrently in-flight chunks. Only
/// one `produce()` runs per stream at a time (it is not concurrent-safe per
/// instance); the within-stream concurrency lives inside that single call.
///
/// The dispatch fiber is woken on: new reader demand (on_demand), consumption
/// (reservation freed), and produce() completion.
class l1_fetch_service {
public:
    l1_fetch_service(
      l1::metastore* metastore,
      l1::io* io,
      cloud_topics::batch_cache* cache,
      config::binding<size_t> mem_budget,
      config::binding<size_t> max_streams,
      config::binding<std::chrono::milliseconds> idle_timeout,
      config::binding<size_t> max_in_flight,
      config::binding<size_t> fill_watermark,
      pacer_config pacer,
      cloud_topics::level_one_reader_probe* l1_reader_probe = nullptr);

    l1_fetch_service(const l1_fetch_service&) = delete;
    l1_fetch_service(l1_fetch_service&&) = delete;
    l1_fetch_service& operator=(const l1_fetch_service&) = delete;
    l1_fetch_service& operator=(l1_fetch_service&&) = delete;
    ~l1_fetch_service();

    ss::future<> start();
    ss::future<> stop();

    /// Get-or-create a stream for `(tidp, cfg.start_offset)`, kick off
    /// prefetch, wait for the first-fill watermark OR the fetch deadline, then
    /// return a memory_first_reader over that stream. A timed-out / empty fetch
    /// still returns a reader so the empty/EOS result surfaces through the
    /// normal reader path.
    ss::future<model::record_batch_reader> get_reader(
      model::topic_id_partition tidp, cloud_topic_log_reader_config cfg);

    /// Tear down every stream for `tidp`: abort attached readers (via the
    /// stream error/abort path), close downloaders, release reservations and
    /// delete the streams. Hooked into partition stop / leadership loss.
    void notify_partition_stopped(const model::topic_id_partition& tidp);

    /// Test accessors -----------------------------------------------------
    prefetch_probe& probe() { return _probe; }
    l1_memory_broker& broker() { return _broker; }
    size_t stream_count() const;
    size_t stream_count(const model::topic_id_partition& tidp) const;
    /// Drive a single dispatch pass synchronously (test hook). Returns the
    /// number of produce() tasks spawned this pass.
    size_t dispatch_once_for_test();
    /// Wait until no produce() task is in flight (test hook).
    ss::future<> drain_in_flight_for_test();

    /// Test hook: the first registered stream for \p tidp, or nullptr. Lets
    /// tests inspect the per-stream L1 index (cached_get / cached_contains /
    /// reclaim) that each stream now owns.
    fetch_stream* stream_for_test(const model::topic_id_partition& tidp);

private:
    /// One registry entry: the owned stream plus the per-stream bookkeeping the
    /// scheduler needs (its reservation against the broker and whether a
    /// produce() is currently in flight for it).
    struct stream_entry {
        std::unique_ptr<fetch_stream> stream;
        // The offset this stream was created to read from. A reader starting at
        // `start` can reuse this stream when start in [read_start, position()].
        kafka::offset read_start;
        // Bytes this stream currently has reserved against the broker. Kept
        // equal to `stream->cached_ahead_bytes() + pending_chunk` by
        // reconcile_reservation, so it is robust to the reader's on_consumed
        // racing with produce() completion.
        size_t reserved{0};
        // Bytes reserved for the in-flight produce() window currently running
        // for this stream, covering ALL the concurrent chunk GETs it dispatches
        // (0 when no produce() is running). This is the reservation that backs
        // the in-flight buffers of every concurrent chunk of the stream.
        size_t pending_chunk{0};
        // Download slots committed to the in-flight produce() for this stream
        // (one per concurrent chunk GET it may dispatch). Counted against the
        // service-wide `_in_flight` slot total / `max_in_flight` cap and
        // released on produce() completion.
        size_t pending_slots{0};
        // True while a produce() task is running for this stream. Enforces the
        // per-stream "one produce() at a time" invariant.
        bool in_flight{false};
        // True while a background close() task still references this entry, so
        // the reaper does not delete it out from under that task.
        bool closing{false};
        // True once the entry has been retired (partition stop / service stop):
        // its reservation is released and must never be grown again.
        bool retired{false};
        // True once the producer reported no more data / a terminal error, so
        // the scheduler stops trying to dispatch it.
        bool drained{false};
        // True while a reader is blocked waiting for data this stream has not
        // produced yet. Set by the demand observer, cleared once the producer
        // catches up to the demanded offset. Drives anti-starvation priority
        // and the borrow decision.
        bool reader_blocked{false};
    };

    using entry_ptr = std::unique_ptr<stream_entry>;
    using entry_list = chunked_vector<entry_ptr>;

    // Registry: per-partition list of streams. Normally one entry per tidp;
    // multiple appear only for concurrent readers at different positions.
    chunked_hash_map<model::topic_id_partition, entry_list> _registry;

    // Streams removed from the registry (partition stop / leadership loss) that
    // still have attached readers (refs > 0). Their downloader is closed and
    // reservation released, but the fetch_stream object is kept alive until the
    // last reader detaches so the reader's raw pointer never dangles. Reaped
    // lazily once refs() hits zero.
    entry_list _dying;

    // Find a reusable stream for (tidp, start) or nullptr.
    stream_entry*
    find_reusable(const model::topic_id_partition& tidp, kafka::offset start);

    // Create a new stream entry, install the consumed observer, enforce the cap
    // (evicting an LRU idle stream if needed), and return it.
    stream_entry*
    create_entry(model::topic_id_partition tidp, kafka::offset start);

    // Reclaim one LRU zero-ref idle stream if over the cap. Returns true if a
    // stream was evicted.
    bool reclaim_one_idle();

    // Background dispatch loop: parks on _dispatch_cv, then runs dispatch
    // passes until no more work can be admitted.
    ss::future<> dispatch_loop();

    // Run one dispatch pass: while in_flight < max && budget allows, pick the
    // best stream and spawn a produce() task for it. Returns spawned count.
    size_t dispatch_pass();

    // Pick the next stream to dispatch, honouring priority (blocked-reader
    // streams first, then largest deficit weighted by drain rate) and memory
    // pressure (skip coldest streams when near budget). Returns nullptr when
    // nothing should be dispatched right now.
    stream_entry* pick_stream();

    // Spawn a single produce() for `e` driving `slots` concurrent chunk GETs
    // over `window` bytes, with `e->pending_chunk`/`e->pending_slots` already
    // reserved by the caller (or borrowed for a starving stream). Reconciles
    // the reservation against the actual produced bytes on completion, releases
    // the slots, and wakes the dispatch loop.
    void
    dispatch_one(stream_entry* e, size_t window, size_t slots, bool borrow);

    // Reconcile `e->reserved` to the single source of truth:
    // `stream->cached_ahead_bytes() + e->pending_chunk`, adjusting the broker
    // by the delta (try_reserve to grow, release to shrink; a blocked stream
    // may borrow to grow). This is the only place broker reservation changes
    // for a stream after creation, so the reader's on_consumed and produce()
    // completion can both call it without double-counting.
    void reconcile_reservation(stream_entry* e, bool allow_borrow);

    // Release `bytes` of `e`'s reservation back to the broker (called from the
    // stream's consumed observer).
    void on_stream_consumed(stream_entry* e, size_t bytes);

    // Mark `e` blocked because a reader is waiting (called from the stream's
    // demand observer) and wake the dispatch loop.
    void on_stream_demand(stream_entry* e);

    // True when a reader is blocked on data this stream has not produced yet.
    static bool is_blocked(const stream_entry& e);

    // Move an entry out of the registry into _dying: close its downloader,
    // release its reservation, wake any attached reader with `err`. If the
    // stream has no attached reader it is deleted immediately; otherwise it is
    // held in _dying until the last reader detaches.
    void retire_entry(entry_ptr e, std::exception_ptr err);

    // Delete dying streams whose readers have all detached (refs() == 0).
    void reap_dying();

    void wake_dispatch();
    void refresh_stream_gauges();

    l1::metastore* _metastore;
    l1::io* _io;
    cloud_topics::batch_cache* _cache;
    // Per-shard L1 reader metrics, reused (footer/read/skipped bytes) by the
    // prefetch producer via each fetch_stream. May be null in tests.
    cloud_topics::level_one_reader_probe* _l1_reader_probe;
    config::binding<size_t> _mem_budget;
    config::binding<size_t> _max_streams;
    config::binding<std::chrono::milliseconds> _idle_timeout;
    config::binding<size_t> _max_in_flight;
    // Decoded bytes that must be cached ahead of `start` before the fill gate
    // hands the memory_first_reader back to the Kafka fetch path. Default 1
    // means "hand back on the first batch"; a higher value trades a little
    // first-byte latency for a warmer reader.
    config::binding<size_t> _fill_watermark;
    pacer_config _pacer_config;

    l1_memory_broker _broker;
    prefetch_probe _probe;

    // Total download slots in flight across all streams (one per concurrent
    // chunk GET). Bounded by `max_in_flight`. A single stream's produce() may
    // hold several slots, giving within-stream concurrency.
    size_t _in_flight{0};

    ss::gate _gate;
    ss::abort_source _as;
    ss::condition_variable _dispatch_cv;
    bool _stopped{false};
};

} // namespace cloud_topics::prefetch
