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
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/prefetch/chunk_downloader.h"
#include "cloud_topics/level_one/prefetch/prefetch_pacer.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <absl/container/btree_set.h>

#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <optional>

namespace storage {
class batch_cache_index;
} // namespace storage

namespace cloud_topics {
class batch_cache;
class level_one_reader_probe;
} // namespace cloud_topics

namespace cloud_topics::prefetch {

/// The producer core of the L1 prefetch service for ONE read position
/// `(tidp, start)`.
///
/// A `fetch_stream` owns the prefetch chain for a single read position:
///   metastore lookahead -> footer prefetch -> drive a `chunk_downloader`
///   over the run(s) -> land decoded batches in the shared `batch_cache`.
///
/// It is a *driven* producer: the per-shard service calls `produce()`
/// repeatedly to advance the chain ahead of consumer demand. Each call walks
/// the chain as needed, then issues up to a window's worth of chunk GETs
/// CONCURRENTLY within the current run (bounded by a caller-supplied
/// concurrency cap) and drains the decoded batches into the cache in byte
/// order. The call is resumable across invocations via the internal chain
/// cursor.
///
/// Concurrency: a single `produce()` may have several chunk GETs in flight at
/// once, so one consumer scanning one partition gets throughput that scales
/// with concurrency/bandwidth. Only ONE `produce()` may run per stream at a
/// time (the chain cursor and reassembler are not concurrent-safe across
/// calls); the within-run concurrency lives inside a single call.
///
/// The stream does not allocate memory or schedule itself; it merely requests
/// work and reports its state. The service (Task 7) owns lifetime, scheduling,
/// and the memory budget.
class fetch_stream {
public:
    fetch_stream(
      model::topic_id_partition tidp,
      kafka::offset start,
      l1::metastore* metastore,
      l1::io* io,
      cloud_topics::batch_cache* cache,
      prefetch_pacer pacer,
      cloud_topics::level_one_reader_probe* l1_reader_probe = nullptr);

    fetch_stream(const fetch_stream&) = delete;
    fetch_stream(fetch_stream&&) = delete;
    fetch_stream& operator=(const fetch_stream&) = delete;
    fetch_stream& operator=(fetch_stream&&) = delete;
    ~fetch_stream();

    /// The topic+partition this stream serves.
    const model::topic_id_partition& tidp() const { return _tidp; }

    /// The next offset that will be produced into the cache.
    kafka::offset position() const { return _next_offset; }

    /// The highest contiguous offset already landed in the cache. Before any
    /// data is produced this is `prev_offset(start)`.
    kafka::offset produced_through() const { return _produced_through; }

    /// Fetch a batch this stream produced into its own L1 index, if present.
    /// The memory-first reader consults this first (current post-compaction L1
    /// data, with ghost batches for gaps) and only falls back to the shared
    /// per-partition cache for offsets this stream has not produced. Returns
    /// nullopt on a miss or when caching is disabled.
    std::optional<model::record_batch> cached_get(model::offset o);

    /// Residency probe on this stream's own L1 index: true when a valid batch
    /// covering \p o is present. Used by the service's reuse gate so a stream
    /// is only reused when the requested offset is still resident in its index.
    bool cached_contains(model::offset o);

    /// Advance the prefetch chain, walking it as needed: ensure the lookahead
    /// buffer is populated, ensure the current object's footer is fetched and a
    /// downloader is open, then dispatch up to `window_budget` bytes of data
    /// chunks from the current run cursor CONCURRENTLY (each pacer-sized and
    /// clamped to the run remaining), with at most `max_concurrent` GETs in
    /// flight at once. Once the dispatched GETs land, drain the decoded batches
    /// into the cache in byte order and advance the position / produced_through
    /// watermarks. At a run/object boundary the downloader is closed and the
    /// next object is taken from the lookahead.
    ///
    /// All chunks issued by one call belong to the same run; the call does not
    /// cross a run boundary mid-flight so the reassembler's contiguous decode
    /// and slack carry-over stay well defined.
    ///
    /// A terminal IO error is recorded against the affected offset range (see
    /// `error()`); the reader's wait observes it rather than hanging. Once an
    /// error is recorded no further progress is made.
    ///
    /// Not concurrent-safe per instance: the scheduler keeps at most one
    /// `produce()` in flight per stream.
    ss::future<> produce(size_t window_budget, size_t max_concurrent);

    /// True when the volume of produced-but-unconsumed data is below the
    /// pacer's adaptive window target, i.e. the stream should be driven
    /// further ahead of demand.
    bool needs_data() const;

    /// True once the producer has reached the end of the available data: no
    /// current object, an empty lookahead and the metastore reported nothing
    /// further. The scheduler stops dispatching an exhausted stream.
    bool is_exhausted() const {
        return _lookahead_exhausted && !_current.has_value()
               && _lookahead.empty();
    }

    /// Scheduler query: the adaptive prefetch window target in bytes, capped at
    /// `reservation_cap`. Delegates to the stream's pacer.
    size_t window_target(size_t reservation_cap) const {
        return _pacer.window_target(reservation_cap);
    }

    /// Scheduler query: how many bytes the stream is below its window target,
    /// i.e. the prefetch deficit used to rank streams. Zero when at/over the
    /// target.
    size_t window_deficit(size_t reservation_cap) const {
        auto target = _pacer.window_target(reservation_cap);
        return _cached_ahead_bytes >= target ? 0 : target - _cached_ahead_bytes;
    }

    /// Scheduler query: the next download chunk size in bytes, computed by the
    /// stream's pacer and clamped to the remaining reservation. `run_remaining`
    /// is a hint; produce() re-clamps to the true run remaining internally.
    size_t
    next_chunk_size(size_t run_remaining, size_t reservation_remaining) const {
        return _pacer.next_chunk_size(run_remaining, reservation_remaining);
    }

    /// Scheduler query: current EWMA consume rate in bytes/s, used to weight
    /// the dispatch deficit by drain rate.
    double consume_rate_bps() const { return _pacer.consume_rate_bps(); }

    /// Reader-side push feedback: data up to `up_to` (inclusive) has been
    /// consumed, totalling `bytes`. Feeds the pacer's consume-rate EWMA and
    /// shrinks the cached-ahead window.
    void on_consumed(
      kafka::offset up_to, size_t bytes, ss::lowres_clock::time_point now);

    /// Service-side observer invoked from on_consumed() with the number of
    /// bytes whose cached-ahead reservation can be released back to the memory
    /// broker. The argument is clamped to the bytes the stream actually held
    /// ahead of the consumer, so the service can release exactly that much. The
    /// service installs this so the reader's push feedback (on_consumed) frees
    /// the per-shard memory budget without the service polling each stream.
    void set_consumed_observer(std::function<void(size_t)> obs) {
        _consumed_observer = std::move(obs);
    }

    /// Reader-side push feedback: a reader blocked waiting for `blocked_at`.
    /// Records the demand watermark used by the scheduler for anti-starvation.
    void on_demand(kafka::offset blocked_at);

    /// Service-side observer invoked from on_demand() so the scheduler wakes
    /// its dispatch loop when a reader blocks (anti-starvation). Without this
    /// the scheduler could park while a reader waits for data that is not being
    /// produced. Default no-op until the service installs one.
    void set_demand_observer(std::function<void()> obs) {
        _demand_observer = std::move(obs);
    }

    /// The frontier-most offset a reader has blocked on, or `start` if none.
    kafka::offset demand_watermark() const { return _demand_watermark; }

    /// Bytes produced into the cache but not yet reported consumed. The service
    /// reconciles its broker reservation against this after each produce().
    size_t cached_ahead_bytes() const { return _cached_ahead_bytes; }

    /// Observability/test accessor: bytes dispatched as concurrent chunk GETs
    /// but not yet decoded into the cache (in-flight = dispatched-but-not-yet-
    /// decoded). This is NOT the budget mechanism — the scheduler reserves the
    /// whole window up front in dispatch_pass before calling produce(); this
    /// counter is only useful for test assertions and metrics.
    size_t in_flight_bytes() const { return _in_flight_bytes; }

    /// Observability/test accessor: number of chunk GETs currently in flight
    /// for this stream (0 when no downloader is open). Not read by the
    /// scheduler for budget decisions; exposed so tests can assert within-
    /// stream concurrency is actually happening.
    size_t in_flight_downloads() const {
        return _current.has_value() ? _current->downloader->in_flight() : 0;
    }

    /// The terminal error recorded for this stream, if any.
    std::exception_ptr error() const { return _error; }

    // Lifetime --------------------------------------------------------------

    void ref() { ++_refs; }
    void unref() {
        --_refs;
        if (_refs == 0 && _detach_observer) {
            _detach_observer();
        }
    }
    long refs() const { return _refs; }

    /// Service-side observer invoked from unref() when the last reader
    /// detaches (refs drops to zero). Used by the service to trigger
    /// reap_dying() immediately rather than waiting for the next dispatch.
    void set_detach_observer(std::function<void()> obs) {
        _detach_observer = std::move(obs);
    }

    ss::lowres_clock::time_point last_active() const { return _last_active; }

    /// True when this stream's position matches `offset`, so it can be reused
    /// by a reader starting at that offset.
    bool reusable_at(kafka::offset offset) const {
        return _next_offset == offset;
    }

    /// Request abort of the active downloader and wake any blocked reader,
    /// synchronously and without touching the chain cursor. Safe to call while
    /// a produce() is in flight (it only fires the abort sources the
    /// downloader and reader observe). The service uses this on shutdown to
    /// unblock in-flight downloads before awaiting them, then calls close()
    /// once they have drained.
    void request_abort();

    /// Abort the active downloader and reset the chain state.
    ss::future<> close();

    /// Abort source that is fired when record_error() records a terminal
    /// error. The memory_first_reader subscribes to this so that a producer
    /// error wakes the reader immediately rather than waiting for the fetch
    /// deadline.
    ss::abort_source& error_abort_source() { return _error_abort; }

    /// Record a terminal error against the stream from outside the producer
    /// (e.g. the service's teardown path on partition stop / leadership loss).
    /// Equivalent to the producer hitting a fatal IO error: fires
    /// error_abort_source() so any blocked memory_first_reader wakes
    /// immediately, and sets error() so the reader surfaces the failure rather
    /// than hanging.
    void set_terminal_error(std::exception_ptr ex) {
        record_error(std::move(ex));
    }

    /// Test helper: expose the bytes-produced-but-not-yet-consumed counter so
    /// tests can verify that on_consumed receives deltas (not cumulative
    /// totals).
    size_t cached_ahead_bytes_for_test() const { return _cached_ahead_bytes; }

    /// Test helper: number of offsets this stream currently holds pinned in its
    /// own L1 index (produced-but-not-yet-consumed prefetch).
    size_t pinned_offsets_for_test() const { return _pinned_offsets.size(); }

    /// Test helper: drive the shared storage reclaimer through this stream's
    /// own index, so tests can assert pinned prefetch survives an eviction pass
    /// and unpinned/consumed batches do not.
    void testing_reclaim_cache(size_t size);

    /// Test helper: inject a batch directly into this stream's own L1 index
    /// (and pin it), exactly as produce() would land it. Lets reader tests
    /// pre-fill the stream without driving the full metastore/download chain.
    void testing_put(const model::record_batch& b) { put_and_pin(b); }

private:
    // Ensure the lookahead buffer has at least one object covering or beyond
    // `_next_offset`. Discards stale entries. Issues a metastore query when
    // the buffer is empty. Returns false when no more data exists.
    ss::future<bool> ensure_lookahead();

    // Ensure the current object's footer is fetched, a downloader is open and
    // the run cursor is positioned for `_next_offset`. Returns false when the
    // object has no data for the offset (e.g. compaction) and the object was
    // skipped.
    ss::future<bool> ensure_current_object();

    // Close and drop the current object's downloader/footer state.
    ss::future<> close_current_object();

    // Unpin every pinned offset at or before `up_to` (inclusive) and drop it
    // from the tracking set. Called from on_consumed as the reader drains.
    void unpin_through(kafka::offset up_to);

    // Unpin and forget every offset still pinned. Called on teardown so no pin
    // is leaked into the shared batch_cache.
    void unpin_all();

    // Put a produced batch into this stream's own L1 index and pin it in the
    // same synchronous span so the reclaimer cannot drop un-consumed prefetch
    // before the pin lands. Tracks the offset for unpin_through/unpin_all.
    // No-op when caching is disabled.
    void put_and_pin(const model::record_batch& b);

    void record_error(std::exception_ptr);

    // Active state for the object currently being downloaded.
    struct current_object {
        l1::object_id oid;
        kafka::offset last_offset;
        std::unique_ptr<chunk_downloader> downloader;
        // Byte position of the next chunk to dispatch within the object.
        size_t run_cursor{0};
        // Byte position one past the end of the partition run.
        size_t run_end{0};
    };

    model::topic_id_partition _tidp;
    kafka::offset _next_offset;
    kafka::offset _produced_through;
    l1::metastore* _metastore;
    l1::io* _io;
    cloud_topics::batch_cache* _cache;
    // This stream's own L1 batch index, backed by the shared shard cache. The
    // stream produces the L1 objects it downloads into this index, so it always
    // serves the current (post-compaction) data and never a stale batch left in
    // the shared per-partition cache by a prior reader or the write path. Null
    // when caching is disabled. Frees its ranges on destruction.
    std::unique_ptr<storage::batch_cache_index> _l1_index;
    prefetch_pacer _pacer;
    // Per-shard L1 reader metrics (reused from the pre-prefetch reader). Counts
    // footer/read/skipped bytes fetched from L1 objects. May be null in tests.
    cloud_topics::level_one_reader_probe* _l1_reader_probe;
    prefix_logger _log;

    // Lookahead buffer of object metadata, ascending by offset. A deque so
    // consumed entries can be popped from the front as the chain advances.
    std::deque<l1::metastore::object_response> _lookahead;
    // True once a metastore query reported no more objects beyond the buffer.
    bool _lookahead_exhausted{false};

    std::optional<current_object> _current;

    // Bytes produced into the cache but not yet reported consumed.
    size_t _cached_ahead_bytes{0};

    // Offsets (data and ghost batch base offsets, in model space) pinned in
    // this stream's own L1 index for batches it has produced but the reader has
    // not yet consumed. Pins keep the LRU reclaimer from dropping
    // un-consumed prefetch; without them an eviction forces a cold-miss re-seek
    // that re-downloads the coarse-index pre-roll and collapses throughput. A
    // sorted set so unpin_through() can release a contiguous consumed prefix.
    absl::btree_set<model::offset> _pinned_offsets;

    // Bytes dispatched as concurrent chunk GETs but not yet decoded into the
    // cache. Incremented when produce() dispatches a chunk and decremented when
    // the dispatched bytes are drained out of the reassembler into the cache.
    size_t _in_flight_bytes{0};

    kafka::offset _demand_watermark;
    // Service-installed hook called from on_consumed() with the bytes whose
    // reservation can be released. Default no-op until the service installs
    // one.
    std::function<void(size_t)> _consumed_observer{[](size_t) {}};
    // Service-installed hook called from on_demand() so the scheduler wakes
    // when a reader blocks. Default no-op until the service installs one.
    std::function<void()> _demand_observer{[]() {}};
    // Service-installed hook called from unref() when refs drops to zero.
    // Default empty (no-op) until the service installs one.
    std::function<void()> _detach_observer;
    long _refs{0};
    ss::lowres_clock::time_point _last_active{ss::lowres_clock::now()};

    std::exception_ptr _error;

    ss::abort_source _abort;
    // Fired by record_error() so any blocked memory_first_reader wakes
    // immediately rather than waiting for the fetch deadline.
    ss::abort_source _error_abort;
};

} // namespace cloud_topics::prefetch
