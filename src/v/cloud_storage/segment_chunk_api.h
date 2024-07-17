/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/segment_chunk.h"
#include "container/fragmented_vector.h"
#include "model/metadata.h"
#include "random/simple_time_jitter.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

#include <absl/container/btree_map.h>

namespace cloud_storage {

class remote_segment;

class segment_chunks {
public:
    using chunk_map_t = absl::btree_map<chunk_start_offset_t, segment_chunk>;

    explicit segment_chunks(
      remote_segment& segment, uint64_t max_hydrated_chunks);

    segment_chunks(const segment_chunks&) = delete;
    segment_chunks& operator=(const segment_chunks&) = delete;
    segment_chunks(segment_chunks&&) = delete;
    segment_chunks& operator=(segment_chunks&&) = delete;
    virtual ~segment_chunks() = default;

    ss::future<> start();
    ss::future<> stop();

    bool downloads_in_progress() const;

    // Hydrates the given chunk id. The remote segment object is used for
    // hydration. The waiters are managed per chunk in `segment_chunk::waiters`.
    // The first reader to request hydration queues the download. The next
    // readers are added to wait list.
    ss::future<segment_chunk::handle_t> hydrate_chunk(
      chunk_start_offset_t chunk_start,
      std::optional<uint16_t> prefetch_override = std::nullopt);

    // For all chunks between first and last, increment the
    // required_by_readers_in_future value by one, and increment the
    // required_after_n_chunks values with progressively larger values to denote
    // how far in future the chunk will be required.
    void
    register_readers(chunk_start_offset_t first, chunk_start_offset_t last);

    // Mark the first chunk id as acquired by decrementing its
    // required_by_readers_in_future count, and decrement the
    // required_after_n_chunks counts for everything from [first, last] by one.
    // A chunk with required_by_readers_in_future count 0 which does not share
    // its handle with any data source is eligible for trimming.
    void mark_acquired_and_update_stats(
      chunk_start_offset_t first, chunk_start_offset_t last);

    // Returns reference to metadata for chunk for given chunk id
    segment_chunk& get(chunk_start_offset_t);

    chunk_start_offset_t get_next_chunk_start(chunk_start_offset_t f) const;

    using iterator_t = chunk_map_t::iterator;
    iterator_t begin();
    iterator_t end();

    /// Returns byte range (inclusive) for given chunk start offset. If the
    /// chunk is the last in segment, the second parameter is used for the end
    /// of the range. This is required because the chunk API only contains start
    /// offsets of chunks.
    std::pair<size_t, size_t> get_byte_range_for_chunk(
      chunk_start_offset_t start_offset, size_t last_byte_in_segment) const;

private:
    // Periodically closes chunk file handles for the space to be reclaimable by
    // cache eviction. The chunks are evicted when they are no longer opened for
    // reading by any readers. We also take into account any readers that may
    // need a chunk soon when evicting, by sorting on
    // `segment_chunk::required_by_readers_in_future` and
    // `segment_chunk::required_after_n_chunks` values.
    ss::future<> trim_chunk_files();

    /// Runs a continuous loop which checks and serves download requests.
    /// Modelled after remote segment bg loop.
    ss::future<> run_hydrate_bg();

    /// Hydrate a single chunk during an iteration of the bg loop. Uses remote
    /// segment to download the chunk and once finished notifies all waiters.
    ss::future<> do_hydrate_chunk(chunk_start_offset_t start_offset);

    /// Schedules prefetches when a chunk is downloaded, by calculating the next
    /// `prefetch` chunks and scheduling downloads by adding to the wait queue.
    /// If prefetch chunks are already hydrated or download is in progress, then
    /// we skip the download.
    void schedule_prefetches(
      chunk_start_offset_t start_offset, size_t n_chunks_to_prefetch);

    /// Periodically resolves prefetch futures. Since there is no explicit
    /// waiter for prefetch downloads, potential errors during these downloads
    /// must be consumed to avoid ignored-future warnings. This method inspects
    /// currently scheduled prefetches, and for those which are available,
    /// extracts exceptions if any.
    void resolve_prefetch_futures();

    /// The chunk map holds a mapping from file offset to chunk metadata. This
    /// struct is initialized when the object starts, and will never change
    /// after this during the lifetime of this object, IE no inserts or deletes
    /// are performed on the map. The metadata may change, specifically the
    /// chunk state or number of waiters. The map cannot be initialized when
    /// this object is constructed because we need the segment index to be
    /// available to ocnstruct the map.
    chunk_map_t _chunks;

    remote_segment& _segment;

    simple_time_jitter<ss::lowres_clock> _cache_backoff_jitter;

    simple_time_jitter<ss::lowres_clock> _eviction_jitter;
    ss::timer<ss::lowres_clock> _eviction_timer;

    ss::abort_source _as;
    ss::gate _gate;

    bool _started{false};

    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;

    uint64_t _max_hydrated_chunks;
    ss::condition_variable _bg_cvar;
    fragmented_vector<ss::future<segment_chunk::handle_t>> _prefetches;
};

class chunk_eviction_strategy {
public:
    chunk_eviction_strategy() = default;
    chunk_eviction_strategy(const chunk_eviction_strategy&) = delete;
    chunk_eviction_strategy(chunk_eviction_strategy&&) = delete;
    chunk_eviction_strategy& operator=(const chunk_eviction_strategy&) = delete;
    chunk_eviction_strategy& operator=(chunk_eviction_strategy&&) = delete;

    virtual ~chunk_eviction_strategy() = default;

    virtual ss::future<> evict(
      std::vector<segment_chunks::chunk_map_t::iterator> chunks,
      retry_chain_logger& rtc)
      = 0;

protected:
    virtual ss::future<> close_files(
      std::vector<ss::lw_shared_ptr<ss::file>> files_to_close,
      retry_chain_logger& rtc);
};

class eager_chunk_eviction_strategy : public chunk_eviction_strategy {
public:
    ss::future<> evict(
      std::vector<segment_chunks::chunk_map_t::iterator> chunks,
      retry_chain_logger& rtc) override;
};

class capped_chunk_eviction_strategy : public chunk_eviction_strategy {
public:
    capped_chunk_eviction_strategy(
      uint64_t max_chunks, uint64_t hydrated_chunks);

    ss::future<> evict(
      std::vector<segment_chunks::chunk_map_t::iterator> chunks,
      retry_chain_logger& rtc) override;

private:
    uint64_t _max_chunks;
    uint64_t _hydrated_chunks;
};

class predictive_chunk_eviction_strategy : public chunk_eviction_strategy {
public:
    predictive_chunk_eviction_strategy(
      uint64_t max_chunks, uint64_t hydrated_chunks);

    ss::future<> evict(
      std::vector<segment_chunks::chunk_map_t::iterator> chunks,
      retry_chain_logger& rtc) override;

private:
    uint64_t _max_chunks;
    uint64_t _hydrated_chunks;
};

std::unique_ptr<chunk_eviction_strategy> make_eviction_strategy(
  model::cloud_storage_chunk_eviction_strategy k,
  uint64_t max_chunks,
  uint64_t hydrated_chunks);

} // namespace cloud_storage
