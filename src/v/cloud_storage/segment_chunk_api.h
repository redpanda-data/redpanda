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
#include "model/metadata.h"
#include "random/simple_time_jitter.h"
#include "utils/retry_chain_node.h"

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

private:
    // Attempts to download chunk into cache and return the file handle for
    // segment_chunk. Should be retried if there is a failure due to cache
    // eviction between download and opening the file handle.
    ss::future<ss::file> do_hydrate_and_materialize(
      chunk_start_offset_t chunk_start,
      std::optional<uint16_t> prefetch_override = std::nullopt);

    // Periodically closes chunk file handles for the space to be reclaimable by
    // cache eviction. The chunks are evicted when they are no longer opened for
    // reading by any readers. We also take into account any readers that may
    // need a chunk soon when evicting, by sorting on
    // `segment_chunk::required_by_readers_in_future` and
    // `segment_chunk::required_after_n_chunks` values.
    ss::future<> trim_chunk_files();

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

class segment_chunk_range {
public:
    using map_t = absl::
      btree_map<chunk_start_offset_t, std::optional<chunk_start_offset_t>>;

    segment_chunk_range(
      const segment_chunks::chunk_map_t& chunks,
      size_t prefetch,
      chunk_start_offset_t start);

    std::optional<chunk_start_offset_t> last_offset() const;
    chunk_start_offset_t first_offset() const;
    size_t chunk_count() const;

    map_t::iterator begin();
    map_t::iterator end();

private:
    map_t _chunks;
};

} // namespace cloud_storage
