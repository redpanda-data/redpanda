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

#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/log_reader_config.h"
#include "config/property.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <optional>

namespace cloud_topics {

/// Per-shard cache of L1 reader instances. Readers are cached between
/// fetches so that subsequent reads at the same offset can reuse the
/// positioned reader, its open object stream, and its lookahead metadata
/// buffer.
class l1_reader_cache {
public:
    struct stats {
        size_t in_use_readers;
        size_t cached_readers;
    };

    l1_reader_cache(
      config::binding<std::chrono::milliseconds> eviction_timeout,
      config::binding<size_t> target_max_size);

    l1_reader_cache(const l1_reader_cache&) = delete;
    l1_reader_cache& operator=(const l1_reader_cache&) = delete;
    l1_reader_cache(l1_reader_cache&&) = delete;
    l1_reader_cache& operator=(l1_reader_cache&&) = delete;

    ~l1_reader_cache();

    /// Look up a cached reader for the given partition and config.
    /// Returns a wrapped reader on hit, nullopt on miss.
    std::optional<model::record_batch_reader> get_reader(
      const model::topic_id_partition& tidp,
      const cloud_topic_log_reader_config& cfg);

    /// Wrap a newly-created reader so it will be returned to the cache
    /// when the caller is done with it.
    model::record_batch_reader
    put(std::unique_ptr<level_one_log_reader_impl> reader);

    stats get_stats() const;

    /// Invalidate cached readers for the given partition whose next read
    /// position falls within any of the ranges. This marks any matching in-use
    /// or idle readers as non-reusable.
    void invalidate_range(
      const model::topic_id_partition& tidp,
      const l1::offset_interval_set& evict_ranges);

    ss::future<> stop();

private:
    struct entry {
        model::record_batch_reader make_cached_reader(l1_reader_cache*);
        std::unique_ptr<level_one_log_reader_impl> reader;
        ss::lowres_clock::time_point last_used = ss::lowres_clock::now();
        safe_intrusive_list_hook _hook;
    };

    struct entry_guard {
        entry_guard(entry_guard&&) noexcept = default;
        entry_guard& operator=(entry_guard&&) noexcept = default;
        entry_guard(const entry_guard&) = delete;
        entry_guard& operator=(const entry_guard&) = delete;

        explicit entry_guard(entry* e, l1_reader_cache* c)
          : _e(e)
          , _cache(c) {}

        ~entry_guard() noexcept;

    private:
        entry* _e;
        l1_reader_cache* _cache;
    };

    ss::future<> maybe_evict();
    void maybe_evict_size();
    bool over_size_limit() const;
    void dispose_in_background(entry* e);
    ss::future<> wait_for_no_inuse_readers();
    void arm_eviction_timer();

    config::binding<std::chrono::milliseconds> _eviction_timeout;
    ss::gate _gate;
    ss::timer<ss::lowres_clock> _eviction_timer;

    counted_intrusive_list<entry, &entry::_hook> _readers;
    counted_intrusive_list<entry, &entry::_hook> _in_use;
    config::binding<size_t> _target_max_size;
    ss::condition_variable _in_use_reader_destroyed;

    // Probe counters
    uint64_t _cache_hits{0};
    uint64_t _cache_misses{0};
    uint64_t _readers_added{0};
    uint64_t _readers_evicted{0};
};

} // namespace cloud_topics
