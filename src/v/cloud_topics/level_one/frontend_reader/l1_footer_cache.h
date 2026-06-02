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

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <memory>
#include <optional>

namespace cloud_topics {
class level_one_reader_probe;
}

namespace cloud_topics::l1 {

/// Per-shard LRU cache of parsed l1::footer structs keyed by l1::object_id.
///
/// Footers are stored as `ss::lw_shared_ptr<const footer>` so cache hits
/// return a refcount bump rather than a deep copy. The cache retains
/// ownership. Eliminates duplicate footer DMA and parse work when multiple
/// readers on the same shard touch the same L1 object.
class l1_footer_cache {
public:
    using footer_ptr = ss::lw_shared_ptr<const footer>;

    struct stats {
        size_t cached_footers;
    };

    /// The optional `probe` is used to publish the cache's current size as
    /// a metric. When provided, the cache registers a size-getter on the
    /// probe at construction and resets it back to the default on
    /// destruction; this keeps the metric callback safe across the
    /// shutdown window where the cache is torn down before the probe.
    l1_footer_cache(
      config::binding<std::chrono::milliseconds> eviction_timeout,
      config::binding<size_t> target_max_size,
      level_one_reader_probe* probe = nullptr);

    l1_footer_cache(const l1_footer_cache&) = delete;
    l1_footer_cache& operator=(const l1_footer_cache&) = delete;
    l1_footer_cache(l1_footer_cache&&) = delete;
    l1_footer_cache& operator=(l1_footer_cache&&) = delete;
    ~l1_footer_cache();

    /// Look up a cached footer by object id. Returns std::nullopt on miss.
    /// On hit, the entry is moved to the most-recently-used position and a
    /// shared pointer to the stored footer is returned.
    std::optional<footer_ptr> get(const object_id& oid);

    /// Insert a parsed footer into the cache. If an entry already exists for
    /// `oid`, it is replaced. Wraps `footer_value` in a `lw_shared_ptr` and
    /// returns a copy of that pointer; on a successful insert the cache and
    /// caller share ownership of the same `const footer`. When the cache is
    /// sized to zero the footer is not stored, but the wrapped pointer is
    /// still returned so the caller can use it directly.
    footer_ptr put(object_id oid, footer footer_value);

    stats get_stats() const;

    ss::future<> stop();

private:
    /// Entries are owned by `_index` (keyed by oid) and linked into `_lru` for
    /// recency ordering. The two structures track distinct eviction criteria:
    ///   * `_lru` order (LRU at front, MRU at back) drives size-bounded
    ///     eviction in `maybe_evict_size`.
    ///   * `last_used` drives time-bounded idle eviction in `maybe_evict`.
    /// The `intrusive_list_hook` is in auto-unlink mode, so destroying an
    /// entry via `_index.erase` removes it from `_lru` without an explicit
    /// unlink at the callsite.
    struct entry {
        entry(object_id oid, footer_ptr cached_footer)
          : oid(std::move(oid))
          , cached_footer(std::move(cached_footer)) {}

        object_id oid;
        footer_ptr cached_footer;
        ss::lowres_clock::time_point last_used = ss::lowres_clock::now();
        intrusive_list_hook _hook;
    };

    void arm_eviction_timer();
    void maybe_evict();
    void maybe_evict_size();
    bool over_size_limit() const;

    config::binding<std::chrono::milliseconds> _eviction_timeout;
    config::binding<size_t> _target_max_size;
    level_one_reader_probe* _probe;

    ss::gate _gate;
    ss::timer<ss::lowres_clock> _eviction_timer;

    intrusive_list<entry, &entry::_hook> _lru;
    chunked_hash_map<object_id, std::unique_ptr<entry>> _index;
};

} // namespace cloud_topics::l1
