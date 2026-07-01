/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/btree_map.h"
#include "cloud_topics/batch_cache/probe.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "storage/api.h"
#include "storage/batch_cache.h"
#include "utils/offset_monitor.h"

#include <seastar/core/abort_source.hh>

#include <chrono>

namespace storage {
class log_manager;
}

namespace cloud_topics {

struct batch_cache_accessor;

constexpr auto default_batch_cache_check_interval = std::chrono::seconds(20);

/// Batch cache used collectively by all cloud topic
/// partitions on a shard.
/// The object maintains a batch_cache_index per cloud topic partition.
/// Partitions can be evicted from cache after some inactivity period
/// or when the index is empty.
/// This component uses record batch cache. The underlying partition also uses
/// the record batch cache but independently (through the normal code path in
/// storage layer). This component stores materialized batches but the
/// batch_cache_index in the segment of the underlying partition stores
/// placeholders.
class batch_cache {
public:
    // The 'log_manager' could be 'nullptr' if caching is disabled
    explicit batch_cache(
      storage::log_manager* log_manager,
      std::chrono::milliseconds gc_interval
      = default_batch_cache_check_interval);

    explicit batch_cache(
      ss::sharded<storage::api>& log_manager,
      std::chrono::milliseconds gc_interval
      = default_batch_cache_check_interval);

    ss::future<> start();
    ss::future<> stop();

    /// True when the shared batch cache is available on this shard. False when
    /// caching was globally disabled (constructed without a log_manager, or
    /// `disable_batch_cache=true`): in that case put()/get()/pin() are all
    /// no-ops and any read path that relies on the cache (e.g. L1 prefetch)
    /// cannot return data. Per-topic `cache_enabled=false` is NOT reflected
    /// here; that is enforced upstream where the partition config is known.
    bool caching_enabled() const;

    // Put element into the batch cache.
    void
    put(const model::topic_id_partition& tidp, const model::record_batch& b);

    // Fetch element from cache.
    std::optional<model::record_batch>
    get(const model::topic_id_partition& tidp, model::offset o);

    /// True when a valid (non-evicted) entry covering \p o is cached for
    /// \p tidp. Unlike get(), this is a residency probe only: it copies no
    /// batch, does not promote the LRU entry, and does not touch the
    /// hit/miss metrics. Used by the L1 prefetch service to decide whether a
    /// warm stream can be reused or the offset must be re-produced.
    bool contains(const model::topic_id_partition& tidp, model::offset o);

    /// Pin the batch containing \p o for \p tidp so the shared cache's LRU
    /// reclaimer will not drop it under memory pressure. The L1 prefetch
    /// service pins un-consumed prefetched batches: the producer advances past
    /// them and never re-produces, so an eviction would hang the reader.
    ///
    /// Pins are reference counted at the underlying range level. Returns true
    /// when a live entry was found and pinned; false when the offset is not
    /// cached (e.g. caching disabled or the entry was already evicted before
    /// the pin). The caller should pin immediately after put(), within the same
    /// synchronous span, so no eviction can intervene.
    bool pin(const model::topic_id_partition& tidp, model::offset o);

    /// Release a pin taken by pin() for (\p tidp, \p o). Returns true when a
    /// live entry was found and unpinned. Must balance a prior pin() that
    /// returned true; a no-op when the offset is no longer cached.
    bool unpin(const model::topic_id_partition& tidp, model::offset o);

    /// Wait until a batch at or beyond \p offset has been added to the cache
    /// for \p tidp, or until timeout/abort. \p last_known seeds a newly
    /// created monitor so that offsets already committed resolve immediately.
    ss::future<> wait_for_offset(
      const model::topic_id_partition& tidp,
      model::offset offset,
      model::offset last_known,
      model::timeout_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as);

    /// Put batches into the cache and notify the offset monitor when the
    /// inserted batches extend the contiguous range tracked by the monitor.
    void put_ordered(
      const model::topic_id_partition& tidp,
      chunked_vector<model::record_batch> batches);

    /// Create a fresh, standalone batch_cache_index backed by this shard's
    /// shared storage batch cache. The L1 prefetch service gives each
    /// fetch_stream its own index so a stream always serves the L1 objects it
    /// downloaded (current post-compaction state) instead of stale batches a
    /// prior reader or the write path left in the shared per-partition index.
    /// Returns nullptr when caching is disabled. The index frees its ranges on
    /// destruction, so a stream's cached data is reclaimed when the stream
    /// dies.
    std::unique_ptr<storage::batch_cache_index> create_index();

    /// Wake readers blocked in wait_for_offset for \p tidp up to \p
    /// last_offset. A fetch_stream calls this after producing into its own
    /// index so a reader parked on the shared per-partition monitor re-checks
    /// and finds the data.
    void notify_produced(
      const model::topic_id_partition& tidp, model::offset last_offset);

private:
    /// Signal that batches up to \p last_offset have been inserted for \p tidp.
    /// Wakes readers blocked in wait_for_offset.
    void
    notify(const model::topic_id_partition& tidp, model::offset last_offset);
    // Remove dead index entries
    ss::future<> cleanup_index_entries();

    std::chrono::milliseconds _gc_interval;
    struct partition_cache_entry {
        storage::batch_cache_index_ptr index;
        // Heap-allocated to avoid alignment issues when nesting btree
        // containers.
        std::unique_ptr<offset_monitor<model::offset>> monitor;
    };

    // NOTE: in the storage layer we have multiple indexes per partition (one
    // per segment). Here we only have one index per cloud storage partition.
    // From what I see it should be OK to use index this way. Likely even more
    // efficient compared to index per segment.
    // The map is keyed by topic_id_partition to prevent batch resurrection
    // when topics are deleted and re-created with the same name.
    absl::btree_map<model::topic_id_partition, partition_cache_entry> _entries;
    storage::log_manager* _lm;
    // Periodic cleanup of the index
    ss::timer<> _cleanup_timer;
    ss::gate _gate;
    batch_cache_probe _probe;

    friend struct batch_cache_accessor;
};

} // namespace cloud_topics
