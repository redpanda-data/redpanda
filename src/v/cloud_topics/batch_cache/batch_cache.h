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
#include "cloud_topics/batch_cache/hydrated_cache_api.h"
#include "cloud_topics/batch_cache/hydrated_object_index.h"
#include "cloud_topics/batch_cache/probe.h"
#include "model/fundamental.h"
#include "storage/api.h"
#include "storage/batch_cache.h"

#include <chrono>
#include <memory>

namespace storage {
class log_manager;
}

namespace cloud_topics {

struct batch_cache_accessor;
class batch_cache;

constexpr auto default_batch_cache_check_interval = std::chrono::seconds(20);

/// Per-partition hydrated cache state.
/// Each partition maintains a translation layer that manages per-epoch
/// batch_cache_index instances internally.
struct partition_hydrated_state {
    /// Translation layer: maps (object_id, byte_offset) to synthetic offsets
    /// and manages per-epoch batch_cache_index instances.
    std::unique_ptr<partition_hydrated_index> translation_map;
};

/// Implementation of partition_hydrated_cache_api that delegates to
/// batch_cache. This provides per-partition hydrated data caching with epoch
/// ordering.
class partition_hydrated_cache final : public partition_hydrated_cache_api {
public:
    explicit partition_hydrated_cache(batch_cache& cache);

    bool is_cached(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      byte_range_size_t size) const override;

    std::optional<iobuf> get(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      byte_range_size_t size) override;

    void put(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      iobuf payload) override;

    void truncate_hydrated(
      const model::topic_id_partition& tidp,
      cluster_epoch invalidated_epoch) override;

private:
    batch_cache& _cache;
};

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

    // Put element into the batch cache. The element shouldn't be dirty.
    // The code that uses this class should only use this to cache committed
    // entries.
    void
    put(const model::topic_id_partition& tidp, const model::record_batch& b);

    // Fetch element from cache.
    std::optional<model::record_batch>
    get(const model::topic_id_partition& tidp, model::offset o);

    /// Get the per-partition hydrated cache.
    /// The returned object implements partition_hydrated_cache_api and provides
    /// access to hydrated L0 data with per-partition indexing and epoch
    /// ordering.
    partition_hydrated_cache_api* get_partition_hydrated_cache();

    /// Get probe statistics for testing/monitoring.
    const batch_cache_probe& probe() const { return _probe; }

private:
    friend class partition_hydrated_cache;
    // Remove dead index entries
    ss::future<> cleanup_index_entries();

    // Per-partition hydrated cache operations (called by
    // partition_hydrated_cache)
    bool is_cached_hydrated_internal(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      byte_range_size_t size) const;

    std::optional<iobuf> get_hydrated_internal(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      byte_range_size_t size);

    void put_hydrated_internal(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      iobuf payload);

    void truncate_hydrated_internal(
      const model::topic_id_partition& tidp, cluster_epoch invalidated_epoch);

    /// Ensure partition-level hydrated state is initialized.
    partition_hydrated_state&
    ensure_partition_hydrated_state(const model::topic_id_partition& tidp);

    std::chrono::milliseconds _gc_interval;
    // NOTE: in the storage layer we have multiple indexes per partition (one
    // per segment). Here we only have one index per cloud storage partition.
    // From what I see it should be OK to use index this way. Likely even more
    // efficient compared to index per segment.
    // The index is keyed by topic_id_partition to prevent batch resurrection
    // when topics are deleted and re-created with the same name.
    absl::btree_map<model::topic_id_partition, storage::batch_cache_index_ptr>
      _index;

    // Per-partition hydrated cache: stores raw L0 object payloads
    // (without materialized offsets). Each partition has its own
    // batch_cache_index and translation layer.
    absl::btree_map<model::topic_id_partition, partition_hydrated_state>
      _partition_hydrated;

    // Per-partition hydrated cache API instance
    std::unique_ptr<partition_hydrated_cache> _partition_hydrated_cache;

    storage::log_manager* _lm;
    // Periodic cleanup of the materialized index
    ss::timer<> _cleanup_timer;
    ss::gate _gate;
    batch_cache_probe _probe;

    friend struct batch_cache_accessor;
};

} // namespace cloud_topics
