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

#include "bytes/iobuf.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"

#include <optional>

namespace cloud_topics {

/// Interface for per-partition hydrated L0 object caching.
///
/// Used by level_zero/reader components to cache hydrated (non-materialized)
/// data. This interface avoids cyclic dependencies between the reader
/// components and data_plane_api. Similar pattern to basic_cache_service_api.
///
/// Hydrated data is raw L0 object payload that does NOT have proper kafka
/// offsets or term_ids assigned yet - these are stored without assigned offsets
/// in the underlying partition.
///
/// Design notes:
/// - Per-partition caching: each partition has its own index and cache
/// - Epoch ordering: synthetic offsets respect epoch ordering
/// - May silently reject puts that violate epoch ordering
class partition_hydrated_cache_api {
public:
    virtual ~partition_hydrated_cache_api() = default;

    /// Check if hydrated data for this extent range is cached for the
    /// partition. Supports subset queries - returns true if any cached extent
    /// contains the requested range [byte_offset, byte_offset + size).
    virtual bool is_cached(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      byte_range_size_t size) const
      = 0;

    /// Get hydrated data for an extent.
    /// Returns nullopt if not cached or if data was evicted.
    virtual std::optional<iobuf> get(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      byte_range_size_t size)
      = 0;

    /// Store hydrated data for an extent.
    /// May silently reject if epoch ordering is violated (epoch < max_epoch
    /// for this partition).
    /// NOTE: No kafka offsets - hydrated data is raw bytes without inherent
    /// offsets. Eviction is handled by the underlying batch_cache_index.
    virtual void put(
      const model::topic_id_partition& tidp,
      const object_id& id,
      first_byte_offset_t byte_offset,
      iobuf payload)
      = 0;

    /// Truncate hydrated cache entries for a partition when epoch is
    /// invalidated. Removes all entries from epochs older than the specified
    /// epoch.
    virtual void truncate_hydrated(
      const model::topic_id_partition& tidp, cluster_epoch invalidated_epoch)
      = 0;
};

} // namespace cloud_topics
