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

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "storage/batch_cache.h"

#include <cstddef>
#include <functional>
#include <optional>

namespace cloud_topics {

/// Factory function type for creating batch_cache_index instances.
/// Returns a unique_ptr to a new batch_cache_index, or nullptr if caching
/// is disabled.
using batch_cache_index_factory
  = std::function<storage::batch_cache_index_ptr()>;

/// Key identifying a specific data range within an L0 object.
/// Used to look up cached hydrated data.
struct l0_object_extent_key {
    object_id id;
    first_byte_offset_t byte_offset;

    bool operator==(const l0_object_extent_key&) const = default;
    auto operator<=>(const l0_object_extent_key&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const l0_object_extent_key& k) {
        return H::combine(std::move(h), k.id, k.byte_offset());
    }
};

/// Metadata for a cached extent, storing its size and synthetic offset
struct cached_extent_meta {
    /// Size of the cached extent in bytes
    size_t size_bytes{0};
    /// Synthetic offset used to store/retrieve data in batch_cache_index
    model::offset synthetic_offset;
};

/// Result of looking up an extent, containing the data and the offset
/// within the cached extent where the requested data starts
struct extent_lookup_result {
    /// The cached data (may be the full extent or a slice)
    iobuf data;
    /// Offset within the cached extent where requested data starts
    /// (0 if the query matches the cached extent's start)
    size_t offset_in_extent{0};
};

/// Per-epoch cache index containing:
/// - A batch_cache_index for actual data storage
/// - A map from extent keys to extent metadata (size + synthetic offset)
/// - A counter for generating unique synthetic offsets within this epoch
struct epoch_cache_index {
    /// The batch_cache_index holding actual cached data for this epoch
    storage::batch_cache_index_ptr index;

    /// Map from (object_id, byte_offset) to extent metadata within this epoch
    /// The key's byte_offset is the START of the cached extent
    absl::flat_hash_map<l0_object_extent_key, cached_extent_meta> extent_map;

    /// Counter for generating unique monotonically increasing offsets
    /// within this epoch
    model::offset counter{0};

    explicit epoch_cache_index(storage::batch_cache_index_ptr idx)
      : index(std::move(idx)) {}

    epoch_cache_index(const epoch_cache_index&) = delete;
    epoch_cache_index& operator=(const epoch_cache_index&) = delete;
    epoch_cache_index(epoch_cache_index&&) = default;
    epoch_cache_index& operator=(epoch_cache_index&&) = default;
    ~epoch_cache_index() = default;
};

/// Per-partition translation layer for hydrated L0 object data.
///
/// Maps (object_id, byte_offset) to synthetic cache offsets that can be used
/// with batch_cache_index, organized by epoch.
///
/// Design notes:
/// - Each epoch has its own batch_cache_index and offset counter
/// - No epoch ordering enforcement - readers can add data in any order
/// - No entry limits - memory pressure is handled by batch_cache eviction
/// - Epoch-based truncation removes entire epoch indices and their data
class partition_hydrated_index {
public:
    /// Construct the index with a factory for creating batch_cache_index
    /// instances.
    explicit partition_hydrated_index(batch_cache_index_factory factory)
      : _index_factory(std::move(factory)) {}

    partition_hydrated_index(const partition_hydrated_index&) = delete;
    partition_hydrated_index& operator=(const partition_hydrated_index&)
      = delete;
    partition_hydrated_index(partition_hydrated_index&&) = default;
    partition_hydrated_index& operator=(partition_hydrated_index&&) = default;
    ~partition_hydrated_index() = default;

    /// Allocate a synthetic offset for an extent and store the mapping.
    ///
    /// The epoch is extracted from the object_id. Each epoch has its own
    /// batch_cache_index and offset counter, so offsets are unique per epoch.
    ///
    /// Returns the synthetic offset for the extent, or nullopt if the cache
    /// index could not be created. If the extent is already cached, returns
    /// the existing synthetic offset.
    std::optional<model::offset> put_extent(
      const object_id& id, first_byte_offset_t byte_offset, size_t size_bytes);

    /// Check if a byte range is cached (supports subset queries).
    ///
    /// Returns true if there is a cached extent that contains the requested
    /// range [byte_offset, byte_offset + size_bytes).
    ///
    /// Example:
    ///   put_extent(id, byte_offset=0, size_bytes=1024)
    ///   has_extent(id, byte_offset=100, size_bytes=200) -> true
    ///   has_extent(id, byte_offset=0, size_bytes=1024)   -> true
    ///   has_extent(id, byte_offset=500, size_bytes=600)  -> false (exceeds)
    bool has_extent(
      const object_id& id,
      first_byte_offset_t byte_offset,
      size_t size_bytes) const;

    /// Get cached extent data (supports subset queries).
    ///
    /// Finds a cached extent that contains the requested range
    /// [byte_offset, byte_offset + size_bytes) and returns the data slice.
    ///
    /// Returns nullopt if:
    /// - No cached extent contains the requested range
    /// - The data was evicted from the underlying batch_cache
    ///
    /// Example:
    ///   put_extent(id, byte_offset=0, size_bytes=1024)
    ///   // (also store 1024 bytes of data)
    ///   get_extent(id, byte_offset=100, size_bytes=200)
    ///     -> returns bytes [100, 300) from the cached data
    std::optional<iobuf> get_extent(
      const object_id& id,
      first_byte_offset_t byte_offset,
      size_t size_bytes) const;

    /// Get the batch_cache_index for a specific epoch.
    /// Returns nullptr if the epoch doesn't exist.
    storage::batch_cache_index*
    get_batch_cache_index(cluster_epoch epoch) const;

    /// Truncate all entries from epochs strictly less than the given epoch.
    /// This removes all epoch_cache_index entries for older epochs and
    /// truncates their batch_cache_index to evict the cached data.
    void truncate_epoch(cluster_epoch epoch);

    /// Total number of extents currently tracked across all epochs.
    size_t extent_count() const;

    /// Number of epochs currently tracked.
    size_t epoch_count() const { return _epochs.size(); }

    /// Approximate memory usage of the translation layer metadata.
    size_t memory_usage() const;

private:
    /// Get or create epoch_cache_index for an epoch.
    /// Returns nullptr if the cache index could not be created.
    epoch_cache_index* get_or_create_epoch(cluster_epoch epoch);

    /// Result of finding a cached extent containing a query range
    struct find_extent_result {
        /// The cached extent's key
        l0_object_extent_key key;
        /// The cached extent's metadata
        cached_extent_meta meta;
        /// Offset within the cached extent where requested range starts
        size_t offset_in_extent{0};
    };

    /// Find a cached extent that contains the requested range.
    /// Returns nullopt if no such extent exists.
    std::optional<find_extent_result> find_containing_extent(
      const object_id& id,
      first_byte_offset_t byte_offset,
      size_t size_bytes) const;

    /// Factory for creating batch_cache_index instances
    batch_cache_index_factory _index_factory;

    /// Ordered map of epochs to their cache indices
    absl::btree_map<cluster_epoch, epoch_cache_index> _epochs;
};

} // namespace cloud_topics
