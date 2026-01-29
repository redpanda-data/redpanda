/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/hydrated_object_index.h"

namespace cloud_topics {

epoch_cache_index*
partition_hydrated_index::get_or_create_epoch(cluster_epoch epoch) {
    auto it = _epochs.find(epoch);
    if (it != _epochs.end()) {
        return &it->second;
    }

    // Create a new batch_cache_index using the factory
    auto index = _index_factory();
    if (!index) {
        // Caching is disabled
        return nullptr;
    }

    auto [new_it, _] = _epochs.emplace(
      epoch, epoch_cache_index{std::move(index)});
    return &new_it->second;
}

std::optional<partition_hydrated_index::find_extent_result>
partition_hydrated_index::find_containing_extent(
  const object_id& id,
  first_byte_offset_t byte_offset,
  size_t size_bytes) const {
    cluster_epoch epoch = id.epoch;

    auto epoch_it = _epochs.find(epoch);
    if (epoch_it == _epochs.end()) {
        return std::nullopt;
    }

    const auto& epoch_index = epoch_it->second;

    // Search for an extent that contains the requested range
    // [byte_offset, byte_offset + size_bytes)
    for (const auto& [key, meta] : epoch_index.extent_map) {
        if (key.id != id) {
            continue;
        }

        // Check if the cached extent [key.byte_offset, key.byte_offset +
        // meta.size_bytes) contains the requested range
        auto cached_start = key.byte_offset();
        auto cached_end = cached_start + meta.size_bytes;
        auto query_start = byte_offset();
        auto query_end = query_start + size_bytes;

        if (query_start >= cached_start && query_end <= cached_end) {
            return find_extent_result{
              .key = key,
              .meta = meta,
              .offset_in_extent = query_start - cached_start,
            };
        }
    }

    return std::nullopt;
}

std::optional<model::offset> partition_hydrated_index::put_extent(
  const object_id& id, first_byte_offset_t byte_offset, size_t size_bytes) {
    l0_object_extent_key key{.id = id, .byte_offset = byte_offset};
    cluster_epoch epoch = id.epoch;

    // Get or create the epoch-specific cache index
    auto* epoch_index = get_or_create_epoch(epoch);
    if (epoch_index == nullptr) {
        return std::nullopt;
    }

    // Check if already cached in this epoch
    if (auto it = epoch_index->extent_map.find(key);
        it != epoch_index->extent_map.end()) {
        return it->second.synthetic_offset;
    }

    // Allocate new synthetic offset for this epoch
    model::offset synthetic = epoch_index->counter;
    epoch_index->counter = model::next_offset(epoch_index->counter);

    // Store extent mapping with size
    cached_extent_meta meta{
      .size_bytes = size_bytes,
      .synthetic_offset = synthetic,
    };
    epoch_index->extent_map.emplace(key, meta);

    return synthetic;
}

bool partition_hydrated_index::has_extent(
  const object_id& id,
  first_byte_offset_t byte_offset,
  size_t size_bytes) const {
    return find_containing_extent(id, byte_offset, size_bytes).has_value();
}

std::optional<iobuf> partition_hydrated_index::get_extent(
  const object_id& id,
  first_byte_offset_t byte_offset,
  size_t size_bytes) const {
    // Find the cached extent containing the requested range
    auto result = find_containing_extent(id, byte_offset, size_bytes);
    if (!result.has_value()) {
        return std::nullopt;
    }

    // Get the batch_cache_index for this epoch
    cluster_epoch epoch = id.epoch;
    auto* index = get_batch_cache_index(epoch);
    if (index == nullptr) {
        return std::nullopt;
    }

    // Look up the full extent data in the cache
    auto batch = index->get(result->meta.synthetic_offset);
    if (!batch.has_value()) {
        // The batch was evicted from the underlying cache
        return std::nullopt;
    }

    // Extract the payload from the fake record batch
    auto full_data = std::move(*batch).release_data();

    // Return the requested slice if it's a subset
    if (
      result->offset_in_extent == 0 && size_bytes == result->meta.size_bytes) {
        // Exact match - return the full data
        return full_data;
    }

    // Return the requested slice
    return full_data.share(result->offset_in_extent, size_bytes);
}

storage::batch_cache_index*
partition_hydrated_index::get_batch_cache_index(cluster_epoch epoch) const {
    auto it = _epochs.find(epoch);
    if (it == _epochs.end()) {
        return nullptr;
    }
    return it->second.index.get();
}

void partition_hydrated_index::truncate_epoch(cluster_epoch epoch) {
    // Remove all epochs strictly less than the given epoch
    auto it = _epochs.begin();
    while (it != _epochs.end() && it->first < epoch) {
        // Truncate the batch_cache_index to evict all cached data
        // Using offset 0 truncates everything
        it->second.index->truncate(model::offset{0});
        it = _epochs.erase(it);
    }
}

size_t partition_hydrated_index::extent_count() const {
    size_t count = 0;
    for (const auto& [_, epoch_index] : _epochs) {
        count += epoch_index.extent_map.size();
    }
    return count;
}

size_t partition_hydrated_index::memory_usage() const {
    size_t usage = 0;

    for (const auto& [_, epoch_index] : _epochs) {
        // extent_map: key + value size * count
        usage += epoch_index.extent_map.size()
                 * (sizeof(l0_object_extent_key) + sizeof(cached_extent_meta));
        // batch_cache_index has its own memory tracking
    }

    // Overhead for the epochs map itself
    usage += _epochs.size()
             * (sizeof(cluster_epoch) + sizeof(epoch_cache_index));

    return usage;
}

} // namespace cloud_topics
