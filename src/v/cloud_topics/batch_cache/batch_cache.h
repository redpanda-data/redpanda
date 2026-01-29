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
#include "storage/api.h"
#include "storage/batch_cache.h"

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

    // Put element into the batch cache. The element shouldn't be dirty.
    // The code that uses this class should only use this to cache committed
    // entries.
    void
    put(const model::topic_id_partition& tidp, const model::record_batch& b);

    // Fetch element from cache.
    std::optional<model::record_batch>
    get(const model::topic_id_partition& tidp, model::offset o);

private:
    // Remove dead index entries
    ss::future<> cleanup_index_entries();

    std::chrono::milliseconds _gc_interval;
    // NOTE: in the storage layer we have multiple indexes per partition (one
    // per segment). Here we only have one index per cloud storage partition.
    // From what I see it should be OK to use index this way. Likely even more
    // efficient compared to index per segment.
    // The index is keyed by topic_id_partition to prevent batch resurrection
    // when topics are deleted and re-created with the same name.
    absl::btree_map<model::topic_id_partition, storage::batch_cache_index_ptr>
      _index;
    storage::log_manager* _lm;
    // Periodic cleanup of the index
    ss::timer<> _cleanup_timer;
    ss::gate _gate;
    batch_cache_probe _probe;

    friend struct batch_cache_accessor;
};

} // namespace cloud_topics
