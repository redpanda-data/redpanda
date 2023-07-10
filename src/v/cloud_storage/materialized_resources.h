/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/materialized_manifest_cache.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/segment_state.h"
#include "config/property.h"
#include "random/simple_time_jitter.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "utils/adjustable_semaphore.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

namespace cloud_storage {

class remote_segment;
class remote_segment_batch_reader;
class remote_probe;
class materialized_manifest_cache;

/**
 * This class tracks:
 * - Instances of materialized_segment that are created by
 *   each individual remote_partition
 * - The segment readers within them, to globally limit concurrent
 *   readers instantiated, as each reader has a memory+fd
 *   impact.
 * - The top level partition readers, which should always be fewer
 *   in number than the segment readers they borrow.
 * - Instances of spillover_manifest used by async_manifest_view.
 *
 * It is important to have shard-global visibility of materialized
 * segment state, in order to apply resources.  As a bonus, this
 * also enables us to have a central fiber for background-stopping
 * evicted objects, instead of each partition doing it independently.
 */
class materialized_resources {
public:
    materialized_resources();

    ss::future<> start();
    ss::future<> stop();

    void register_segment(materialized_segment_state& s);

    ssx::semaphore_units get_segment_reader_units();

    ss::future<ssx::semaphore_units> get_partition_reader_units(size_t);

    ssx::semaphore_units get_segment_units();

    materialized_manifest_cache& get_materialized_manifest_cache();

private:
    /// Timer use to periodically evict stale segment readers
    ss::timer<ss::lowres_clock> _stm_timer;
    simple_time_jitter<ss::lowres_clock> _stm_jitter;

    config::binding<uint32_t> _max_partitions_per_shard;
    config::binding<std::optional<uint32_t>> _max_readers_per_shard;
    config::binding<std::optional<uint32_t>> _max_segments_per_shard;

    size_t max_readers() const;
    size_t max_segments() const;

    /// How many remote_segment_batch_reader instances exist
    size_t current_segment_readers() const;

    /// How many partition_record_batch_reader_impl instances exist
    size_t current_partition_readers() const;

    /// How many materialized_segment_state instances exist
    size_t current_segments() const;

    /// Counts the number of times when get_partition_reader_units() was
    /// called and had to sleep because no units were immediately available.
    uint64_t get_partition_readers_delayed() {
        return _partition_readers_delayed;
    }

    /// Consume from _eviction_list
    ss::future<> run_eviction_loop();

    /// Try to evict segment readers until `target_free` units are available in
    /// _reader_units, i.e. available for new readers to be created.
    void trim_segment_readers(size_t target_free);

    /// Synchronous scan of segments for eviction, reads+modifies _materialized
    /// and writes victims to _eviction_list
    void trim_segments(std::optional<size_t>);

    // List of segments to offload, accumulated during trim_segments
    using offload_list_t
      = std::vector<std::pair<remote_partition*, model::offset>>;

    void maybe_trim_segment(materialized_segment_state&, offload_list_t&);

    // Permit probe to query object counts
    friend class remote_probe;

    // We need to quickly look up readers by segment, to find any readers
    // for a segment that is targeted by a read.  Within those readers,
    // we may do a linear scan to find if any of those readers matches
    // the offset that the reader is looking for.
    intrusive_list<
      materialized_segment_state,
      &materialized_segment_state::_hook>
      _materialized;

    /// Gate for background eviction
    ss::gate _gate;

    /// Size limit on the cache of remote_segment_batch_reader instances,
    /// per shard.  These are limited because they consume a lot of memory
    /// with their read buffer.
    adjustable_semaphore _segment_reader_units;

    /// Concurrency limit on how many partition_record_batch_reader_impl may be
    /// instantiated at once on one shard.  This is a de-facto limit on how many
    /// concurrent reads may be done.  We need this in addition to
    /// segment_reader_units, because that is a soft limit (guides trimming),
    /// whereas this is a hard limit (readers will not be created unless units
    /// are available), and can be enforced very early in the lifetime of a
    /// kafka fetch/timequery request.
    adjustable_semaphore _partition_reader_units;

    /// Concurrency limit on how many segments may be materialized at
    /// once: this will trigger faster trimming under pressure.
    adjustable_semaphore _segment_units;

    /// Size of the materialized_manifest_cache
    config::binding<size_t> _manifest_meta_size;

    /// Cache used to store materialized spillover manifests
    ss::shared_ptr<materialized_manifest_cache> _manifest_cache;

    /// Counter that is exposed via probe object.
    uint64_t _partition_readers_delayed{0};
};

} // namespace cloud_storage
