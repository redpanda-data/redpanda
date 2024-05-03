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

#include "base/seastarx.h"
#include "cloud_storage/materialized_manifest_cache.h"
#include "cloud_storage/read_path_probes.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/segment_state.h"
#include "config/property.h"
#include "container/intrusive_list_helpers.h"
#include "random/simple_time_jitter.h"
#include "ssx/semaphore.h"
#include "utils/adjustable_semaphore.h"
#include "utils/token_bucket.h"

#include <seastar/core/abort_source.hh>
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
    friend class throttled_dl_source;

public:
    materialized_resources();

    ss::future<> start();
    ss::future<> stop();

    void register_segment(materialized_segment_state& s);

    ss::future<segment_reader_units>
    get_segment_reader_units(storage::opt_abort_source_t as);

    ss::future<ssx::semaphore_units>
    get_partition_reader_units(storage::opt_abort_source_t as);

    ss::future<segment_units> get_segment_units(storage::opt_abort_source_t as);

    materialized_manifest_cache& get_materialized_manifest_cache();

    ts_read_path_probe& get_read_path_probe();

    /// Acquire hydration units
    ///
    /// The undrlying semaphore limits number of parallel hydrations
    ss::future<ssx::semaphore_units> get_hydration_units(size_t n);
    ss::input_stream<char>
    throttle_download(ss::input_stream<char> underlying, ss::abort_source& as);

private:
    /// Timer use to periodically evict stale segment readers
    ss::timer<ss::lowres_clock> _stm_timer;
    simple_time_jitter<ss::lowres_clock> _stm_jitter;

    config::binding<std::optional<uint32_t>> _max_segment_readers_per_shard;
    config::binding<std::optional<uint32_t>>
      _max_concurrent_hydrations_per_shard;
    config::binding<size_t> _storage_read_buffer_size;
    config::binding<int16_t> _storage_read_readahead_count;

    size_t max_memory_utilization() const;
    size_t max_parallel_hydrations() const;

    /// How many remote_segment_batch_reader instances exist
    size_t current_segment_readers() const;

    /// How many partition_record_batch_reader_impl instances exist
    size_t current_ongoing_hydrations() const;

    /// How many materialized_segment_state instances exist
    size_t current_segments() const;

    /// Counts the number of times when get_partition_reader_units() was
    /// called and had to sleep because no units were immediately available.
    uint64_t get_partition_readers_delayed() {
        return _partition_readers_delayed;
    }

    /// Counts the number of times when get_segment_reader_units() was
    /// called and had to sleep because no units were immediately available.
    uint64_t get_segment_readers_delayed() { return _segment_readers_delayed; }

    /// Counts the number of times when get_segment_units() was
    /// called and had to sleep because no units were immediately available.
    uint64_t get_segments_delayed() { return _segments_delayed; }

    /// Consume from _eviction_list
    ss::future<> run_eviction_loop();

    /// Set bandwidth for tiered-storage scheduling_group
    ss::future<> set_disk_max_bandwidth(size_t tput);

    /// Set download bandwidth for cloud storage API
    void set_net_max_bandwidth(size_t tput);

    /// Recalculate and reset throughput limits
    ss::future<> update_throughput();

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

    adjustable_semaphore _mem_units;
    adjustable_semaphore _hydration_units;

    /// Size of the materialized_manifest_cache
    config::binding<size_t> _manifest_meta_size;

    /// Cache used to store materialized spillover manifests
    ss::shared_ptr<materialized_manifest_cache> _manifest_cache;

    /// Counter that is exposed via probe object.
    uint64_t _partition_readers_delayed{0};
    uint64_t _segment_readers_delayed{0};
    uint64_t _segments_delayed{0};

    ts_read_path_probe _read_path_probe;
    token_bucket<> _throughput_limit;
    config::binding<std::optional<size_t>> _throughput_shard_limit_config;
    config::binding<std::optional<size_t>> _relative_throughput;
    bool _throttling_disabled{false};
    std::optional<size_t> _device_throughput;
    config::binding<uint32_t> _cache_carryover_bytes;
    // Memory reserved for cache carryover mechanism
    std::optional<ssx::semaphore_units> _carryover_units;
};

} // namespace cloud_storage
