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

#include "cloud_storage/remote_partition.h"
#include "cloud_storage/segment_state.h"
#include "config/property.h"
#include "random/simple_time_jitter.h"
#include "resource_mgmt/available_memory.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "utils/adjustable_semaphore.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

namespace cloud_storage {

class remote_segment;
class remote_segment_batch_reader;
class remote_probe;

/**
 * This class tracks:
 * - Instances of materialized_segment that are created by
 *   each individual remote_partition
 * - The readers within them, to globally limit concurrent
 *   readers instantiated, as each reader has a memory+fd
 *   impact.
 *
 * It is important to have shard-global visibility of materialized
 * segment state, in order to apply resources.  As a bonus, this
 * also enables us to have a central fiber for background-stopping
 * evicted objects, instead of each partition doing it independently.
 */
class materialized_segments {
public:
    explicit materialized_segments(ss::scheduling_group);

    ss::future<> start();
    ss::future<> stop();

    void register_segment(materialized_segment_state& s);

    /// Put reader into the eviction list which will
    /// eventually lead to it being closed and deallocated
    void evict_reader(std::unique_ptr<remote_segment_batch_reader> reader);
    void evict_segment(ss::lw_shared_ptr<remote_segment> segment);

    ssx::semaphore_units get_reader_units();

    ssx::semaphore_units get_segment_units();

    /// Wait until any evicted items in the _eviction_list have been removed.
    ss::future<> flush_evicted();

    struct reclaimable_memory_t {
        size_t readers_memory_use_estimate;
        size_t num_readers;
        size_t segments_memory_use_estimate;
        size_t num_segments;
    };
    /// Calculate amount of memory that can be freed by evicting this segment
    reclaimable_memory_t reclaimable_memory() const;

private:
    /// Timer use to periodically evict stale readers
    ss::timer<ss::lowres_clock> _stm_timer;
    simple_time_jitter<ss::lowres_clock> _stm_jitter;

    config::binding<uint32_t> _max_partitions_per_shard;
    config::binding<std::optional<uint32_t>> _max_readers_per_shard;
    config::binding<std::optional<uint32_t>> _max_segments_per_shard;

    size_t max_readers() const;
    size_t max_segments() const;

    /// How many remote_segment_batch_reader instances exist
    size_t current_readers() const;

    /// How many materialized_segment_state instances exist
    size_t current_segments() const;

    ss::memory::reclaiming_result reclaim(ss::memory::reclaimer::request req);

    /// Special item in eviction_list that holds a promise and sets it
    /// when the eviction fiber calls stop() (see flush_evicted)
    struct eviction_barrier {
        ss::promise<> promise;

        ss::future<> stop() {
            promise.set_value();
            stopped = true;
            return ss::now();
        }

        bool stopped{false};

        bool is_stopped() const { return stopped; }
    };

    using evicted_resource_t = std::variant<
      std::unique_ptr<remote_segment_batch_reader>,
      ss::lw_shared_ptr<remote_segment>,
      ss::lw_shared_ptr<eviction_barrier>>;
    using eviction_list_t = std::deque<evicted_resource_t>;

    /// List of segments and readers waiting to have their stop() method
    /// called before destruction
    eviction_list_t _eviction_pending;
    eviction_list_t _eviction_in_flight;

    // We need to quickly look up readers by segment, to find any readers
    // for a segment that is targeted by a read.  Within those readers,
    // we may do a linear scan to find if any of those readers matches
    // the offset that the reader is looking for.
    intrusive_list<
      materialized_segment_state,
      &materialized_segment_state::_hook>
      _materialized;

    /// Kick this condition variable when appending to eviction_list
    ss::condition_variable _cvar;

    /// Gate for background eviction
    ss::gate _gate;

    /// Concurrency limit on how many remote_segment_batch_reader may be
    /// instantiated at once on one shard.
    adjustable_semaphore _reader_units;

    /// Concurrency limit on how many segments may be materialized at
    /// once: this will trigger faster trimming under pressure.
    adjustable_semaphore _segment_units;

    /// Consume from _eviction_list
    ss::future<> run_eviction_loop();

    /// Try to evict readers until `target_free` units are available in
    /// _reader_units, i.e. available for new readers to be created.
    void trim_readers(size_t target_free);

    /// Synchronous scan of segments for eviction, reads+modifies _materialized
    /// and writes victims to _eviction_list
    void trim_segments(std::optional<size_t>);

    // List of segments to offload, accumulated during trim_segments
    using offload_list_t
      = std::vector<std::pair<remote_partition*, model::offset>>;

    void maybe_trim_segment(materialized_segment_state&, offload_list_t&);

    // Permit probe to query object counts
    friend class remote_probe;

    ss::memory::reclaimer _reclaimer;
    ss::scheduling_group _eviction_sg;
};

} // namespace cloud_storage