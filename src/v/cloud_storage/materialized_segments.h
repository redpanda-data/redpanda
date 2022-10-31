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

#include "cloud_storage/segment_state.h"
#include "random/simple_time_jitter.h"
#include "seastarx.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

namespace cloud_storage {

class remote_segment;
class remote_segment_batch_reader;

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
    materialized_segments();

    ss::future<> start();
    ss::future<> stop();

    using evicted_resource_t = std::variant<
      std::unique_ptr<remote_segment_batch_reader>,
      ss::lw_shared_ptr<remote_segment>>;

    using eviction_list_t = std::deque<evicted_resource_t>;

    void register_segment(materialized_segment_state& s);

    /// Put reader into the eviction list which will
    /// eventually lead to it being closed and deallocated
    void evict_reader(std::unique_ptr<remote_segment_batch_reader> reader);
    void evict_segment(ss::lw_shared_ptr<remote_segment> segment);

    /// Before materializing a segment, consider trimmign to release resources
    void maybe_trim();

private:
    /// Timer use to periodically evict stale readers
    ss::timer<ss::lowres_clock> _stm_timer;
    simple_time_jitter<ss::lowres_clock> _stm_jitter;

    /// List of segments and readers waiting to have their stop() method
    /// called before destruction
    eviction_list_t _eviction_list;

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

    /// Consume from _eviction_list
    ss::future<> run_eviction_loop();

    /// Synchronous scan of segments for eviction, reads+modifies _materialized
    /// and writes victims to _eviction_list
    void gc_stale_materialized_segments(bool force_collection);
};

} // namespace cloud_storage