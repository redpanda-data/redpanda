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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/compaction/filter.h"
#include "cloud_topics/level_one/compaction/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"

namespace cloud_topics::l1 {

class compaction_sink;

class compaction_source : public compaction::sliding_window_reducer::source {
public:
    compaction_source(
      model::ntp,
      model::topic_id_partition,
      const chunked_vector<offset_interval_set::interval>&,
      const offset_interval_set&,
      metastore::extent_offsets_t,
      compaction::key_offset_map*,
      metastore*,
      io*,
      ss::abort_source&,
      compaction_job_state&,
      compaction_worker_probe&);
    ss::future<> initialize() final;
    ss::future<ss::stop_iteration> map_building_iteration() final;
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

private:
    // Returns true if the compaction process has been pre-empted to stop.
    bool preempted() const;

private:
    friend compaction_sink;

    const model::ntp _ntp;
    const model::topic_id_partition _tp;

    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec& _dirty_range_intervals;
    const offset_interval_set& _removable_tombstone_ranges;

    // Iterator used during `map_building_iteration()` which points into the
    // above vector `_dirty_range_intervals`.
    interval_vec::const_iterator _dirty_range_it;

    metastore::extent_offsets_t _extents;
    metastore::extent_offsets_t::const_iterator _extents_it;
    metastore::extent_offsets_t::const_iterator _extents_end_it;

    // The base offset of the currently referenced extent. Set _before_ batches
    // in the current extent are processed by the `filter` & `sink`.
    kafka::offset _extent_base_offset{0};

    // The last offset of the previously referenced extent. Set _after_ batches
    // in the previous extent are processed by the `filter` & `sink`.
    kafka::offset _extent_last_offset;

    // The key-offset map for this run of compaction. Built up from existing
    // data during `map_building_iteration()` by iterating over `_dirty_ranges`
    // and used for removal of old keys in `deduplication_iteration`.
    compaction::key_offset_map* _map;

    metastore* _metastore;
    io* _io;

    ss::abort_source& _as;
    compaction_job_state& _state;
    compaction_worker_probe& _probe;

    // The start offset (inclusive) for the next round of
    // `deduplication_iteration()`.
    kafka::offset _next_deduplication_start_offset;
};

} // namespace cloud_topics::l1
