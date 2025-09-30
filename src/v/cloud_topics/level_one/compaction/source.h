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
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"

namespace cloud_topics::l1 {

enum class compaction_job_state { idle, running, stopped };

class compaction_source : public compaction::sliding_window_reducer::source {
public:
    compaction_source(
      model::ntp,
      model::topic_id_partition,
      metastore::compaction_offsets_response,
      compaction::key_offset_map*,
      metastore*,
      io*,
      ss::abort_source&,
      compaction_job_state&);
    ss::future<> initialize() final;
    ss::future<ss::stop_iteration> map_building_iteration() final;
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

private:
    // Returns true if the compaction process has been pre-empted to stop.
    bool preempted() const;

private:
    const model::ntp _ntp;
    const model::topic_id_partition _tp;

    // Offset ranges for the contained `topic_id_partition` obtained from the
    // metastore.
    const offset_interval_set _dirty_ranges;
    const offset_interval_set _removable_tombstone_ranges;

    // The key-offset map for this run of compaction. Built up from existing
    // data during `map_building_iteration()` by iterating over `_dirty_ranges`
    // and used for removal of old keys in `deduplication_iteration`.
    compaction::key_offset_map* _map;

    metastore* _metastore;
    io* _io;

    ss::abort_source& _as;
    compaction_job_state& _state;

    // Container representation of above `_dirty_ranges`.
    using interval_vec = chunked_vector<offset_interval_set::interval>;
    const interval_vec _dirty_range_intervals;

    // Iterator used during `map_building_iteration()` which points into the
    // above vector `_dirty_range_intervals`.
    interval_vec::const_iterator _map_building_it;

    // The offset intervals that were indexed during
    // `map_building_iteration()`s.
    // TODO: could potentially be represented using only one offset for the
    // highest indexed record.
    offset_interval_set _indexed_intervals;
};

} // namespace cloud_topics::l1
