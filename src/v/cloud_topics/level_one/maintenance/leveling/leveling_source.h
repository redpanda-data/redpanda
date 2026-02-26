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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/reducer.h"

namespace cloud_topics::l1 {

class leveling_sink;

// Source for leveling jobs. Unlike compaction, leveling does not build a
// key-offset map and does not perform deduplication. It simply reads extents
// within the identified leveling ranges and passes all data through to the
// sink to be rewritten into properly-sized L1 objects.
class leveling_source : public compaction::sliding_window_reducer::source {
public:
    leveling_source(
      model::ntp,
      model::topic_id_partition,
      chunked_vector<offset_interval_set::interval> leveling_ranges,
      metastore*,
      io*,
      ss::abort_source&,
      maintenance_job_state&,
      maintenance_worker_probe&);

    ss::future<> initialize() final;

    // No-op: leveling does not build a key-offset map.
    ss::future<ss::stop_iteration> map_building_iteration() final;

    // Reads extents aligned to leveling ranges and passes all data through
    // the leveling filter to the sink.
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

private:
    bool preempted() const;

private:
    friend leveling_sink;

    const model::ntp _ntp;
    const model::topic_id_partition _tp;

    // Offset ranges identified as needing leveling (extents in
    // undersized/fragmented L1 objects).
    chunked_vector<offset_interval_set::interval> _leveling_ranges;

    // Iterator over _leveling_ranges for the current range being processed.
    chunked_vector<offset_interval_set::interval>::iterator _range_it;

    metastore* _metastore;
    io* _io;

    ss::abort_source& _as;
    maintenance_job_state& _state;
    maintenance_worker_probe& _probe;
};

} // namespace cloud_topics::l1
