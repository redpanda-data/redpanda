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
#include "cloud_topics/level_one/metastore/leveling_range_builder.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "compaction/reducer.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

class leveling_sink;

/// Source for leveling jobs. Unlike compaction, leveling does not build a
/// key-offset map and does not perform deduplication. It reads extents within
/// the identified leveling ranges and passes all data through to the sink to
/// be rewritten into properly-sized L1 objects.
class leveling_source : public compaction::sliding_window_reducer::source {
public:
    leveling_source(
      model::ntp,
      model::topic_id_partition,
      chunked_vector<levelable_range> leveling_ranges,
      metastore*,
      io*,
      ss::abort_source&,
      compaction_job_state&,
      prefix_logger&);

    ss::future<> initialize() final;

    // No-op: leveling does not build a key-offset map.
    ss::future<ss::stop_iteration> map_building_iteration() final;

    // Reads extents aligned to leveling ranges and passes all data through
    // to the sink.
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

private:
    bool preempted() const;

    friend leveling_sink;

    const model::ntp _ntp;
    const model::topic_id_partition _tp;

    // Offset ranges identified as needing leveling (extents in
    // undersized/fragmented L1 objects).
    chunked_vector<levelable_range> _leveling_ranges;

    // Cursor into `_leveling_ranges`. deduplication_iteration() processes the
    // range it points at — iterating that range's extents one at a time — then
    // advances it, one range per call, until the ranges are exhausted.
    chunked_vector<levelable_range>::iterator _range_it;

    metastore* _metastore;
    io* _io;

    ss::abort_source& _as;
    compaction_job_state& _state;
    prefix_logger& _ctxlog;
};

} // namespace cloud_topics::l1
