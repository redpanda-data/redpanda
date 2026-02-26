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

#include "cloud_topics/level_one/maintenance/object_sink.h"

namespace cloud_topics::l1 {

/// Sink for leveling jobs. Reuses L1 object building logic from
/// `l1_object_sink` but commits via `replace_objects()` rather than
/// `compact_objects()`, since leveling does not modify compaction state
/// (cleaned ranges, tombstones).
class leveling_sink : public l1_object_sink {
public:
    leveling_sink(
      model::topic_id_partition,
      l1::io*,
      l1::metastore*,
      ss::abort_source&,
      config::binding<size_t> max_object_size,
      object_builder::options = {});

    ss::future<bool>
    initialize(compaction::sliding_window_reducer::source&) final;

    ss::future<ss::stop_iteration> operator()(model::record_batch) final;

    ss::future<> finalize(bool success) final;
};

} // namespace cloud_topics::l1
