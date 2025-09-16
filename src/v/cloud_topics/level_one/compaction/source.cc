/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/source.h"

#include "compaction/reducer.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

compaction_source::compaction_source(
  model::topic_id_partition tp,
  ss::abort_source& as,
  compaction_job_state& state)
  : _tp(tp)
  , _as(as)
  , _state(state) {}

ss::future<> compaction_source::initialize() { co_return; }

ss::future<ss::stop_iteration> compaction_source::map_building_iteration() {
    co_return ss::stop_iteration::yes;
}

ss::future<ss::stop_iteration> compaction_source::deduplication_iteration(
  compaction::sliding_window_reducer::sink&) {
    co_return ss::stop_iteration::yes;
}

} // namespace cloud_topics::l1
