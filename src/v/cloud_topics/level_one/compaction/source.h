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

#include "compaction/reducer.h"

namespace cloud_topics::l1 {

enum class compaction_job_state { idle, running, stopped };

class compaction_source : public compaction::sliding_window_reducer::source {
public:
    compaction_source(
      model::topic_id_partition, ss::abort_source&, compaction_job_state&);
    ss::future<> initialize() final;
    ss::future<ss::stop_iteration> map_building_iteration() final;
    ss::future<ss::stop_iteration>
    deduplication_iteration(compaction::sliding_window_reducer::sink&) final;

private:
    [[maybe_unused]] model::topic_id_partition _tp;
    [[maybe_unused]] ss::abort_source& _as;
    [[maybe_unused]] compaction_job_state& _state;
};

} // namespace cloud_topics::l1
