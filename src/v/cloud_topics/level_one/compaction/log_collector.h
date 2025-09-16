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

#include "base/seastarx.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

class partition_leaders_table;
class topic_table;

} // namespace cluster

namespace cloud_topics::l1 {

class compaction_scheduler;

class log_collector {
public:
    log_collector(compaction_scheduler*);

    virtual ~log_collector() noexcept = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

protected:
    compaction_scheduler* _scheduler;
};

struct compaction_cluster_state {
    model::node_id self;
    ss::sharded<cluster::partition_leaders_table>* leaders;
    ss::sharded<cluster::topic_table>* topic_table;
};

std::unique_ptr<log_collector>
make_default_log_collector(compaction_scheduler*, compaction_cluster_state);

} // namespace cloud_topics::l1
