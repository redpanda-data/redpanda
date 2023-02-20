/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/types.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cluster {

class health_manager {
    using clock_type = ss::lowres_clock;
    static constexpr std::chrono::seconds set_replicas_timeout = 15s;
    // after changing replica set introduce a short cooling off delay
    static constexpr std::chrono::seconds stabilize_delay = 10s;

public:
    static constexpr ss::shard_id shard = 0;

    health_manager(
      model::node_id,
      size_t,
      std::chrono::milliseconds,
      ss::sharded<topic_table>&,
      ss::sharded<topics_frontend>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<members_table>&,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<bool> ensure_topic_replication(model::topic_namespace_view);
    ss::future<bool> ensure_partition_replication(model::ntp);
    void tick();
    ss::future<> do_tick();

    model::node_id _self;
    size_t _target_replication_factor;
    std::chrono::milliseconds _tick_interval;
    ss::sharded<topic_table>& _topics;
    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<members_table>& _members;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _gate;
    ss::timer<clock_type> _timer;
};

} // namespace cluster
