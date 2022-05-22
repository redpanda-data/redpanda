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

#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class partition_balancer_backend {
public:
    static constexpr ss::shard_id shard = 0;

    partition_balancer_backend(
      consensus_ptr raft0,
      ss::sharded<controller_stm>&,
      ss::sharded<topic_table>&,
      ss::sharded<health_monitor_frontend>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<topics_frontend>&,
      config::binding<model::partition_autobalancing_mode>&& mode,
      config::binding<std::chrono::seconds>&& availability_timeout,
      config::binding<unsigned>&& max_disk_usage_percent,
      config::binding<std::chrono::milliseconds>&& tick_interval,
      config::binding<size_t>&& movement_batch_size_bytes);

    void start();
    ss::future<> stop();

private:
    void tick();
    ss::future<> do_tick();

    void on_mode_changed();

    bool is_enabled() const {
        return _mode() == model::partition_autobalancing_mode::continuous;
    }

private:
    consensus_ptr _raft0;

    controller_stm& _controller_stm;
    topic_table& _topic_table;
    health_monitor_frontend& _health_monitor;
    partition_allocator& _partition_allocator;
    topics_frontend& _topics_frontend;

    config::binding<model::partition_autobalancing_mode> _mode;
    config::binding<std::chrono::seconds> _availability_timeout;
    config::binding<unsigned> _max_disk_usage_percent;
    config::binding<std::chrono::milliseconds> _tick_interval;
    config::binding<size_t> _movement_batch_size_bytes;

    ss::gate _gate;
    ss::timer<ss::lowres_clock> _timer;
};

} // namespace cluster
