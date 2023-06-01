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
#include "cluster/partition_balancer_types.h"
#include "cluster/types.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "seastarx.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>

#include <chrono>

namespace cluster {

class partition_balancer_backend {
public:
    static constexpr ss::shard_id shard = 0;

    partition_balancer_backend(
      consensus_ptr raft0,
      ss::sharded<controller_stm>&,
      ss::sharded<partition_balancer_state>&,
      ss::sharded<health_monitor_frontend>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<topics_frontend>&,
      config::binding<model::partition_autobalancing_mode>&& mode,
      config::binding<std::chrono::seconds>&& availability_timeout,
      config::binding<unsigned>&& max_disk_usage_percent,
      config::binding<unsigned>&& storage_space_alert_free_threshold_percent,
      config::binding<std::chrono::milliseconds>&& tick_interval,
      config::binding<size_t>&& movement_batch_size_bytes,
      config::binding<size_t>&& max_concurrent_actions,
      config::binding<double>&& moves_drop_threshold,
      config::binding<size_t>&& segment_fallocation_step,
      config::binding<std::optional<size_t>> min_partition_size_threshold,
      config::binding<std::chrono::milliseconds> node_status_interval,
      config::binding<size_t> raft_learner_recovery_rate);

    void start();
    ss::future<> stop();

    bool is_leader() const { return _raft0->is_leader(); }

    std::optional<model::node_id> leader_id() const {
        auto leader_id = _raft0->get_leader_id();
        if (leader_id && leader_id == _raft0->self().id() && !is_leader()) {
            return std::nullopt;
        }
        return leader_id;
    }

    partition_balancer_overview_reply overview() const;

private:
    void tick();
    ss::future<> do_tick();

    /// If now, rearms to run immediately, else rearms to _tick_interval or
    /// current timeout whichever is minimum.
    void maybe_rearm_timer(bool now = false);
    void on_members_update(model::node_id, model::membership_state);
    void on_topic_table_update();
    size_t get_min_partition_size_threshold() const;

private:
    using clock_t = ss::lowres_clock;
    consensus_ptr _raft0;

    controller_stm& _controller_stm;
    partition_balancer_state& _state;
    health_monitor_frontend& _health_monitor;
    partition_allocator& _partition_allocator;
    topics_frontend& _topics_frontend;

    config::binding<model::partition_autobalancing_mode> _mode;
    config::binding<std::chrono::seconds> _availability_timeout;
    config::binding<unsigned> _max_disk_usage_percent;
    config::binding<unsigned> _storage_space_alert_free_threshold_percent;
    config::binding<std::chrono::milliseconds> _tick_interval;
    config::binding<size_t> _movement_batch_size_bytes;
    config::binding<size_t> _max_concurrent_actions;
    config::binding<double> _concurrent_moves_drop_threshold;
    config::binding<size_t> _segment_fallocation_step;
    config::binding<std::optional<size_t>> _min_partition_size_threshold;
    config::binding<std::chrono::milliseconds> _node_status_interval;
    config::binding<size_t> _raft_learner_recovery_rate;

    model::term_id _last_leader_term;
    clock_t::time_point _last_tick_time;
    partition_balancer_violations _last_violations;
    partition_balancer_status _last_status;
    size_t _last_tick_in_progress_updates = 0;

    mutex _lock{};
    ss::gate _gate;
    ss::timer<clock_t> _timer;
    notification_id_type _topic_table_updates;
    notification_id_type _member_updates;
};

} // namespace cluster
