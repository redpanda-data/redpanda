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

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/types.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>

#include <chrono>

namespace cluster {

struct node_health_report;

class partition_balancer_backend {
public:
    static constexpr ss::shard_id shard = 0;

    partition_balancer_backend(
      consensus_ptr raft0,
      ss::sharded<controller_stm>&,
      ss::sharded<partition_balancer_state>&,
      ss::sharded<health_monitor_backend>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<topics_frontend>&,
      ss::sharded<members_frontend>&,
      config::binding<model::partition_autobalancing_mode>&& mode,
      config::binding<std::chrono::seconds>&& availability_timeout,
      config::binding<unsigned>&& max_disk_usage_percent,
      config::binding<unsigned>&& storage_space_alert_free_threshold_percent,
      config::binding<std::chrono::milliseconds>&& tick_interval,
      config::binding<size_t>&& max_concurrent_actions,
      config::binding<double>&& moves_drop_threshold,
      config::binding<size_t>&& segment_fallocation_step,
      config::binding<std::optional<size_t>> min_partition_size_threshold,
      config::binding<std::chrono::milliseconds> node_status_interval,
      config::binding<size_t> raft_learner_recovery_rate,
      config::binding<bool> topic_aware);

    void start();
    ss::future<> stop();

    bool is_leader() const { return _raft0->is_leader(); }

    bool is_enabled() const;

    std::optional<model::node_id> leader_id() const {
        auto leader_id = _raft0->get_leader_id();
        if (leader_id && leader_id == _raft0->self().id() && !is_leader()) {
            return std::nullopt;
        }
        return leader_id;
    }

    partition_balancer_overview_reply overview() const;

    ss::future<std::error_code> request_rebalance();

private:
    void tick();
    ss::future<> do_tick();

    /// If now, rearms to run immediately, else rearms to _tick_interval or
    /// current timeout whichever is minimum.
    void maybe_rearm_timer(bool now = false);
    void on_members_update(model::node_id, model::membership_state);
    void on_topic_table_update();
    void on_health_monitor_update(
      const node_health_report&,
      std::optional<ss::lw_shared_ptr<const node_health_report>>);
    size_t get_min_partition_size_threshold() const;

private:
    using clock_t = ss::lowres_clock;
    consensus_ptr _raft0;

    controller_stm& _controller_stm;
    partition_balancer_state& _state;
    health_monitor_backend& _health_monitor;
    partition_allocator& _partition_allocator;
    topics_frontend& _topics_frontend;
    members_frontend& _members_frontend;

    config::binding<model::partition_autobalancing_mode> _mode;
    config::binding<std::chrono::seconds> _availability_timeout;
    config::binding<unsigned> _max_disk_usage_percent;
    config::binding<unsigned> _storage_space_alert_free_threshold_percent;
    config::binding<std::chrono::milliseconds> _tick_interval;
    config::binding<size_t> _max_concurrent_actions;
    config::binding<double> _concurrent_moves_drop_threshold;
    config::binding<size_t> _segment_fallocation_step;
    config::binding<std::optional<size_t>> _min_partition_size_threshold;
    config::binding<std::chrono::milliseconds> _node_status_interval;
    config::binding<size_t> _raft_learner_recovery_rate;
    config::binding<bool> _topic_aware;

    mutex _lock{"partition_balancer_backend::lock"};
    ss::gate _gate;
    ss::timer<clock_t> _timer;
    notification_id_type _topic_table_updates;
    notification_id_type _member_updates;
    notification_id_type _health_monitor_updates;

    // Balancer runs in a series of ticks during a controller leadership term.
    // While the term stays the same, we know that no other balancer instance
    // runs concurrently with us, so the backend state from the previous tick
    // remains valid. But if the term changes, this means that the backend state
    // is obsolete and must be reset.
    struct per_term_state {
        explicit per_term_state(model::term_id term)
          : id(term)
          , term_start_time(clock_t::now()) {}

        model::term_id id;
        clock_t::time_point term_start_time;
        clock_t::time_point last_tick_time;
        partition_balancer_violations last_violations;
        partition_balancer_status last_status
          = partition_balancer_status::starting;
        size_t last_tick_in_progress_updates = 0;

        absl::flat_hash_map<model::node_id, absl::btree_set<model::ntp>>
          last_tick_decommission_realloc_failures;

        bool _ondemand_rebalance_requested = false;
        bool _force_health_report_refresh = false;
    };
    std::optional<per_term_state> _cur_term;

    std::optional<ss::abort_source> _tick_in_progress;
};

} // namespace cluster
