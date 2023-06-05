/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_backend.h"

#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_balancer_planner.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "config/property.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>

#include <chrono>
#include <optional>

using namespace std::chrono_literals;
using planner_status = cluster::partition_balancer_planner::status;

namespace cluster {

static constexpr std::chrono::seconds controller_stm_sync_timeout = 10s;
static constexpr std::chrono::seconds add_move_cmd_timeout = 10s;

partition_balancer_backend::partition_balancer_backend(
  consensus_ptr raft0,
  ss::sharded<controller_stm>& controller_stm,
  ss::sharded<partition_balancer_state>& state,
  ss::sharded<health_monitor_frontend>& health_monitor,
  ss::sharded<partition_allocator>& partition_allocator,
  ss::sharded<topics_frontend>& topics_frontend,
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
  config::binding<size_t> raft_learner_recovery_rate)
  : _raft0(std::move(raft0))
  , _controller_stm(controller_stm.local())
  , _state(state.local())
  , _health_monitor(health_monitor.local())
  , _partition_allocator(partition_allocator.local())
  , _topics_frontend(topics_frontend.local())
  , _mode(std::move(mode))
  , _availability_timeout(std::move(availability_timeout))
  , _max_disk_usage_percent(std::move(max_disk_usage_percent))
  , _storage_space_alert_free_threshold_percent(
      std::move(storage_space_alert_free_threshold_percent))
  , _tick_interval(std::move(tick_interval))
  , _movement_batch_size_bytes(std::move(movement_batch_size_bytes))
  , _max_concurrent_actions(std::move(max_concurrent_actions))
  , _concurrent_moves_drop_threshold(std::move(moves_drop_threshold))
  , _segment_fallocation_step(std::move(segment_fallocation_step))
  , _min_partition_size_threshold(std::move(min_partition_size_threshold))
  , _node_status_interval(std::move(node_status_interval))
  , _raft_learner_recovery_rate(std::move(raft_learner_recovery_rate))
  , _timer([this] { tick(); }) {}

void partition_balancer_backend::start() {
    _topic_table_updates = _state.topics().register_lw_notification(
      [this]() { on_topic_table_update(); });
    _member_updates = _state.members().register_members_updated_notification(
      [this](model::node_id n, model::membership_state state) {
          on_members_update(n, state);
      });
    maybe_rearm_timer();
    vlog(clusterlog.info, "partition balancer started");
}

void partition_balancer_backend::maybe_rearm_timer(bool now) {
    if (_gate.is_closed()) {
        return;
    }
    auto schedule_at = now ? clock_t::now() : clock_t::now() + _tick_interval();
    auto duration_ms = [](clock_t::time_point time_point) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                 time_point - clock_t::now())
          .count();
    };
    if (_timer.armed()) {
        schedule_at = std::min(schedule_at, _timer.get_timeout());
        _timer.rearm(schedule_at);
        vlog(
          clusterlog.debug,
          "Tick rescheduled to run in: {}ms",
          duration_ms(schedule_at));
    } else if (_lock.waiters() == 0) {
        _timer.arm(schedule_at);
        vlog(
          clusterlog.debug,
          "Tick scheduled to run in: {}ms",
          duration_ms(schedule_at));
    }
}

void partition_balancer_backend::on_members_update(
  model::node_id, model::membership_state state) {
    if (!is_leader()) {
        return;
    }
    if (
      state == model::membership_state::active
      || state == model::membership_state::draining) {
        maybe_rearm_timer(/*now = */ true);
    }
}

void partition_balancer_backend::on_topic_table_update() {
    if (!is_leader()) {
        return;
    }
    auto current_in_progress_updates
      = _state.topics().updates_in_progress().size();
    auto schedule_now = double(current_in_progress_updates)
                        < (1 - _concurrent_moves_drop_threshold())
                            * double(_last_tick_in_progress_updates);
    if (schedule_now) {
        maybe_rearm_timer(/*now=*/true);
    }
}

void partition_balancer_backend::tick() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return do_tick().finally(
                            [this] { maybe_rearm_timer(); });
                      }).handle_exception([](const std::exception_ptr& e) {
        vlog(clusterlog.warn, "tick error: {}", e);
    });
}

ss::future<> partition_balancer_backend::stop() {
    vlog(clusterlog.info, "stopping...");
    _state.topics().unregister_lw_notification(_topic_table_updates);
    _state.members().unregister_members_updated_notification(_member_updates);
    _timer.cancel();
    _lock.broken();
    return _gate.close();
}

ss::future<> partition_balancer_backend::do_tick() {
    if (!_raft0->is_leader()) {
        vlog(clusterlog.debug, "not leader, skipping tick");
        co_return;
    }

    auto units = co_await _lock.get_units();

    vlog(clusterlog.debug, "tick");

    auto current_term = _raft0->term();

    co_await _controller_stm.wait(
      _raft0->committed_offset(),
      model::timeout_clock::now() + controller_stm_sync_timeout);

    if (_raft0->term() != current_term) {
        vlog(clusterlog.debug, "lost leadership, exiting");
        co_return;
    }

    auto health_report = co_await _health_monitor.get_cluster_health(
      cluster_report_filter{},
      force_refresh::no,
      model::timeout_clock::now() + controller_stm_sync_timeout);
    if (!health_report) {
        vlog(
          clusterlog.info,
          "unable to get health report - {}",
          health_report.error().message());
        co_return;
    }

    if (!_raft0->is_leader() || _raft0->term() != current_term) {
        vlog(clusterlog.debug, "lost leadership, exiting");
        co_return;
    }

    double soft_max_disk_usage_ratio = _max_disk_usage_percent() / 100.0;
    double hard_max_disk_usage_ratio
      = (100 - _storage_space_alert_free_threshold_percent()) / 100.0;
    // claim node unresponsive it doesn't responded to at least 7
    // status requests by default 700ms
    auto const node_responsiveness_timeout = _node_status_interval() * 7;
    auto plan_data
      = partition_balancer_planner(
          planner_config{
            .mode = _mode(),
            .soft_max_disk_usage_ratio = soft_max_disk_usage_ratio,
            .hard_max_disk_usage_ratio = hard_max_disk_usage_ratio,
            .movement_disk_size_batch = _movement_batch_size_bytes(),
            .max_concurrent_actions = _max_concurrent_actions(),
            .node_availability_timeout_sec = _availability_timeout(),
            .segment_fallocation_step = _segment_fallocation_step(),
            .min_partition_size_threshold = get_min_partition_size_threshold(),
            .node_responsiveness_timeout = node_responsiveness_timeout},
          _state,
          _partition_allocator)
          .plan_actions(health_report.value());

    _last_leader_term = _raft0->term();
    _last_tick_time = clock_t::now();
    _last_violations = std::move(plan_data.violations);
    if (
      _state.topics().has_updates_in_progress()
      || plan_data.status == planner_status::actions_planned) {
        _last_status = partition_balancer_status::in_progress;
    } else if (plan_data.status == planner_status::waiting_for_reports) {
        _last_status = partition_balancer_status::starting;
    } else if (
      plan_data.failed_actions_count > 0
      || plan_data.status == planner_status::waiting_for_maintenance_end) {
        _last_status = partition_balancer_status::stalled;
    } else {
        _last_status = partition_balancer_status::ready;
    }

    if (_last_status != partition_balancer_status::ready) {
        vlog(
          clusterlog.info,
          "last status: {}; "
          "violations: unavailable nodes: {}, full nodes: {}; "
          "updates in progress: {}; "
          "action counts: reassignments: {}, cancellations: {}, failed: {}",
          _last_status,
          _last_violations.unavailable_nodes.size(),
          _last_violations.full_nodes.size(),
          _state.topics().updates_in_progress().size(),
          plan_data.reassignments.size(),
          plan_data.cancellations.size(),
          plan_data.failed_actions_count);
    }

    auto moves_before = _state.topics().updates_in_progress().size();

    co_await ss::max_concurrent_for_each(
      plan_data.cancellations, 32, [this, current_term](model::ntp& ntp) {
          auto f = _topics_frontend.cancel_moving_partition_replicas(
            ntp,
            model::timeout_clock::now() + add_move_cmd_timeout,
            current_term);

          return f.then([ntp = std::move(ntp)](auto errc) {
              if (errc) {
                  vlog(
                    clusterlog.warn,
                    "submitting {} movement cancellation failed, error: {}",
                    ntp,
                    errc.message());
              }
          });
      });

    co_await ss::max_concurrent_for_each(
      plan_data.reassignments,
      32,
      [this, current_term](ntp_reassignment& reassignment) {
          auto f = _topics_frontend.move_partition_replicas(
            reassignment.ntp,
            reassignment.allocated.replicas(),
            model::timeout_clock::now() + add_move_cmd_timeout,
            current_term);
          return f.then([reassignment = std::move(reassignment)](auto errc) {
              if (errc) {
                  vlog(
                    clusterlog.warn,
                    "submitting {} reassignment failed, error: {}",
                    reassignment.ntp,
                    errc.message());
              }
          });
      });

    _last_tick_in_progress_updates = moves_before
                                     + plan_data.cancellations.size()
                                     + plan_data.reassignments.size();
}

partition_balancer_overview_reply partition_balancer_backend::overview() const {
    vassert(ss::this_shard_id() == shard, "called on a wrong shard");

    partition_balancer_overview_reply ret;

    if (_mode() != model::partition_autobalancing_mode::continuous) {
        ret.status = partition_balancer_status::off;
        ret.error = errc::feature_disabled;
        return ret;
    }

    if (!is_leader()) {
        ret.error = errc::not_leader;
        return ret;
    }

    if (_raft0->term() != _last_leader_term) {
        // we haven't done a single tick in this term yet, return empty response
        ret.status = partition_balancer_status::starting;
        ret.error = errc::success;
        return ret;
    }

    ret.status = _last_status;
    ret.violations = _last_violations;

    auto now = clock_t::now();
    auto time_since_last_tick = now - _last_tick_time;
    ret.last_tick_time = model::to_timestamp(
      model::timestamp_clock::now()
      - std::chrono::duration_cast<model::timestamp_clock::duration>(
        time_since_last_tick));

    ret.error = errc::success;
    return ret;
}

size_t partition_balancer_backend::get_min_partition_size_threshold() const {
    // if there is an override coming from cluster config use it
    if (_min_partition_size_threshold()) {
        return _min_partition_size_threshold().value();
    }

    // TODO: replace partition_autobalancing_concurrent_moves after we have it
    // as a field in balancer backend
    const auto min_rate
      = _raft_learner_recovery_rate()
        / config::shard_local_cfg().partition_autobalancing_concurrent_moves();

    /**
     * We use a heuristic to calculate the minimum size of of partition, we
     * want that the partition with the threshold size to have enough data that
     * it will move for at least ten seconds.
     */
    return min_rate * 10;
}

} // namespace cluster
