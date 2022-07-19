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
#include "cluster/logger.h"
#include "cluster/partition_balancer_planner.h"
#include "cluster/topics_frontend.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cluster {

static constexpr std::chrono::seconds controller_stm_sync_timeout = 10s;
static constexpr std::chrono::seconds add_move_cmd_timeout = 10s;

partition_balancer_backend::partition_balancer_backend(
  consensus_ptr raft0,
  ss::sharded<controller_stm>& controller_stm,
  ss::sharded<topic_table>& topic_table,
  ss::sharded<health_monitor_frontend>& health_monitor,
  ss::sharded<partition_allocator>& partition_allocator,
  ss::sharded<topics_frontend>& topics_frontend,
  config::binding<model::partition_autobalancing_mode>&& mode,
  config::binding<std::chrono::seconds>&& availability_timeout,
  config::binding<unsigned>&& max_disk_usage_percent,
  config::binding<std::chrono::milliseconds>&& tick_interval,
  config::binding<size_t>&& movement_batch_size_bytes)
  : _raft0(std::move(raft0))
  , _controller_stm(controller_stm.local())
  , _topic_table(topic_table.local())
  , _health_monitor(health_monitor.local())
  , _partition_allocator(partition_allocator.local())
  , _topics_frontend(topics_frontend.local())
  , _mode(std::move(mode))
  , _availability_timeout(std::move(availability_timeout))
  , _max_disk_usage_percent(std::move(max_disk_usage_percent))
  , _tick_interval(std::move(tick_interval))
  , _movement_batch_size_bytes(std::move(movement_batch_size_bytes))
  , _timer([this] { tick(); }) {}

void partition_balancer_backend::start() {
    if (is_enabled()) {
        vlog(clusterlog.info, "partition autobalancing enabled");
        _timer.arm(_tick_interval());
    }

    _mode.watch([this] { on_mode_changed(); });
}

void partition_balancer_backend::tick() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return do_tick().finally([this] {
                              if (is_enabled() && !_gate.is_closed()) {
                                  _timer.arm(_tick_interval());
                              }
                          });
                      }).handle_exception([](const std::exception_ptr& e) {
        vlog(clusterlog.warn, "tick error: {}", e);
    });
}

void partition_balancer_backend::on_mode_changed() {
    if (_gate.is_closed()) {
        return;
    }

    if (is_enabled()) {
        vlog(clusterlog.info, "partition autobalancing enabled");
        if (!_timer.armed()) {
            _timer.arm(0ms);
        }
    } else {
        vlog(clusterlog.info, "partition autobalancing disabled");
        _timer.cancel();
    }
}

ss::future<> partition_balancer_backend::stop() {
    vlog(clusterlog.info, "stopping...");
    _timer.cancel();
    return _gate.close();
}

ss::future<> partition_balancer_backend::do_tick() {
    if (!_raft0->is_leader()) {
        vlog(clusterlog.debug, "not leader, skipping tick");
        co_return;
    }

    vlog(clusterlog.debug, "tick");

    auto current_term = _raft0->term();

    co_await _controller_stm.wait(
      _raft0->committed_offset(),
      model::timeout_clock::now() + controller_stm_sync_timeout);

    if (_raft0->term() != current_term) {
        vlog(clusterlog.debug, "lost leadership, exiting");
        co_return;
    }

    auto health_report
      = co_await _health_monitor.get_current_cluster_health_snapshot(
        cluster_report_filter{});

    auto plan_data
      = partition_balancer_planner(
          planner_config{
            .max_disk_usage_ratio = _max_disk_usage_percent() / 100.0,
            .movement_disk_size_batch = _movement_batch_size_bytes(),
            .node_availability_timeout_sec = _availability_timeout(),
          },
          _topic_table,
          _partition_allocator)
          .plan_reassignments(health_report, _raft0->get_follower_metrics());

    if (!plan_data.violations.is_empty()) {
        vlog(
          clusterlog.info,
          "violations: {} unavailable nodes, {} full nodes; planned {} "
          "reassignments; cancelled {} reassignments",
          plan_data.violations.unavailable_nodes.size(),
          plan_data.violations.full_nodes.size(),
          plan_data.reassignments.size(),
          plan_data.cancellations.size());
    }

    _last_leader_term = _raft0->term();
    _last_tick_time = ss::lowres_clock::now();
    _last_violations = std::move(plan_data.violations);

    if (
      _topic_table.has_updates_in_progress() || !plan_data.reassignments.empty()
      || !plan_data.cancellations.empty()) {
        _last_status = partition_balancer_status::in_progress;
    } else if (plan_data.failed_reassignments_count > 0) {
        _last_status = partition_balancer_status::stalled;
    } else {
        _last_status = partition_balancer_status::ready;
    }

    co_await ss::max_concurrent_for_each(
      plan_data.cancellations, 32, [this, current_term](model::ntp& ntp) {
          vlog(clusterlog.info, "cancel movement for ntp {}", ntp);
          return _topics_frontend
            .cancel_moving_partition_replicas(
              ntp,
              model::timeout_clock::now() + add_move_cmd_timeout,
              current_term)
            .then([ntp = std::move(ntp)](auto errc) {
                vlog(
                  clusterlog.info,
                  "{} movement cancellation submitted, errc: {}",
                  ntp,
                  errc);
            });
      });

    co_await ss::max_concurrent_for_each(
      plan_data.reassignments,
      32,
      [this, current_term](ntp_reassignments& reassignment) {
          vlog(
            clusterlog.info,
            "moving {} to {}",
            reassignment.ntp,
            reassignment.allocation_units.get_assignments().front().replicas);

          return _topics_frontend
            .move_partition_replicas(
              reassignment.ntp,
              reassignment.allocation_units.get_assignments().front().replicas,
              model::timeout_clock::now() + add_move_cmd_timeout,
              current_term)
            .then([reassignment = std::move(reassignment)](auto errc) {
                vlog(
                  clusterlog.info,
                  "{} reassignment submitted, errc: {}",
                  reassignment.ntp,
                  errc);
            });
      });
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

    auto now = ss::lowres_clock::now();
    auto time_since_last_tick = now - _last_tick_time;
    ret.last_tick_time = model::to_timestamp(
      model::timestamp_clock::now()
      - std::chrono::duration_cast<model::timestamp_clock::duration>(
        time_since_last_tick));

    ret.error = errc::success;
    return ret;
}

} // namespace cluster
