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

    auto reassignments
      = partition_balancer_planner(
          planner_config{
            .max_disk_usage_ratio = _max_disk_usage_percent() / 100.0,
            .movement_disk_size_batch = _movement_batch_size_bytes(),
            .node_availability_timeout_sec = _availability_timeout(),
          },
          _topic_table,
          _partition_allocator)
          .get_ntp_reassignments(health_report, _raft0->get_follower_metrics());

    co_await ss::max_concurrent_for_each(
      reassignments, 32, [this, current_term](ntp_reassignments& reassignment) {
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

} // namespace cluster
