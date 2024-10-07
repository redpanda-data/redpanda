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

#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_frontend.h"
#include "cluster/members_table.h"
#include "cluster/partition_balancer_planner.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/property.h"
#include "random/generators.h"
#include "utils/stable_iterator_adaptor.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>

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
  ss::sharded<health_monitor_backend>& health_monitor,
  ss::sharded<partition_allocator>& partition_allocator,
  ss::sharded<topics_frontend>& topics_frontend,
  ss::sharded<members_frontend>& members_frontend,
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
  config::binding<bool> topic_aware)
  : _raft0(std::move(raft0))
  , _controller_stm(controller_stm.local())
  , _state(state.local())
  , _health_monitor(health_monitor.local())
  , _partition_allocator(partition_allocator.local())
  , _topics_frontend(topics_frontend.local())
  , _members_frontend(members_frontend.local())
  , _mode(std::move(mode))
  , _availability_timeout(std::move(availability_timeout))
  , _max_disk_usage_percent(std::move(max_disk_usage_percent))
  , _storage_space_alert_free_threshold_percent(
      std::move(storage_space_alert_free_threshold_percent))
  , _tick_interval(std::move(tick_interval))
  , _max_concurrent_actions(std::move(max_concurrent_actions))
  , _concurrent_moves_drop_threshold(std::move(moves_drop_threshold))
  , _segment_fallocation_step(std::move(segment_fallocation_step))
  , _min_partition_size_threshold(std::move(min_partition_size_threshold))
  , _node_status_interval(std::move(node_status_interval))
  , _raft_learner_recovery_rate(std::move(raft_learner_recovery_rate))
  , _topic_aware(std::move(topic_aware))
  , _timer([this] { tick(); }) {}

bool partition_balancer_backend::is_enabled() const {
    return is_leader() && !config::node().recovery_mode_enabled();
}

void partition_balancer_backend::start() {
    _topic_table_updates = _state.topics().register_lw_ntp_notification(
      [this]() { on_topic_table_update(); });
    _member_updates = _state.members().register_members_updated_notification(
      [this](model::node_id n, model::membership_state state) {
          on_members_update(n, state);
      });
    _health_monitor_updates = _health_monitor.register_node_callback(
      [this](const auto& report, auto old_report) {
          on_health_monitor_update(report, old_report);
      });
    maybe_rearm_timer();
    vlog(clusterlog.info, "partition balancer started");
}

ss::future<std::error_code> partition_balancer_backend::request_rebalance() {
    auto g = _gate.hold();

    if (!is_leader()) {
        co_return errc::not_leader;
    }

    if (config::node().recovery_mode_enabled()) {
        co_return errc::feature_disabled;
    }

    auto units = co_await _lock.get_units();

    if (!is_leader()) {
        co_return errc::not_leader;
    }

    if (!_cur_term || _raft0->term() != _cur_term->id) {
        _cur_term = per_term_state(_raft0->term());
    }

    if (
      _cur_term->_ondemand_rebalance_requested
      || !_state.nodes_to_rebalance().empty()) {
        vlog(
          clusterlog.info,
          "rebalance already in progress, "
          "on demand: {}; nodes to rebalance count: {}",
          _cur_term->_ondemand_rebalance_requested,
          _state.nodes_to_rebalance().size());
        co_return errc::update_in_progress;
    }

    vlog(clusterlog.info, "requesting on demand rebalance");
    _cur_term->_ondemand_rebalance_requested = true;
    maybe_rearm_timer(/*now=*/true);
    co_return errc::success;
}

void partition_balancer_backend::maybe_rearm_timer(bool now) {
    if (_gate.is_closed()) {
        return;
    }
    if (config::node().recovery_mode_enabled()) {
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
  model::node_id id, model::membership_state state) {
    if (!is_enabled()) {
        return;
    }

    if (
      state == model::membership_state::active
      || state == model::membership_state::draining) {
        if (_tick_in_progress) {
            _tick_in_progress->request_abort_ex(balancer_tick_aborted_exception{
              fmt::format("new membership update: {}", state)});
        }
    }

    if (state == model::membership_state::draining) {
        vlog(
          clusterlog.debug,
          "node {} state notification: {}, scheduling tick",
          id,
          state);

        maybe_rearm_timer(/*now = */ true);
    }
    // Only schedule tick on node addition if health report is already
    // available
    if (
      state == model::membership_state::active
      && _health_monitor.contains_node_health_report(id)) {
        vlog(
          clusterlog.debug,
          "node {} state notification: {}, scheduling tick as health report "
          "for node is already present",
          id,
          state);
        maybe_rearm_timer(/*now = */ true);
    }
}

void partition_balancer_backend::on_topic_table_update() {
    if (!is_enabled()) {
        return;
    }

    if (!_cur_term || _raft0->term() != _cur_term->id) {
        // don't try to check how in-progress updates count dropped before doing
        // a single tick in this term.
        return;
    }
    auto last_in_progress_updates = _cur_term->last_tick_in_progress_updates;

    auto current_in_progress_updates
      = _state.topics().updates_in_progress().size();
    auto schedule_now = double(current_in_progress_updates)
                        < (1 - _concurrent_moves_drop_threshold())
                            * double(last_in_progress_updates);
    if (schedule_now) {
        vlog(
          clusterlog.debug,
          "current updates in progress: {} (after last tick: {}), "
          "scheduling tick",
          current_in_progress_updates,
          last_in_progress_updates);

        maybe_rearm_timer(/*now=*/true);
    }
}

void partition_balancer_backend::on_health_monitor_update(
  const node_health_report& report,
  std::optional<ss::lw_shared_ptr<const node_health_report>> old_report) {
    if (!old_report) {
        vlog(
          clusterlog.debug,
          "health report for node {} appeared, scheduling tick",
          report.id);

        maybe_rearm_timer(/*now=*/true);
    }
}

void partition_balancer_backend::tick() {
    ssx::background
      = ssx::spawn_with_gate_then(
          _gate,
          [this] {
              return do_tick().finally([this] {
                  _tick_in_progress = {};
                  maybe_rearm_timer(
                    _cur_term && _cur_term->_force_health_report_refresh);
              });
          })
          .handle_exception_type([](balancer_tick_aborted_exception& e) {
              vlog(clusterlog.info, "tick aborted, reason: {}", e.what());
          })
          .handle_exception_type(
            [this](topic_table::concurrent_modification_error& e) {
                vlog(
                  clusterlog.debug,
                  "concurrent modification of topics table: {}, rescheduling "
                  "tick",
                  e.what());
                maybe_rearm_timer(true);
            })
          .handle_exception_type([this](iterator_stability_violation& e) {
              vlog(
                clusterlog.debug,
                "iterator_stability_violation: {}, rescheduling tick",
                e.what());
              maybe_rearm_timer(true);
          })
          .handle_exception([](const std::exception_ptr& e) {
              vlog(clusterlog.warn, "tick error: {}", e);
          });
}

ss::future<> partition_balancer_backend::stop() {
    vlog(clusterlog.info, "stopping...");
    _state.topics().unregister_lw_ntp_notification(_topic_table_updates);
    _state.members().unregister_members_updated_notification(_member_updates);
    _health_monitor.unregister_node_callback(_health_monitor_updates);
    _timer.cancel();
    _lock.broken();
    if (_tick_in_progress) {
        _tick_in_progress->request_abort_ex(
          balancer_tick_aborted_exception{"shutting down"});
    }
    return _gate.close();
}

ss::future<> partition_balancer_backend::do_tick() {
    if (!is_enabled()) {
        vlog(clusterlog.debug, "not leader, skipping tick");
        co_return;
    }

    auto units = co_await _lock.get_units();

    if (!_raft0->is_leader()) {
        vlog(clusterlog.debug, "lost leadership, exiting");
        co_return;
    }

    vlog(clusterlog.debug, "tick");

    if (!_cur_term || _raft0->term() != _cur_term->id) {
        _cur_term = per_term_state(_raft0->term());
    }

    co_await _controller_stm.wait(
      _raft0->committed_offset(),
      model::timeout_clock::now() + controller_stm_sync_timeout);

    if (_raft0->term() != _cur_term->id) {
        vlog(clusterlog.debug, "lost leadership, exiting");
        // TODO: add term checks to planner
        co_return;
    }

    _tick_in_progress = ss::abort_source{};

    const bool force_refresh_this_tick
      = _cur_term->_force_health_report_refresh;
    auto health_report = co_await _health_monitor.get_cluster_health(
      cluster_report_filter{},
      force_refresh(force_refresh_this_tick),
      model::timeout_clock::now() + controller_stm_sync_timeout);
    _cur_term->_force_health_report_refresh = false;

    if (!health_report) {
        vlog(
          clusterlog.info,
          "unable to get health report - {}",
          health_report.error().message());
        co_return;
    }

    if (_raft0->term() != _cur_term->id) {
        vlog(clusterlog.debug, "lost leadership, exiting");
        co_return;
    }

    double soft_max_disk_usage_ratio = _max_disk_usage_percent() / 100.0;
    double hard_max_disk_usage_ratio
      = (100 - _storage_space_alert_free_threshold_percent()) / 100.0;
    // claim node unresponsive it doesn't responded to at least 7
    // status requests by default 700ms
    const auto node_responsiveness_timeout = _node_status_interval() * 7;

    partition_balancer_planner planner(
      planner_config{
        .mode = _mode(),
        .soft_max_disk_usage_ratio = soft_max_disk_usage_ratio,
        .hard_max_disk_usage_ratio = hard_max_disk_usage_ratio,
        .max_concurrent_actions = _max_concurrent_actions(),
        .node_availability_timeout_sec = _availability_timeout(),
        .ondemand_rebalance_requested
        = _cur_term->_ondemand_rebalance_requested,
        .segment_fallocation_step = _segment_fallocation_step(),
        .min_partition_size_threshold = get_min_partition_size_threshold(),
        .node_responsiveness_timeout = node_responsiveness_timeout,
        .topic_aware = _topic_aware(),
      },
      _state,
      _partition_allocator);

    auto plan_data = co_await planner.plan_actions(
      health_report.value(), _tick_in_progress.value());

    _cur_term->last_tick_time = clock_t::now();
    _cur_term->last_violations = std::move(plan_data.violations);
    _cur_term->last_tick_decommission_realloc_failures = std::move(
      plan_data.decommission_realloc_failures);
    if (
      _state.topics().has_updates_in_progress()
      || plan_data.status == planner_status::actions_planned) {
        _cur_term->last_status = partition_balancer_status::in_progress;
    } else if (
      plan_data.status == planner_status::missing_sizes
      && !force_refresh_this_tick) {
        // If we are missing partition sizes and haven't yet force-refreshed the
        // health monitor, do it immediately on the next tick.
        _cur_term->_force_health_report_refresh = true;
        _cur_term->last_status = partition_balancer_status::in_progress;
    } else if (plan_data.status == planner_status::waiting_for_reports) {
        _cur_term->last_status = partition_balancer_status::starting;
    } else if (plan_data.failed_actions_count > 0) {
        _cur_term->last_status = partition_balancer_status::stalled;
    } else {
        _cur_term->last_status = partition_balancer_status::ready;
    }

    if (_cur_term->last_status != partition_balancer_status::ready) {
        vlog(
          clusterlog.info,
          "last status: {}; "
          "violations: unavailable nodes: {}, full nodes: {}; "
          "nodes to rebalance count: {}; on demand rebalance requested: {}; "
          "updates in progress: {}; "
          "action counts: reassignments: {}, cancellations: {}, failed: {}; "
          "counts rebalancing finished: {}, force refresh health report: {}",
          _cur_term->last_status,
          _cur_term->last_violations.unavailable_nodes.size(),
          _cur_term->last_violations.full_nodes.size(),
          _state.nodes_to_rebalance().size(),
          _cur_term->_ondemand_rebalance_requested,
          _state.topics().updates_in_progress().size(),
          plan_data.reassignments.size(),
          plan_data.cancellations.size(),
          plan_data.failed_actions_count,
          plan_data.counts_rebalancing_finished,
          _cur_term->_force_health_report_refresh);
    }

    auto moves_before = _state.topics().updates_in_progress().size();

    if (moves_before == 0 && plan_data.counts_rebalancing_finished) {
        _cur_term->_ondemand_rebalance_requested = false;

        // make a copy in case the collection is modified concurrently.
        auto nodes_to_finish = _state.nodes_to_rebalance();
        co_await ss::max_concurrent_for_each(
          nodes_to_finish, 32, [this](model::node_id node) {
              _tick_in_progress->check();

              return _members_frontend
                .finish_node_reallocations(node, _cur_term->id)
                .then([node](auto errc) {
                    if (errc) {
                        vlog(
                          clusterlog.warn,
                          "submitting finish_reallocations for node {} failed, "
                          "error: {}",
                          node,
                          errc.message());
                    }
                });
          });
    }

    co_await ss::max_concurrent_for_each(
      plan_data.cancellations, 32, [this](model::ntp& ntp) {
          _tick_in_progress->check();
          auto f = _topics_frontend.cancel_moving_partition_replicas(
            ntp,
            model::timeout_clock::now() + add_move_cmd_timeout,
            _cur_term->id);

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
      plan_data.reassignments, 32, [this](ntp_reassignment& reassignment) {
          _tick_in_progress->check();
          auto f = ss::make_ready_future<std::error_code>();
          switch (reassignment.type) {
          case regular:
              f = _topics_frontend.move_partition_replicas(
                reassignment.ntp,
                reassignment.allocated.replicas(),
                reassignment.reconfiguration_policy,
                model::timeout_clock::now() + add_move_cmd_timeout,
                _cur_term->id);
              break;
          case force:
              f = _topics_frontend.force_update_partition_replicas(
                reassignment.ntp,
                reassignment.allocated.replicas(),
                model::timeout_clock::now() + add_move_cmd_timeout);
              break;
          default:
              vassert(
                false,
                "unexpected ntp reassignment type: {}",
                reassignment.type);
              break;
          }
          return std::move(f).then(
            [reassignment = std::move(reassignment)](auto errc) {
                if (errc) {
                    vlog(
                      clusterlog.warn,
                      "submitting {} reassignment failed, error: {}",
                      reassignment.ntp,
                      errc.message());
                }
            });
      });

    _cur_term->last_tick_in_progress_updates = moves_before
                                               + plan_data.cancellations.size()
                                               + plan_data.reassignments.size();
}

partition_balancer_overview_reply partition_balancer_backend::overview() const {
    vassert(ss::this_shard_id() == shard, "called on a wrong shard");

    partition_balancer_overview_reply ret;

    if (config::node().recovery_mode_enabled()) {
        ret.status = partition_balancer_status::off;
        ret.error = errc::feature_disabled;
        return ret;
    }

    if (!is_leader()) {
        ret.error = errc::not_leader;
        return ret;
    }

    if (!_cur_term || _raft0->term() != _cur_term->id) {
        // we haven't done a single tick in this term yet, return empty response
        ret.status = partition_balancer_status::starting;
        ret.partitions_pending_force_recovery_count = -1;
        ret.error = errc::success;
        return ret;
    }

    ret.status = _cur_term->last_status;
    ret.violations = _cur_term->last_violations;
    ret.decommission_realloc_failures
      = _cur_term->last_tick_decommission_realloc_failures;
    ret.partitions_pending_force_recovery_count
      = _state.topics().partitions_to_force_recover().size();
    if (ret.partitions_pending_force_recovery_count > 0) {
        constexpr size_t max_partitions_to_include = 10;
        auto sample_size = std::min(
          ret.partitions_pending_force_recovery_count,
          max_partitions_to_include);
        ret.partitions_pending_force_recovery_sample.reserve(sample_size);
        for (const auto& [ntp, _] :
             _state.topics().partitions_to_force_recover()) {
            if (
              ret.partitions_pending_force_recovery_sample.size()
              > max_partitions_to_include) {
                break;
            }
            ret.partitions_pending_force_recovery_sample.push_back(ntp);
        }
    }

    auto now = clock_t::now();
    auto time_since_last_tick = now - _cur_term->last_tick_time;
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
