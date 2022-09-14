// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"

#include "cluster/cluster_utils.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_api.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_service.h"
#include "cluster/data_policy_frontend.h"
#include "cluster/feature_backend.h"
#include "cluster/feature_manager.h"
#include "cluster/feature_table.h"
#include "cluster/fwd.h"
#include "cluster/health_manager.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/members_backend.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/metrics_reporter.h"
#include "cluster/partition_balancer_backend.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/raft0_utils.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/security_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/fwd.h"
#include "raft/raft_feature_table.h"
#include "security/acl.h"
#include "ssx/future-util.h"

#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

namespace cluster {

controller::controller(
  config_manager::preload_result&& config_preload,
  ss::sharded<rpc::connection_cache>& ccache,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  ss::sharded<storage::api>& storage,
  ss::sharded<storage::node_api>& storage_node,
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<v8_engine::data_policy_table>& data_policy_table,
  ss::sharded<feature_table>& feature_table,
  ss::sharded<cloud_storage::remote>& cloud_storage_api)
  : _config_preload(std::move(config_preload))
  , _connections(ccache)
  , _partition_manager(pm)
  , _shard_table(st)
  , _storage(storage)
  , _storage_node(storage_node)
  , _tp_updates_dispatcher(_partition_allocator, _tp_state, _partition_leaders)
  , _security_manager(_credentials, _authorizer)
  , _data_policy_manager(data_policy_table)
  , _raft_manager(raft_manager)
  , _feature_table(feature_table)
  , _cloud_storage_api(cloud_storage_api)
  , _probe(*this) {}

ss::future<> controller::wire_up() {
    return _as.start()
      .then([this] { return _members_table.start(); })
      .then([this] {
          return _partition_allocator.start_single(
            std::ref(_members_table),
            config::shard_local_cfg().topic_memory_per_partition.bind(),
            config::shard_local_cfg().topic_fds_per_partition.bind(),
            config::shard_local_cfg().topic_partitions_per_shard.bind(),
            config::shard_local_cfg().topic_partitions_reserve_shard0.bind(),
            config::shard_local_cfg().enable_rack_awareness.bind());
      })
      .then([this] { return _credentials.start(); })
      .then([this] {
          return _authorizer.start(
            []() { return config::shard_local_cfg().superusers.bind(); });
      })
      .then([this] { return _tp_state.start(std::ref(_partition_manager)); })
      .then([this] { _probe.start(); });
}

ss::future<> controller::start() {
    /**
     * wire up raft features with feature table
     */
    ssx::spawn_with_gate(_gate, [this] {
        return _raft_manager.invoke_on_all([this](raft::group_manager& mgr) {
            return _feature_table.local()
              .await_feature(feature::raft_improved_configuration, _as.local())
              .then([&mgr] {
                  mgr.set_feature_active(
                    raft::raft_feature::improved_config_change);
              });
        });
    });

    std::vector<model::broker> initial_raft0_brokers;
    if (config::node().seed_servers().empty()) {
        initial_raft0_brokers.push_back(
          cluster::make_self_broker(config::node()));
    }
    return create_raft0(
             _partition_manager,
             _shard_table,
             config::node().data_directory().as_sstring(),
             std::move(initial_raft0_brokers))
      .then([this](consensus_ptr c) { _raft0 = c; })
      .then([this] { return _partition_leaders.start(std::ref(_tp_state)); })
      .then(
        [this] { return _drain_manager.start(std::ref(_partition_manager)); })
      .then([this] {
          return _members_manager.start_single(
            _raft0,
            std::ref(_members_table),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_storage),
            std::ref(_drain_manager),
            std::ref(_as));
      })
      .then([this] {
          // validate configuration invariants to exit early
          return _members_manager.local().validate_configuration_invariants();
      })
      .then([this] {
          return _feature_backend.start_single(std::ref(_feature_table));
      })
      .then([this] {
          return _config_frontend.start(
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_leaders),
            std::ref(_feature_table),
            std::ref(_as));
      })
      .then([this] {
          return _config_manager.start_single(
            std::ref(_config_preload),
            std::ref(_config_frontend),
            std::ref(_connections),
            std::ref(_partition_leaders),
            std::ref(_feature_table),
            std::ref(_as));
      })
      .then([this] {
          return _stm.start_single(
            std::ref(clusterlog),
            _raft0.get(),
            raft::persistent_last_applied::yes,
            std::ref(_tp_updates_dispatcher),
            std::ref(_security_manager),
            std::ref(_members_manager),
            std::ref(_data_policy_manager),
            std::ref(_config_manager),
            std::ref(_feature_backend));
      })
      .then([this] {
          return _members_frontend.start(
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_leaders),
            std::ref(_feature_table),
            std::ref(_as));
      })
      .then([this] {
          return _security_frontend.start(
            _raft0->self().id(),
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_leaders),
            std::ref(_feature_table),
            std::ref(_as),
            std::ref(_authorizer));
      })
      .then([this] {
          return _data_policy_frontend.start(
            std::ref(_stm), std::ref(_feature_table), std::ref(_as));
      })
      .then([this] {
          return _tp_frontend.start(
            _raft0->self().id(),
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_partition_leaders),
            std::ref(_tp_state),
            std::ref(_data_policy_frontend),
            std::ref(_as),
            std::ref(_cloud_storage_api),
            std::ref(_feature_table));
      })
      .then([this] {
          return _members_backend.start_single(
            std::ref(_tp_frontend),
            std::ref(_tp_state),
            std::ref(_partition_allocator),
            std::ref(_members_table),
            std::ref(_api),
            std::ref(_members_manager),
            std::ref(_members_frontend),
            _raft0,
            std::ref(_as));
      })
      .then([this] {
          return _backend.start(
            std::ref(_tp_state),
            std::ref(_shard_table),
            std::ref(_partition_manager),
            std::ref(_members_table),
            std::ref(_partition_leaders),
            std::ref(_tp_frontend),
            std::ref(_storage),
            std::ref(_as));
      })
      .then(
        [this] { return _drain_manager.invoke_on_all(&drain_manager::start); })
      .then([this] {
          return _members_manager.invoke_on(
            members_manager::shard, &members_manager::start);
      })
      .then([this] {
          /**
           * Controller state machine MUST be started after all entities that
           * receives `apply_update` notifications
           */
          return _stm.invoke_on(controller_stm_shard, &controller_stm::start);
      })
      .then([this] {
          return _stm.invoke_on(controller_stm_shard, [](controller_stm& stm) {
              // we do not have to use timeout in here as all the batches to
              // apply have to be accesssible
              return stm.wait(stm.bootstrap_last_applied(), model::no_timeout);
          });
      })
      .then([this] { return cluster_creation_hook(); })
      .then(
        [this] { return _backend.invoke_on_all(&controller_backend::start); })
      .then([this] {
          return _api.start(
            _raft0->self().id(),
            std::ref(_backend),
            std::ref(_tp_state),
            std::ref(_shard_table),
            std::ref(_connections),
            std::ref(_as));
      })
      .then([this] {
          return _members_backend.invoke_on(
            members_manager::shard, &members_backend::start);
      })
      .then([this] {
          return _config_manager.invoke_on(
            config_manager::shard, &config_manager::start);
      })
      .then([this] {
          return _feature_manager.start_single(
            std::ref(_stm),
            std::ref(_as),
            std::ref(_members_table),
            std::ref(_raft_manager),
            std::ref(_hm_frontend),
            std::ref(_hm_backend),
            std::ref(_feature_table),
            std::ref(_connections),
            _raft0->group());
      })
      .then([this] {
          _leader_balancer = std::make_unique<leader_balancer>(
            _tp_state.local(),
            _partition_leaders.local(),
            _members_table.local(),
            _raft_manager.local().raft_client(),
            std::ref(_shard_table),
            std::ref(_partition_manager),
            std::ref(_as),
            config::shard_local_cfg().enable_leader_balancer.bind(),
            config::shard_local_cfg().leader_balancer_idle_timeout.bind(),
            config::shard_local_cfg().leader_balancer_mute_timeout.bind(),
            config::shard_local_cfg().leader_balancer_node_mute_timeout.bind(),
            config::shard_local_cfg()
              .leader_balancer_transfer_limit_per_shard.bind(),
            _raft0);
          return _leader_balancer->start();
      })
      .then([this] {
          return _health_manager.start_single(
            _raft0->self().id(),
            config::shard_local_cfg().internal_topic_replication_factor(),
            config::shard_local_cfg().health_manager_tick_interval(),
            std::ref(_tp_state),
            std::ref(_tp_frontend),
            std::ref(_partition_allocator),
            std::ref(_partition_leaders),
            std::ref(_members_table),
            std::ref(_as));
      })
      .then([this] {
          return _health_manager.invoke_on(
            health_manager::shard, &health_manager::start);
      })
      .then([this] {
          return _hm_backend.start_single(
            _raft0,
            std::ref(_members_table),
            std::ref(_connections),
            std::ref(_partition_manager),
            std::ref(_raft_manager),
            std::ref(_as),
            std::ref(_storage_node),
            std::ref(_storage),
            std::ref(_drain_manager),
            std::ref(_feature_table),
            config::shard_local_cfg()
              .storage_space_alert_free_threshold_bytes.bind(),
            config::shard_local_cfg()
              .storage_space_alert_free_threshold_percent.bind(),
            config::shard_local_cfg().storage_min_free_bytes.bind());
      })
      .then([this] { return _hm_frontend.start(std::ref(_hm_backend)); })
      .then([this] {
          return _hm_frontend.invoke_on_all(&health_monitor_frontend::start);
      })
      .then([this] {
          return _feature_manager.invoke_on(
            feature_manager::backend_shard, &feature_manager::start);
      })
      .then([this] {
          return _metrics_reporter.start_single(
            _raft0,
            std::ref(_members_table),
            std::ref(_tp_state),
            std::ref(_hm_frontend),
            std::ref(_config_frontend),
            std::ref(_as));
      })
      .then([this] {
          return _metrics_reporter.invoke_on(0, &metrics_reporter::start);
      })
      .then([this] {
          return _partition_balancer.start_single(
            _raft0,
            std::ref(_stm),
            std::ref(_tp_state),
            std::ref(_hm_frontend),
            std::ref(_members_table),
            std::ref(_partition_allocator),
            std::ref(_tp_frontend),
            config::shard_local_cfg().partition_autobalancing_mode.bind(),
            config::shard_local_cfg()
              .partition_autobalancing_node_availability_timeout_sec.bind(),
            config::shard_local_cfg()
              .partition_autobalancing_max_disk_usage_percent.bind(),
            config::shard_local_cfg()
              .storage_space_alert_free_threshold_percent.bind(),
            config::shard_local_cfg()
              .partition_autobalancing_tick_interval_ms.bind(),
            config::shard_local_cfg()
              .partition_autobalancing_movement_batch_size_bytes.bind());
      })
      .then([this] {
          return _partition_balancer.invoke_on(
            partition_balancer_backend::shard,
            &partition_balancer_backend::start);
      });
}

ss::future<> controller::shutdown_input() {
    _raft0->shutdown_input();
    return _as.invoke_on_all(&ss::abort_source::request_abort);
}

ss::future<> controller::stop() {
    auto f = ss::now();
    if (unlikely(!_raft0)) {
        return f;
    }

    if (!_as.local().abort_requested()) {
        f = shutdown_input();
    }

    _probe.stop();
    return f.then([this] {
        auto stop_leader_balancer = _leader_balancer ? _leader_balancer->stop()
                                                     : ss::now();
        return stop_leader_balancer
          .then([this] { return _partition_balancer.stop(); })
          .then([this] { return _metrics_reporter.stop(); })
          .then([this] { return _feature_manager.stop(); })
          .then([this] { return _hm_frontend.stop(); })
          .then([this] { return _hm_backend.stop(); })
          .then([this] { return _health_manager.stop(); })
          .then([this] { return _members_backend.stop(); })
          .then([this] { return _config_manager.stop(); })
          .then([this] { return _api.stop(); })
          .then([this] { return _backend.stop(); })
          .then([this] { return _tp_frontend.stop(); })
          .then([this] { return _security_frontend.stop(); })
          .then([this] { return _data_policy_frontend.stop(); })
          .then([this] { return _members_frontend.stop(); })
          .then([this] { return _config_frontend.stop(); })
          .then([this] { return _feature_backend.stop(); })
          .then([this] { return _stm.stop(); })
          .then([this] { return _authorizer.stop(); })
          .then([this] { return _credentials.stop(); })
          .then([this] { return _tp_state.stop(); })
          .then([this] { return _members_manager.stop(); })
          .then([this] { return _drain_manager.stop(); })
          .then([this] { return _partition_allocator.stop(); })
          .then([this] { return _partition_leaders.stop(); })
          .then([this] { return _members_table.stop(); })
          .then([this] { return _gate.close(); })
          .then([this] { return _as.stop(); });
    });
}

/**
 * This function provides for writing the controller log immediately
 * after it has been created, before anything else has been written
 * to it, and before we have started communicating with peers.
 */
ss::future<> controller::cluster_creation_hook() {
    if (!config::node().seed_servers().empty()) {
        // We are not on the root node
        co_return;
    } else if (
      _raft0->last_visible_index() > model::offset{}
      || _raft0->config().brokers().size() > 1) {
        // The controller log has already been written to
        co_return;
    }

    // Internal RPC does not start until after controller startup
    // is complete (we are called during controller startup), so
    // it is guaranteed that if we were single node/empty controller
    // log at start of this function, we will still be in that state
    // here.  The wait for leadership is really just a wait for the
    // consensus object to finish writing its last_voted_for from
    // its self-vote.
    while (!_raft0->is_leader()) {
        co_await ss::sleep(100ms);
    }

    auto err
      = co_await _security_frontend.local().maybe_create_bootstrap_user();
    vassert(
      err == errc::success,
      "Controller write should always succeed in single replica state during "
      "creation");
}

/**
 * Helper for subsystems that create internal topics, to discover
 * how many replicas they should use.
 */
int16_t controller::internal_topic_replication() const {
    auto replication_factor
      = (int16_t)config::shard_local_cfg().internal_topic_replication_factor();
    if (
      replication_factor
      > (int16_t)_members_table.local().all_brokers().size()) {
        // Fall back to r=1 if we do not have sufficient nodes
        return 1;
    } else {
        // Respect `internal_topic_replication_factor` if enough
        // nodes were available.
        return replication_factor;
    }
}

} // namespace cluster
