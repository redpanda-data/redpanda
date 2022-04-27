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
#include "cluster/controller_api.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_service.h"
#include "cluster/data_policy_frontend.h"
#include "cluster/feature_backend.h"
#include "cluster/feature_manager.h"
#include "cluster/feature_table.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/members_backend.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/metrics_reporter.h"
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
#include "security/acl.h"

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
  ss::sharded<v8_engine::data_policy_table>& data_policy_table)
  : _config_preload(std::move(config_preload))
  , _connections(ccache)
  , _partition_manager(pm)
  , _shard_table(st)
  , _storage(storage)
  , _storage_node(storage_node)
  , _tp_updates_dispatcher(_partition_allocator, _tp_state, _partition_leaders)
  , _security_manager(_credentials, _authorizer)
  , _data_policy_manager(data_policy_table)
  , _raft_manager(raft_manager) {}

ss::future<> controller::wire_up() {
    return _as.start()
      .then([this] { return _members_table.start(); })
      .then([this] { return _feature_table.start(); })
      .then([this] {
          return _partition_allocator.start_single(
            std::ref(_members_table),
            config::shard_local_cfg().topic_memory_per_partition.bind(),
            config::shard_local_cfg().topic_fds_per_partition.bind(),
            config::shard_local_cfg().segment_fallocation_step.bind(),
            config::shard_local_cfg().enable_rack_awareness.bind());
      })
      .then([this] { return _credentials.start(); })
      .then([this] {
          return _authorizer.start(
            []() { return config::shard_local_cfg().superusers.bind(); });
      })
      .then([this] { return _tp_state.start(); });
}

ss::future<> controller::start() {
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
            std::ref(_as));
      })
      .then([this] {
          return _config_manager.start(
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
            std::ref(_as),
            std::ref(_authorizer));
      })
      .then([this] {
          return _data_policy_frontend.start(std::ref(_stm), std::ref(_as));
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
            std::ref(_as));
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
            _raft_manager.local().raft_client(),
            std::ref(_shard_table),
            std::ref(_partition_manager),
            std::ref(_raft_manager),
            std::ref(_as),
            config::shard_local_cfg().enable_leader_balancer.bind(),
            config::shard_local_cfg().leader_balancer_idle_timeout.bind(),
            config::shard_local_cfg().leader_balancer_mute_timeout.bind(),
            config::shard_local_cfg().leader_balancer_node_mute_timeout.bind(),
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
            std::ref(_drain_manager),
            std::ref(_feature_table),
            config::shard_local_cfg()
              .storage_space_alert_free_threshold_bytes.bind(),
            config::shard_local_cfg()
              .storage_space_alert_free_threshold_percent.bind());
      })
      .then([this] { return _hm_frontend.start(std::ref(_hm_backend)); })
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

    return f.then([this] {
        auto stop_leader_balancer = _leader_balancer ? _leader_balancer->stop()
                                                     : ss::now();
        return stop_leader_balancer
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
          .then([this] { return _feature_table.stop(); })
          .then([this] { return _members_table.stop(); })
          .then([this] { return _as.stop(); });
    });
}

} // namespace cluster
