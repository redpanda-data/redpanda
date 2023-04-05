// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"

#include "cluster/bootstrap_backend.h"
#include "cluster/cluster_discovery.h"
#include "cluster/cluster_utils.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_api.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_log_limiter.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/feature_backend.h"
#include "cluster/feature_manager.h"
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
#include "cluster/partition_balancer_state.h"
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
#include "features/feature_table.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/fwd.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "security/ephemeral_credential_store.h"
#include "ssx/future-util.h"

#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

namespace cluster {

controller::controller(
  config_manager::preload_result&& config_preload,
  ss::sharded<rpc::connection_cache>& ccache,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  ss::sharded<storage::api>& storage,
  ss::sharded<node::local_monitor>& local_monitor,
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cloud_storage::remote>& cloud_storage_api)
  : _config_preload(std::move(config_preload))
  , _connections(ccache)
  , _partition_manager(pm)
  , _shard_table(st)
  , _storage(storage)
  , _local_monitor(local_monitor)
  , _tp_updates_dispatcher(
      _partition_allocator,
      _tp_state,
      _partition_leaders,
      _partition_balancer_state)
  , _security_manager(_credentials, _authorizer)
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
      .then([this] { return _ephemeral_credentials.start(); })
      .then([this] {
          return _authorizer.start(
            []() { return config::shard_local_cfg().superusers.bind(); });
      })
      .then([this] { return _tp_state.start(); })
      .then([this] {
          return _partition_balancer_state.start_single(
            std::ref(_tp_state),
            std::ref(_members_table),
            std::ref(_partition_allocator));
      })
      .then([this] { _probe.start(); });
}

ss::future<>
controller::start(cluster_discovery& discovery, ss::abort_source& shard0_as) {
    auto initial_raft0_brokers = discovery.founding_brokers();
    std::vector<model::node_id> seed_nodes;
    seed_nodes.reserve(initial_raft0_brokers.size());
    std::transform(
      initial_raft0_brokers.cbegin(),
      initial_raft0_brokers.cend(),
      std::back_inserter(seed_nodes),
      [](const model::broker& b) { return b.id(); });

    return validate_configuration_invariants()
      .then([this, initial_raft0_brokers]() mutable {
          return create_raft0(
            _partition_manager,
            _shard_table,
            config::node().data_directory().as_sstring(),
            std::move(initial_raft0_brokers));
      })
      .then([this](consensus_ptr c) { _raft0 = c; })
      .then([this] { return _partition_leaders.start(std::ref(_tp_state)); })
      .then(
        [this] { return _drain_manager.start(std::ref(_partition_manager)); })
      .then([this] {
          return _members_manager.start_single(
            _raft0,
            std::ref(_stm),
            std::ref(_feature_table),
            std::ref(_members_table),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_storage),
            std::ref(_drain_manager),
            std::ref(_as));
      })
      .then([this] {
          return _feature_backend.start_single(
            std::ref(_feature_table), std::ref(_storage));
      })
      .then([this] {
          return _bootstrap_backend.start_single(
            std::ref(_credentials),
            std::ref(_storage),
            std::ref(_members_manager),
            std::ref(_feature_table),
            std::ref(_feature_backend));
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
            std::ref(_members_table),
            std::ref(_as));
      })
      .then([this] {
          limiter_configuration limiter_conf{
            config::shard_local_cfg()
              .enable_controller_log_rate_limiting.bind(),
            config::shard_local_cfg().rps_limit_topic_operations.bind(),
            config::shard_local_cfg()
              .controller_log_accummulation_rps_capacity_topic_operations
              .bind(),
            config::shard_local_cfg()
              .rps_limit_acls_and_users_operations.bind(),
            config::shard_local_cfg()
              .controller_log_accummulation_rps_capacity_acls_and_users_operations
              .bind(),
            config::shard_local_cfg()
              .rps_limit_node_management_operations.bind(),
            config::shard_local_cfg()
              .controller_log_accummulation_rps_capacity_node_management_operations
              .bind(),
            config::shard_local_cfg().rps_limit_move_operations.bind(),
            config::shard_local_cfg()
              .controller_log_accummulation_rps_capacity_move_operations.bind(),
            config::shard_local_cfg().rps_limit_configuration_operations.bind(),
            config::shard_local_cfg()
              .controller_log_accummulation_rps_capacity_configuration_operations
              .bind(),
          };
          return _stm.start_single(
            std::move(limiter_conf),
            std::ref(_feature_table),
            config::shard_local_cfg().controller_snapshot_max_age_sec.bind(),
            std::ref(clusterlog),
            _raft0.get(),
            raft::persistent_last_applied::yes,
            absl::flat_hash_set<model::record_batch_type>{
              model::record_batch_type::checkpoint,
              model::record_batch_type::raft_configuration,
              model::record_batch_type::data_policy_management_cmd},
            std::ref(_tp_updates_dispatcher),
            std::ref(_security_manager),
            std::ref(_members_manager),
            std::ref(_config_manager),
            std::ref(_feature_backend),
            std::ref(_bootstrap_backend));
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
          return _ephemeral_credential_frontend.start(
            self(),
            std::ref(_credentials),
            std::ref(_ephemeral_credentials),
            std::ref(_feature_table),
            std::ref(_connections));
      })
      .then([this] {
          return _tp_frontend.start(
            _raft0->self().id(),
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_partition_leaders),
            std::ref(_tp_state),
            std::ref(_hm_frontend),
            std::ref(_as),
            std::ref(_cloud_storage_api),
            std::ref(_feature_table),
            std::ref(_members_table),
            std::ref(_partition_manager),
            std::ref(_shard_table),
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .storage_space_alert_free_threshold_percent.bind();
            }));
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
            std::ref(_feature_table),
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
            std::ref(_feature_table),
            std::ref(_as));
      })
      .then(
        [this] { return _drain_manager.invoke_on_all(&drain_manager::start); })
      .then([this, initial_raft0_brokers]() mutable {
          return _members_manager.invoke_on(
            members_manager::shard,
            [initial_raft0_brokers = std::move(initial_raft0_brokers)](
              members_manager& manager) mutable {
                return manager.start(std::move(initial_raft0_brokers));
            });
      })
      .then([this] {
          /**
           * Controller state machine MUST be started after all entities that
           * receives `apply_update` notifications
           */
          return _stm.invoke_on(controller_stm_shard, &controller_stm::start);
      })
      .then([this, &as = shard0_as] {
          auto disk_dirty_offset = _raft0->log().offsets().dirty_offset;

          return _stm
            .invoke_on(
              controller_stm_shard,
              [disk_dirty_offset, &as = as](controller_stm& stm) {
                  // we do not have to use timeout in here as all the batches to
                  // apply have to be accesssible
                  auto last_applied = stm.bootstrap_last_applied();

                  // Consistency check: on a bug-free system, the last_applied
                  // value in the kvstore always points to data on disk.
                  // However, if there is a bug, or someone has removed some log
                  // segments out of band, we will hang trying to read up to
                  // last_applied. Mitigate this by clamping it to the top of
                  // the log on disk.
                  if (last_applied > disk_dirty_offset) {
                      vlog(
                        clusterlog.error,
                        "Inconsistency detected between KVStore last_applied "
                        "({}) and actual log size ({}).  If the storage "
                        "directory was not modified intentionally, this is a "
                        "bug.",
                        last_applied,
                        disk_dirty_offset);

                      // Try waiting for replay to reach the disk_dirty_offset,
                      // ignore last_applied.
                      return stm
                        .wait(disk_dirty_offset, model::time_from_now(5s))
                        .handle_exception_type([](const ss::timed_out_error&) {
                            // Ignore timeout: it just means the controller
                            // log replay is done without hitting the disk
                            // log hwm (truncation happened), or that we were
                            // slow and controller replay will continue in
                            // the background while the rest of redpanda
                            // starts up.
                        });
                  }

                  vlog(
                    clusterlog.info,
                    "Controller log replay starting (to offset {})",
                    last_applied);

                  if (last_applied == model::offset{}) {
                      return ss::now();
                  } else {
                      // The abort source we use here is specific to our startup
                      // phase, where we can't yet safely use our member abort
                      // source.
                      return stm.wait(last_applied, model::no_timeout, as);
                  }
              })
            .then(
              [] { vlog(clusterlog.info, "Controller log replay complete."); });
      })
      .then([this, &discovery] { return cluster_creation_hook(discovery); })
      .then(
        [this] { return _backend.invoke_on_all(&controller_backend::start); })
      .then([this] {
          return _api.start(
            _raft0->self().id(),
            std::ref(_backend),
            std::ref(_tp_state),
            std::ref(_shard_table),
            std::ref(_connections),
            std::ref(_hm_frontend),
            std::ref(_members_table),
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
            std::ref(_connections),
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
            std::ref(_local_monitor),
            std::ref(_drain_manager),
            std::ref(_feature_table),
            std::ref(_partition_leaders),
            std::ref(_tp_state));
      })
      .then([this] { return _hm_frontend.start(std::ref(_hm_backend)); })
      .then([this] {
          return _hm_frontend.invoke_on_all(&health_monitor_frontend::start);
      })
      .then([this, seed_nodes = std::move(seed_nodes)]() mutable {
          return _feature_manager.invoke_on(
            feature_manager::backend_shard,
            &feature_manager::start,
            std::move(seed_nodes));
      })
      .then([this] {
          return _metrics_reporter.start_single(
            _raft0,
            std::ref(_stm),
            std::ref(_members_table),
            std::ref(_tp_state),
            std::ref(_hm_frontend),
            std::ref(_config_frontend),
            std::ref(_feature_table),
            std::ref(_as));
      })
      .then([this] {
          return _metrics_reporter.invoke_on(0, &metrics_reporter::start);
      })
      .then([this] {
          return _partition_balancer.start_single(
            _raft0,
            std::ref(_stm),
            std::ref(_partition_balancer_state),
            std::ref(_hm_frontend),
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
              .partition_autobalancing_movement_batch_size_bytes.bind(),
            config::shard_local_cfg().segment_fallocation_step.bind());
      })
      .then([this] {
          return _partition_balancer.invoke_on(
            partition_balancer_backend::shard,
            &partition_balancer_backend::start);
      });
}

ss::future<> controller::set_ready() {
    if (_is_ready) {
        return ss::now();
    }
    _is_ready = true;
    return _raft_manager.invoke_on_all(&raft::group_manager::set_ready);
}

ss::future<> controller::shutdown_input() {
    vlog(clusterlog.debug, "Shutting down controller inputs");
    if (_raft0) {
        _raft0->shutdown_input();
    }
    co_await _as.invoke_on_all(&ss::abort_source::request_abort);
    vlog(clusterlog.debug, "Shut down controller inputs");
}

ss::future<> controller::stop() {
    auto f = ss::now();

    if (!_as.local().abort_requested()) {
        f = shutdown_input();
    }

    _probe.stop();
    return f.then([this] {
        auto stop_leader_balancer = _leader_balancer ? _leader_balancer->stop()
                                                     : ss::now();
        return stop_leader_balancer
          .then([this] {
              return ss::smp::submit_to(controller_stm_shard, [&stm = _stm] {
                  if (stm.local_is_initialized()) {
                      return stm.local().shutdown();
                  }
                  return ss::now();
              });
          })
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
          .then([this] { return _ephemeral_credential_frontend.stop(); })
          .then([this] { return _security_frontend.stop(); })
          .then([this] { return _members_frontend.stop(); })
          .then([this] { return _config_frontend.stop(); })
          .then([this] { return _feature_backend.stop(); })
          .then([this] { return _bootstrap_backend.stop(); })
          .then([this] { return _authorizer.stop(); })
          .then([this] { return _ephemeral_credentials.stop(); })
          .then([this] { return _credentials.stop(); })
          .then([this] { return _tp_state.stop(); })
          .then([this] { return _members_manager.stop(); })
          .then([this] { return _stm.stop(); })
          .then([this] { return _drain_manager.stop(); })
          .then([this] { return _partition_balancer_state.stop(); })
          .then([this] { return _partition_allocator.stop(); })
          .then([this] { return _partition_leaders.stop(); })
          .then([this] { return _members_table.stop(); })
          .then([this] { return _gate.close(); })
          .then([this] { return _as.stop(); });
    });
}

ss::future<>
controller::create_cluster(const bootstrap_cluster_cmd_data cmd_data) {
    vassert(
      ss::this_shard_id() == controller_stm_shard,
      "Cluster can only be created from controller_stm_shard");

    for (simple_time_jitter<model::timeout_clock> retry_jitter(1s);;) {
        // A new cluster is created by the leader of raft0 consisting of seed
        // servers, once elected, or by the root.
        // * In seed driven bootstrap mode, non-leaders will wait for
        // cluster_uuid to appear, or for leadership change, while the leader
        // will proceed with cluster creation once elected.
        // * In root driven bootstrap mode, root is the only cluster member and
        // joiners never make it into create_cluster(), so checking for
        // cluster_uuid() never returns true and the wait for leadership is
        // really just a wait for the consensus object to finish writing its
        // last_voted_for from its self-vote.
        while (!_storage.local().get_cluster_uuid() && !_raft0->is_leader()) {
            co_await ss::sleep_abortable(100ms, _as.local());
        }
        if (_storage.local().get_cluster_uuid()) {
            // Cluster has been created by other seed node and replicated here
            vlog(
              clusterlog.info,
              "Cluster UUID is {}",
              *_storage.local().get_cluster_uuid());
            co_return;
        }

        vlog(clusterlog.info, "Creating cluster UUID {}", cmd_data.uuid);
        model::record_batch b = serde_serialize_cmd(
          bootstrap_cluster_cmd{0, cmd_data});
        // cluster::replicate_and_wait() cannot be used here because it checks
        // for serde_raft_0 feature to be enabled. Before a cluster is here,
        // no feature is active, however the bootstrap_cluster_cmd is serde-only
        const std::error_code errc = co_await _stm.local().replicate_and_wait(
          std::move(b),
          model::timeout_clock::now() + 30s,
          _as.local(),
          std::nullopt);
        if (errc == errc::success) {
            vlog(clusterlog.info, "Cluster UUID created {}", cmd_data.uuid);
            co_return;
        }
        vlog(
          clusterlog.warn,
          "Failed to replicate and wait the cluster bootstrap cmd. {} ({})",
          errc.message(),
          errc);
        if (errc == errc::cluster_already_exists) {
            co_return;
        }

        co_await ss::sleep_abortable(retry_jitter.next_duration(), _as.local());
        vlog(
          clusterlog.trace,
          "Will retry to create cluster UUID {}",
          cmd_data.uuid);
    }
}

/**
 * This function provides for writing the controller log immediately
 * after it has been created, before anything else has been written
 * to it, and before we have started communicating with peers.
 */
ss::future<> controller::cluster_creation_hook(cluster_discovery& discovery) {
    if (co_await discovery.is_cluster_founder()) {
        // Full cluster bootstrap
        bootstrap_cluster_cmd_data cmd_data;
        cmd_data.uuid = model::cluster_uuid(uuid_t::create());
        cmd_data.bootstrap_user_cred
          = security_frontend::get_bootstrap_user_creds_from_env();
        cmd_data.node_ids_by_uuid = std::move(discovery.get_node_ids_by_uuid());
        cmd_data.founding_version
          = features::feature_table::get_latest_logical_version();
        cmd_data.initial_nodes = discovery.founding_brokers();
        co_return co_await create_cluster(std::move(cmd_data));
    }

    if (_storage.local().get_cluster_uuid()) {
        vlog(
          clusterlog.info,
          "Member of cluster UUID {}",
          *_storage.local().get_cluster_uuid());
        co_return;
    }

    // A missing cluster UUID on a non-founder may indicate this node was
    // upgraded from a version before 22.3. Replicate just a cluster UUID so we
    // can advertise that a cluster has already been bootstrapped to nodes
    // trying to discover existing clusters.
    ssx::background
      = _feature_table.local()
          .await_feature(
            features::feature::seeds_driven_bootstrap_capable, _as.local())
          .then([this] {
              bootstrap_cluster_cmd_data cmd_data;
              cmd_data.uuid = model::cluster_uuid(uuid_t::create());
              return create_cluster(std::move(cmd_data));
          })
          .handle_exception([](const std::exception_ptr e) {
              vlog(clusterlog.warn, "Error creating cluster UUID. {}", e);
          });
}

/**
 * Helper for subsystems that create internal topics, to discover
 * how many replicas they should use.
 */
int16_t controller::internal_topic_replication() const {
    auto replication_factor
      = (int16_t)config::shard_local_cfg().internal_topic_replication_factor();
    if (replication_factor > (int16_t)_members_table.local().node_count()) {
        // Fall back to r=1 if we do not have sufficient nodes
        return 1;
    } else {
        // Respect `internal_topic_replication_factor` if enough
        // nodes were available.
        return replication_factor;
    }
}

/**
 * Validate that:
 * - node_id never changes
 * - core count only increases, never decreases.
 *
 * Core count decreases are forbidden because our partition placement
 * code does not know how to re-assign partitions away from non-existent
 * cores if some cores are removed.  This may be improved in future, at
 * which time we may remove this restriction on core count decreases.
 *
 * These checks are applied early during startup based on a locally
 * stored record from previous startup, to prevent a misconfigured node
 * from startup up far enough to disrupt the rest of the cluster.
 * @return
 */
ss::future<> controller::validate_configuration_invariants() {
    static const bytes invariants_key("configuration_invariants");
    auto invariants_buf = _storage.local().kvs().get(
      storage::kvstore::key_space::controller, invariants_key);
    vassert(
      config::node().node_id(),
      "Node id must be set before checking configuration invariants");

    auto current = configuration_invariants(
      *config::node().node_id(), ss::smp::count);

    if (!invariants_buf) {
        // store configuration invariants
        return _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key,
          reflection::to_iobuf(std::move(current)));
    }
    auto invariants = reflection::from_iobuf<configuration_invariants>(
      std::move(*invariants_buf));
    // node id changed

    if (invariants.node_id != current.node_id) {
        vlog(
          clusterlog.error,
          "Detected node id change from {} to {}. Node id change is not "
          "supported",
          invariants.node_id,
          current.node_id);
        return ss::make_exception_future(
          configuration_invariants_changed(invariants, current));
    }
    if (invariants.core_count > current.core_count) {
        vlog(
          clusterlog.error,
          "Detected change in number of cores dedicated to run redpanda."
          "Decreasing redpanda core count is not allowed. Expected core "
          "count "
          "{}, currently have {} cores.",
          invariants.core_count,
          ss::smp::count);
        return ss::make_exception_future(
          configuration_invariants_changed(invariants, current));
    } else if (invariants.core_count != current.core_count) {
        // Update the persistent invariants to reflect increased core
        // count -- this tracks the high water mark of core count, to
        // reject subsequent decreases.
        return _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key,
          reflection::to_iobuf(std::move(current)));
    }
    return ss::now();
}

} // namespace cluster
