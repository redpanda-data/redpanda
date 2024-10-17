// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"

#include "base/likely.h"
#include "cluster/bootstrap_backend.h"
#include "cluster/client_quota_backend.h"
#include "cluster/client_quota_frontend.h"
#include "cluster/client_quota_store.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/cluster_recovery_backend.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cloud_metadata/offsets_upload_rpc_types.h"
#include "cluster/cloud_metadata/producer_id_recovery_manager.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/cluster_discovery.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/cluster_utils.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_api.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_log_limiter.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/data_migration_backend.h"
#include "cluster/data_migration_frontend.h"
#include "cluster/data_migration_irpc_frontend.h"
#include "cluster/data_migration_table.h"
#include "cluster/data_migration_types.h"
#include "cluster/data_migration_worker.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/feature_backend.h"
#include "cluster/feature_manager.h"
#include "cluster/fwd.h"
#include "cluster/health_manager.h"
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/members_backend.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/metrics_reporter.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_balancer_backend.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/plugin_backend.h"
#include "cluster/plugin_frontend.h"
#include "cluster/raft0_utils.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/security_frontend.h"
#include "cluster/shard_balancer.h"
#include "cluster/shard_placement_table.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/fwd.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "security/ephemeral_credential_store.h"
#include "security/oidc_service.h"
#include "security/role_store.h"
#include "ssx/future-util.h"

#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

#include <chrono>
#include <optional>
namespace cluster {

bytes controller::invariants_key() {
    return bytes::from_string("configuration_invariants");
}

controller::controller(
  config_manager::preload_result&& config_preload,
  ss::sharded<rpc::connection_cache>& ccache,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  ss::sharded<storage::api>& storage,
  ss::sharded<node::local_monitor>& local_monitor,
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<cloud_storage::cache>& cloud_cache,
  ss::sharded<node_status_table>& node_status_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache)
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
  , _security_manager(_credentials, _authorizer, _roles)
  , _raft_manager(raft_manager)
  , _feature_table(feature_table)
  , _cloud_storage_api(cloud_storage_api)
  , _cloud_cache(cloud_cache)
  , _node_status_table(node_status_table)
  , _metadata_cache(metadata_cache)
  , _probe(*this) {}

// Explicit destructor in the .cc file just to avoid bloating the header with
// includes for destructors of all its members (e.g. the metadata uploader).
controller::~controller() = default;

std::optional<cloud_storage_clients::bucket_name>
controller::get_configured_bucket() {
    auto& bucket_property = cloud_storage::configuration::get_bucket_config();
    if (
      !bucket_property.is_overriden() || !bucket_property().has_value()
      || !_cloud_storage_api.local_is_initialized()) {
        return std::nullopt;
    }
    return cloud_storage_clients::bucket_name(bucket_property().value());
}

ss::future<> controller::wire_up() {
    return _as.start()
      .then([this] { return _members_table.start(); })
      .then([this] {
          return _partition_allocator.start_single(
            std::ref(_members_table),
            std::ref(_feature_table),
            config::shard_local_cfg().topic_memory_per_partition.bind(),
            config::shard_local_cfg().topic_fds_per_partition.bind(),
            config::shard_local_cfg().topic_partitions_per_shard.bind(),
            config::shard_local_cfg().topic_partitions_reserve_shard0.bind(),
            config::shard_local_cfg().kafka_nodelete_topics.bind(),
            config::shard_local_cfg().enable_rack_awareness.bind());
      })
      .then([this] { return _credentials.start(); })
      .then([this] { return _ephemeral_credentials.start(); })
      .then([this] { return _roles.start(); })
      .then([this] { return _data_migrated_resources.start(); })
      .then([this] {
          return _data_migration_table.start_on(
            data_migrations::data_migrations_shard,
            std::ref(_data_migrated_resources),
            std::ref(_tp_state),
            config::shard_local_cfg().cloud_storage_enabled()
              && config::shard_local_cfg()
                   .cloud_storage_disable_archiver_manager());
      })
      .then([this] {
          return _authorizer.start(
            ss::sharded_parameter(
              []() { return config::shard_local_cfg().superusers.bind(); }),
            ss::sharded_parameter([this] { return &_roles.local(); }));
      })
      .then([this] {
          return _oidc_service.start(
            ss::sharded_parameter(
              [] { return config::shard_local_cfg().sasl_mechanisms.bind(); }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().http_authentication.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().oidc_discovery_url.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().oidc_token_audience.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .oidc_clock_skew_tolerance.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().oidc_principal_mapping.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .oidc_keys_refresh_interval.bind();
            }));
      })
      .then([this] {
          return _tp_state.start(ss::sharded_parameter(
            [this] { return std::ref(_data_migrated_resources.local()); }));
      })
      .then([this] {
          return _partition_balancer_state.start_single(
            std::ref(_tp_state),
            std::ref(_members_table),
            std::ref(_partition_allocator),
            std::ref(_node_status_table));
      })
      .then([this] {
          return _shard_placement.start(
            ss::sharded_parameter([] { return ss::this_shard_id(); }),
            ss::sharded_parameter(
              [this] { return std::ref(_storage.local().kvs()); }));
      })
      .then([this] { _probe.start(); });
}

ss::future<> controller::start(
  cluster_discovery& discovery,
  ss::abort_source& shard0_as,
  ss::shared_ptr<cluster::cloud_metadata::offsets_upload_requestor>
    offsets_uploader,
  ss::shared_ptr<cluster::cloud_metadata::producer_id_recovery_manager>
    producer_id_recovery,
  ss::shared_ptr<cluster::cloud_metadata::offsets_recovery_requestor>
    offsets_recovery,
  std::chrono::milliseconds application_start_time) {
    auto initial_raft0_brokers = discovery.founding_brokers();
    std::vector<model::node_id> seed_nodes;
    seed_nodes.reserve(initial_raft0_brokers.size());
    std::transform(
      initial_raft0_brokers.cbegin(),
      initial_raft0_brokers.cend(),
      std::back_inserter(seed_nodes),
      [](const model::broker& b) { return b.id(); });

    auto conf_invariants = co_await validate_configuration_invariants();

    _raft0 = co_await create_raft0(
      _partition_manager,
      _shard_table,
      config::node().data_directory().as_sstring(),
      initial_raft0_brokers);

    co_await _partition_leaders.start(std::ref(_tp_state));
    co_await _drain_manager.start(std::ref(_partition_manager));
    co_await _members_manager.start_single(
      _raft0,
      std::ref(_stm),
      std::ref(_feature_table),
      std::ref(_members_table),
      std::ref(_connections),
      std::ref(_partition_allocator),
      std::ref(_storage),
      std::ref(_drain_manager),
      std::ref(_partition_balancer_state),
      std::ref(_as),
      application_start_time);
    co_await _feature_backend.start_single(
      std::ref(_feature_table), std::ref(_storage));
    co_await _bootstrap_backend.start_single(
      std::ref(_credentials),
      std::ref(_storage),
      std::ref(_members_manager),
      std::ref(_feature_table),
      std::ref(_feature_backend),
      std::ref(_recovery_table));

    co_await _recovery_table.start();
    co_await _recovery_manager.start_single(
      std::ref(_as),
      std::ref(_stm),
      std::ref(_feature_table),
      std::ref(_cloud_storage_api),
      std::ref(_recovery_table),
      std::ref(_storage),
      _raft0);

    co_await _plugin_table.start();
    co_await _plugin_backend.start_single(&_plugin_table);

    co_await _quota_store.start();
    co_await _quota_backend.start_single(std::ref(_quota_store));

    co_await _config_frontend.start(
      std::ref(_stm),
      std::ref(_connections),
      std::ref(_partition_leaders),
      std::ref(_feature_table),
      std::ref(_as));
    co_await _config_manager.start_single(
      std::ref(_config_preload),
      std::ref(_config_frontend),
      std::ref(_connections),
      std::ref(_partition_leaders),
      std::ref(_feature_table),
      std::ref(_members_table),
      std::ref(_as));

    co_await _data_migration_frontend.start(
      _raft0->self().id(),
      _cloud_storage_api.local_is_initialized(),
      std::ref(_data_migration_table),
      std::ref(_feature_table),
      std::ref(_stm),
      std::ref(_partition_leaders),
      std::ref(_connections),
      std::ref(_as));

    co_await _data_migration_worker.start(
      _raft0->self().id(),
      ss::sharded_parameter(
        [this] { return std::ref(_partition_leaders.local()); }),
      ss::sharded_parameter(
        [this] { return std::ref(_partition_manager.local()); }),
      ss::sharded_parameter([this] { return std::ref(_as.local()); }));
    {
        limiter_configuration limiter_conf{
          config::shard_local_cfg().enable_controller_log_rate_limiting.bind(),
          config::shard_local_cfg().rps_limit_topic_operations.bind(),
          config::shard_local_cfg()
            .controller_log_accummulation_rps_capacity_topic_operations.bind(),
          config::shard_local_cfg().rps_limit_acls_and_users_operations.bind(),
          config::shard_local_cfg()
            .controller_log_accummulation_rps_capacity_acls_and_users_operations
            .bind(),
          config::shard_local_cfg().rps_limit_node_management_operations.bind(),
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
        co_await _stm.start_single(
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
          std::ref(_bootstrap_backend),
          std::ref(_plugin_backend),
          std::ref(_recovery_manager),
          std::ref(_quota_backend),
          std::ref(_data_migration_table.local()));
    }

    co_await _members_frontend.start(
      std::ref(_stm),
      std::ref(_connections),
      std::ref(_partition_leaders),
      std::ref(_feature_table),
      std::ref(_as));

    co_await _security_frontend.start(
      _raft0->self().id(),
      this,
      std::ref(_stm),
      std::ref(_connections),
      std::ref(_partition_leaders),
      std::ref(_feature_table),
      std::ref(_as),
      std::ref(_authorizer));
    co_await _ephemeral_credential_frontend.start(
      self(),
      std::ref(_credentials),
      std::ref(_ephemeral_credentials),
      std::ref(_feature_table),
      std::ref(_connections));

    co_await _tp_frontend.start(
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
      std::ref(_shard_balancer),
      std::ref(_storage),
      ss::sharded_parameter(
        [this] { return std::ref(_data_migrated_resources.local()); }),
      ss::sharded_parameter([this] { return std::ref(_plugin_table.local()); }),
      ss::sharded_parameter(
        [this] { return std::ref(_metadata_cache.local()); }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .storage_space_alert_free_threshold_percent.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg().minimum_topic_replication.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .partition_autobalancing_topic_aware.bind();
      }));

    co_await _plugin_frontend.start(
      _raft0->self().id(),
      ss::sharded_parameter([this] { return &_partition_leaders.local(); }),
      ss::sharded_parameter([this] { return &_plugin_table.local(); }),
      ss::sharded_parameter([this] { return &_tp_state.local(); }),
      ss::sharded_parameter([this] {
          return _stm.local_is_initialized() ? &_stm.local() : nullptr;
      }),
      ss::sharded_parameter([this] { return &_connections.local(); }),
      ss::sharded_parameter([this] { return &_as.local(); }));

    co_await _quota_frontend.start(
      _raft0->self().id(),
      std::ref(_stm),
      std::ref(_connections),
      std::ref(_partition_leaders),
      std::ref(_as));

    co_await _members_backend.start_single(
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

    co_await _backend.start(
      std::ref(_tp_state),
      std::ref(_shard_placement),
      std::ref(_shard_table),
      std::ref(_partition_manager),
      std::ref(_members_table),
      std::ref(_partition_leaders),
      std::ref(_tp_frontend),
      std::ref(_storage),
      std::ref(_feature_table),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .controller_backend_housekeeping_interval_ms.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .initial_retention_local_target_bytes_default.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .initial_retention_local_target_ms_default.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .retention_local_target_bytes_default.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .retention_local_target_ms_default.bind();
      }),
      ss::sharded_parameter(
        [] { return config::shard_local_cfg().retention_local_strict.bind(); }),
      std::ref(_as));

    co_await _shard_balancer.start_single(
      std::ref(_shard_placement),
      std::ref(_feature_table),
      std::ref(_storage),
      std::ref(_tp_state),
      std::ref(_backend),
      config::shard_local_cfg().core_balancing_on_core_count_change.bind(),
      config::shard_local_cfg().core_balancing_continuous.bind(),
      config::shard_local_cfg().core_balancing_debounce_timeout.bind(),
      config::shard_local_cfg().topic_partitions_per_shard.bind(),
      config::shard_local_cfg().topic_partitions_reserve_shard0.bind());

    co_await _drain_manager.invoke_on_all(&drain_manager::start);

    co_await _members_manager.invoke_on(
      members_manager::shard,
      [initial_raft0_brokers](members_manager& manager) mutable {
          return manager.start(std::move(initial_raft0_brokers));
      });

    /**
     * Controller state machine MUST be started after all entities that
     * receives `apply_update` notifications
     */
    co_await _stm.invoke_on(controller_stm_shard, &controller_stm::start);

    auto disk_dirty_offset = _raft0->log()->offsets().dirty_offset;
    co_await _stm.invoke_on(
      controller_stm_shard,
      [disk_dirty_offset, &as = shard0_as](controller_stm& stm) {
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
              return stm.wait(disk_dirty_offset, model::time_from_now(5s))
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
      });
    vlog(clusterlog.info, "Controller log replay complete.");

    /// Once the controller log is replayed and topics are recovered
    /// print the RF minimum warning
    _tp_frontend.local().print_rf_warning_message();

    co_await cluster_creation_hook(discovery);

    // start shard_balancer before controller_backend so that it bootstraps
    // shard_placement_table and controller_backend can start with already
    // initialized table.
    co_await _shard_balancer.invoke_on(
      shard_balancer::shard_id,
      &shard_balancer::start,
      conf_invariants.core_count);

    if (conf_invariants.core_count > ss::smp::count) {
        // Successfully starting shard_balancer with reduced core count means
        // that all partition info from extra kvstores has been copied and we
        // can finally update the configuration invariants.
        auto new_invariants = configuration_invariants(
          *config::node().node_id(), ss::smp::count);
        co_await _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key(),
          reflection::to_iobuf(configuration_invariants{new_invariants}));
        conf_invariants = new_invariants;
        vlog(
          clusterlog.info,
          "successfully decreased core count, "
          "updated configuration invariants: {}",
          conf_invariants);
    }

    co_await _backend.invoke_on_all(&controller_backend::start);

    co_await _api.start(
      _raft0->self().id(),
      std::ref(_backend),
      std::ref(_tp_state),
      std::ref(_shard_table),
      std::ref(_connections),
      std::ref(_hm_frontend),
      std::ref(_members_table),
      std::ref(_partition_balancer),
      std::ref(_as));

    co_await _members_backend.invoke_on(
      members_manager::shard, &members_backend::start);
    co_await _config_manager.invoke_on(
      config_manager::shard, &config_manager::start);
    co_await _feature_manager.start_single(
      std::ref(_stm),
      std::ref(_as),
      std::ref(_members_table),
      std::ref(_raft_manager),
      std::ref(_hm_frontend),
      std::ref(_hm_backend),
      std::ref(_feature_table),
      std::ref(_connections),
      std::ref(_roles),
      std::ref(_tp_state),
      _raft0->group());

    co_await _health_manager.start_single(
      _raft0->self().id(),
      config::shard_local_cfg().internal_topic_replication_factor(),
      config::shard_local_cfg().health_manager_tick_interval(),
      config::shard_local_cfg().partition_autobalancing_concurrent_moves.bind(),
      std::ref(_tp_state),
      std::ref(_tp_frontend),
      std::ref(_partition_allocator),
      std::ref(_partition_leaders),
      std::ref(_members_table),
      std::ref(_as));
    co_await _health_manager.invoke_on(
      health_manager::shard, &health_manager::start);

    co_await _hm_backend.start_single(
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

    _leader_balancer = std::make_unique<leader_balancer>(
      _tp_state.local(),
      _partition_leaders.local(),
      _members_table.local(),
      _hm_backend.local(),
      _feature_table.local(),
      std::ref(_connections),
      std::ref(_shard_table),
      std::ref(_partition_manager),
      std::ref(_as),
      config::shard_local_cfg().enable_leader_balancer.bind(),
      config::shard_local_cfg().leader_balancer_idle_timeout.bind(),
      config::shard_local_cfg().leader_balancer_mute_timeout.bind(),
      config::shard_local_cfg().leader_balancer_node_mute_timeout.bind(),
      config::shard_local_cfg().leader_balancer_transfer_limit_per_shard.bind(),
      config::shard_local_cfg().enable_rack_awareness.bind(),
      config::shard_local_cfg().default_leaders_preference.bind(),
      config::shard_local_cfg().metadata_dissemination_interval_ms(),
      _raft0);
    co_await _leader_balancer->start();

    co_await _hm_frontend.start(
      std::ref(_hm_backend),
      std::ref(_node_status_table),
      ss::sharded_parameter(
        []() { return config::shard_local_cfg().alive_timeout_ms.bind(); }));
    co_await _hm_frontend.invoke_on_all(&health_monitor_frontend::start);

    co_await _oidc_service.invoke_on_all(&security::oidc::service::start);

    co_await _feature_manager.invoke_on(
      feature_manager::backend_shard,
      &feature_manager::start,
      std::vector{seed_nodes});

    co_await _metrics_reporter.start_single(
      _raft0,
      std::ref(_stm),
      std::ref(_members_table),
      std::ref(_tp_state),
      std::ref(_hm_frontend),
      std::ref(_config_frontend),
      std::ref(_feature_table),
      std::ref(_roles),
      std::addressof(_plugin_table),
      std::addressof(_feature_manager),
      std::ref(_as));
    co_await _metrics_reporter.invoke_on(0, &metrics_reporter::start);

    co_await _partition_balancer.start_single(
      _raft0,
      std::ref(_stm),
      std::ref(_partition_balancer_state),
      std::ref(_hm_backend),
      std::ref(_partition_allocator),
      std::ref(_tp_frontend),
      std::ref(_members_frontend),
      config::shard_local_cfg().partition_autobalancing_mode.bind(),
      config::shard_local_cfg()
        .partition_autobalancing_node_availability_timeout_sec.bind(),
      config::shard_local_cfg()
        .partition_autobalancing_max_disk_usage_percent.bind(),
      config::shard_local_cfg()
        .storage_space_alert_free_threshold_percent.bind(),
      config::shard_local_cfg().partition_autobalancing_tick_interval_ms.bind(),
      config::shard_local_cfg().partition_autobalancing_concurrent_moves.bind(),
      config::shard_local_cfg()
        .partition_autobalancing_tick_moves_drop_threshold.bind(),
      config::shard_local_cfg().segment_fallocation_step.bind(),
      config::shard_local_cfg()
        .partition_autobalancing_min_size_threshold.bind(),
      config::shard_local_cfg().node_status_interval.bind(),
      config::shard_local_cfg().raft_learner_recovery_rate.bind(),
      config::shard_local_cfg().partition_autobalancing_topic_aware.bind());
    co_await _partition_balancer.invoke_on(
      partition_balancer_backend::shard, &partition_balancer_backend::start);

    if (!config::node().recovery_mode_enabled()) {
        auto bucket_opt = get_configured_bucket();
        if (bucket_opt.has_value()) {
            cloud_storage_clients::bucket_name bucket = bucket_opt.value();
            _metadata_uploader = std::make_unique<cloud_metadata::uploader>(
              _raft_manager.local(),
              _storage.local(),
              bucket,
              _cloud_storage_api.local(),
              _raft0,
              _tp_state.local(),
              offsets_uploader);
            if (config::shard_local_cfg()
                  .enable_cluster_metadata_upload_loop()) {
                _metadata_uploader->start();
            }
            _recovery_backend
              = std::make_unique<cloud_metadata::cluster_recovery_backend>(
                _recovery_manager.local(),
                _raft_manager.local(),
                _cloud_storage_api.local(),
                _cloud_cache.local(),
                _members_table.local(),
                _feature_table.local(),
                _credentials.local(),
                _tp_state.local(),
                _api.local(),
                _feature_manager.local(),
                _config_frontend.local(),
                _security_frontend.local(),
                _tp_frontend.local(),
                producer_id_recovery,
                offsets_recovery,
                std::ref(_recovery_table),
                _raft0);
            if (!config::shard_local_cfg()
                   .disable_cluster_recovery_loop_for_tests()) {
                _recovery_backend->start();
            }
        }
    }

    co_await _data_migration_backend.start_on(
      data_migrations::data_migrations_shard,
      std::ref(_data_migration_table.local()),
      std::ref(_data_migration_frontend.local()),
      std::ref(_data_migration_worker),
      std::ref(_partition_leaders.local()),
      std::ref(_tp_frontend.local()),
      std::ref(_tp_state.local()),
      std::ref(_shard_table.local()),
      _cloud_storage_api.local_is_initialized()
        ? std::make_optional(std::ref(_cloud_storage_api.local()))
        : std::nullopt,
      std::ref(_as.local()));
    co_await _data_migration_backend.invoke_on_instance(
      &data_migrations::backend::start);
    co_await _data_migration_irpc_frontend.start(
      std::ref(_feature_table), std::ref(_data_migration_backend));
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
    if (_metadata_uploader) {
        _metadata_uploader->stop();
    }
    co_await _as.invoke_on_all(&ss::abort_source::request_abort);
    vlog(clusterlog.debug, "Shut down controller inputs");
}

ss::future<> controller::stop() {
    _probe.stop();

    if (!_as.local().abort_requested()) {
        co_await shutdown_input();
    }

    if (_leader_balancer) {
        co_await _leader_balancer->stop();
    }

    co_await ss::smp::submit_to(controller_stm_shard, [&stm = _stm] {
        if (stm.local_is_initialized()) {
            return stm.local().shutdown();
        }
        return ss::now();
    });

    if (_metadata_uploader) {
        co_await _metadata_uploader->stop_and_wait();
    }
    co_await _data_migration_irpc_frontend.stop();
    co_await _data_migration_backend.stop();
    if (_recovery_backend) {
        co_await _recovery_backend->stop_and_wait();
    }
    co_await _recovery_manager.stop();
    co_await _recovery_table.stop();
    co_await _partition_balancer.stop();
    co_await _metrics_reporter.stop();
    co_await _feature_manager.stop();
    co_await _hm_frontend.stop();
    co_await _hm_backend.stop();
    co_await _health_manager.stop();
    co_await _members_backend.stop();
    co_await _data_migration_worker.stop();
    co_await _data_migration_frontend.stop();
    co_await _config_manager.stop();
    co_await _api.stop();
    co_await _shard_balancer.stop();
    co_await _backend.stop();
    co_await _tp_frontend.stop();
    co_await _plugin_frontend.stop();
    co_await _quota_frontend.stop();
    co_await _ephemeral_credential_frontend.stop();
    co_await _security_frontend.stop();
    co_await _members_frontend.stop();
    co_await _config_frontend.stop();
    co_await _feature_backend.stop();
    co_await _bootstrap_backend.stop();
    co_await _oidc_service.stop();
    co_await _authorizer.stop();
    co_await _ephemeral_credentials.stop();
    co_await _data_migration_table.stop();
    co_await _data_migrated_resources.stop();
    co_await _roles.stop();
    co_await _credentials.stop();
    co_await _tp_state.stop();
    co_await _members_manager.stop();
    co_await _stm.stop();
    co_await _quota_backend.stop();
    co_await _quota_store.stop();
    co_await _plugin_backend.stop();
    co_await _plugin_table.stop();
    co_await _drain_manager.stop();
    co_await _shard_placement.stop();
    co_await _partition_balancer_state.stop();
    co_await _partition_allocator.stop();
    co_await _partition_leaders.stop();
    co_await _members_table.stop();
    co_await _gate.close();
    co_await _as.stop();
}

ss::future<> controller::create_cluster(bootstrap_cluster_cmd_data cmd_data) {
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

        // Check if there is any cluster metadata in the cloud.
        auto bucket_opt = get_configured_bucket();
        if (
          bucket_opt.has_value()
          && config::shard_local_cfg()
               .cloud_storage_attempt_cluster_restore_on_bootstrap.value()) {
            retry_chain_node retry_node(_as.local(), 300s, 5s);
            auto res
              = co_await cloud_metadata::download_highest_manifest_in_bucket(
                _cloud_storage_api.local(), bucket_opt.value(), retry_node);
            if (res.has_value()) {
                vlog(
                  clusterlog.info,
                  "Found cluster metadata manifest {} in bucket {}",
                  res.value(),
                  bucket_opt.value());
                cmd_data.recovery_state.emplace();
                cmd_data.recovery_state->manifest = std::move(res.value());
                cmd_data.recovery_state->bucket = bucket_opt.value();
                // Proceed with recovery via cluster bootstrap.
            } else {
                const auto& err = res.error();
                if (
                  err == cloud_metadata::error_outcome::no_matching_metadata) {
                    vlog(
                      clusterlog.info,
                      "No cluster manifest in bucket {}, proceeding without "
                      "recovery",
                      bucket_opt.value());
                    // Fall through to regular cluster bootstrap.
                } else {
                    vlog(
                      clusterlog.error,
                      "Error looking for cluster recovery material in cloud, "
                      "retrying: {}",
                      err);
                    co_await ss::sleep_abortable(
                      retry_jitter.next_duration(), _as.local());
                    continue;
                }
            }
        }

        vlog(clusterlog.info, "Creating cluster UUID {}", cmd_data.uuid);
        const std::error_code errc = co_await replicate_and_wait(
          _stm,
          _as,
          bootstrap_cluster_cmd{0, cmd_data},
          model::timeout_clock::now() + 30s,
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

ss::future<result<std::vector<partition_state>>>
controller::get_controller_partition_state() {
    std::vector<model::node_id> nodes_to_query
      = _members_table.local().node_ids();

    std::vector<ss::future<result<partition_state_reply>>> futures;
    futures.reserve(nodes_to_query.size());
    for (const auto& node : nodes_to_query) {
        futures.push_back(do_get_controller_partition_state(node));
    }

    auto finished_futures = co_await ss::when_all(
      futures.begin(), futures.end());
    std::vector<partition_state> results;
    results.reserve(finished_futures.size());
    for (auto& fut : finished_futures) {
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(
              clusterlog.info,
              "Failed to get controller partition state, failure: {}",
              ex);
            continue;
        }
        auto result = fut.get();
        if (result.has_error()) {
            vlog(
              clusterlog.info,
              "Failed to get controller state, result: {}",
              result.error());
            continue;
        }
        auto res = std::move(result.value());
        if (res.error_code != errc::success || !res.state) {
            vlog(
              clusterlog.debug,
              "Error during controller partition state fetch, error: {}",
              res.error_code);
            continue;
        }
        results.push_back(std::move(*res.state));
    }
    co_return results;
}

ss::future<result<partition_state_reply>>
controller::do_get_controller_partition_state(model::node_id target_node) {
    if (target_node == _raft0->self().id()) {
        partition_state_reply reply{};

        return _partition_manager.invoke_on(
          controller_stm_shard,
          [reply = std::move(reply)](partition_manager& pm) mutable {
              auto partition = pm.get(model::controller_ntp);
              if (!partition) {
                  reply.error_code = errc::partition_not_exists;
                  return ss::make_ready_future<result<partition_state_reply>>(
                    reply);
              }
              reply.state = ::cluster::get_partition_state(partition);
              reply.error_code = errc::success;
              return ss::make_ready_future<result<partition_state_reply>>(
                reply);
          });
    }
    auto timeout = model::timeout_clock::now() + 5s;
    return _connections.local().with_node_client<controller_client_protocol>(
      _raft0->self().id(),
      ss::this_shard_id(),
      target_node,
      timeout,
      [timeout](controller_client_protocol client) mutable {
          return client
            .get_partition_state(
              partition_state_request{.ntp = model::controller_ntp},
              rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<partition_state_reply>);
      });
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
ss::future<configuration_invariants>
controller::validate_configuration_invariants() {
    auto invariants_buf = _storage.local().kvs().get(
      storage::kvstore::key_space::controller, invariants_key());
    vassert(
      config::node().node_id(),
      "Node id must be set before checking configuration invariants");

    auto current = configuration_invariants(
      *config::node().node_id(), ss::smp::count);

    if (!invariants_buf) {
        // store configuration invariants
        co_await _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key(),
          reflection::to_iobuf(configuration_invariants{current}));
        vlog(
          clusterlog.info,
          "persisted initial configuration invariants: {}",
          current);
        co_return current;
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
        throw configuration_invariants_changed(invariants, current);
    }
    if (current.core_count > invariants.core_count) {
        // Update the persistent invariants to reflect increased core
        // count -- this tracks the high water mark of core count, to
        // track the number of extra kvstore shards that we need to process if
        // the core count later decreases.
        co_await _storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          invariants_key(),
          reflection::to_iobuf(configuration_invariants{current}));
        invariants = current;
        vlog(clusterlog.info, "updated configuration invariants: {}", current);
    } else if (current.core_count < invariants.core_count) {
        // If core count decreased, do nothing just now. shard_balancer will
        // check if decreasing is possible and we will update
        // configuration_invariants in kvstore later.
    }
    co_return invariants;
}

} // namespace cluster
