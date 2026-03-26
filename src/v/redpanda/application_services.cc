// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_topics/app.h"
#include "cluster/archival/types.h"
#include "cluster/cloud_metadata/offsets_lookup.h"
#include "cluster/controller.h"
#include "cluster/migrations/tx_manager_migrator.h"
#include "cluster/node_isolation_watcher.h"
#include "cluster/node_status_backend.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_manager.h"
#include "cluster/producer_state_manager.h"
#include "cluster/self_test_backend.h"
#include "cluster/self_test_frontend.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "raft/coordinated_recovery_throttle.h"
#include "raft/group_manager.h"
#include "redpanda/application.h"
#include "resource_mgmt/available_memory.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "resource_mgmt/storage.h"
#include "syschecks/syschecks.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

void application::wire_up_redpanda_services(
  model::node_id node_id,
  ::stop_signal& app_signal,
  std::optional<cloud_storage_clients::bucket_name>& bucket_name,
  cloud_topics::test_fixture_cfg ct_test_cfg) {
    ss::smp::invoke_on_all([] {
        resources::available_memory::local().register_metrics();
    }).get();

    // cluster
    syschecks::systemd_message("Initializing connection cache").get();
    construct_service(
      _connection_cache, std::ref(_as), std::nullopt, ss::sharded_parameter([] {
          return config::shard_local_cfg().rpc_client_connections_per_peer();
      }))
      .get();
    syschecks::systemd_message("Building shard-lookup tables").get();
    construct_service(shard_table).get();

    syschecks::systemd_message("Intializing raft recovery throttle").get();
    recovery_throttle
      .start(
        ss::sharded_parameter([] {
            return config::shard_local_cfg().raft_learner_recovery_rate.bind();
        }),
        ss::sharded_parameter([] {
            return config::shard_local_cfg()
              .raft_recovery_throttle_disable_dynamic_mode.bind();
        }))
      .get();

    recovery_throttle.invoke_on_all(&raft::coordinated_recovery_throttle::start)
      .get();

    syschecks::systemd_message("Intializing raft group manager").get();
    raft_group_manager
      .start(
        node_id,
        scheduling_groups::instance().raft_recv_sg(),
        scheduling_groups::instance().raft_send_sg(),
        scheduling_groups::instance().raft_heartbeats(),
        [] {
            return raft::group_manager::configuration{
              .heartbeat_interval
              = config::shard_local_cfg().raft_heartbeat_interval_ms.bind(),
              .heartbeat_timeout
              = config::shard_local_cfg().raft_heartbeat_timeout_ms.bind(),
              .raft_io_timeout_ms
              = config::shard_local_cfg().raft_io_timeout_ms.bind(),
              .enable_lw_heartbeat
              = config::shard_local_cfg().raft_enable_lw_heartbeat.bind(),
              .recovery_concurrency_per_shard
              = config::shard_local_cfg()
                  .raft_recovery_concurrency_per_shard.bind(),
              .election_timeout_ms
              = config::shard_local_cfg().raft_election_timeout_ms.bind(),
              .write_caching
              = config::shard_local_cfg().write_caching_default.bind(),
              .write_caching_flush_ms
              = config::shard_local_cfg()
                  .raft_replica_max_flush_delay_ms.bind(),
              .write_caching_flush_bytes
              = config::shard_local_cfg()
                  .raft_replica_max_pending_flush_bytes.bind(),
              .enable_longest_log_detection
              = config::shard_local_cfg()
                  .raft_enable_longest_log_detection.bind(),
              .max_buffered_bytes_per_node
              = config::shard_local_cfg()
                  .raft_max_buffered_follower_append_entries_bytes_per_shard
                  .bind(),
              .max_inflight_requests_per_node
              = config::shard_local_cfg()
                  .raft_max_inflight_follower_append_entries_requests_per_shard
                  .bind(),
            };
        },
        ss::sharded_parameter([] {
            return config::shard_local_cfg().raft_max_recovery_memory.bind();
        }),
        std::ref(_connection_cache),
        std::ref(storage),
        std::ref(recovery_throttle),
        std::ref(feature_table))
      .get();

    // custom handling for recovery_throttle and raft group manager shutdown.
    // the former needs to happen first in order to ensure that any raft groups
    // that are being throttled are released so that they can make be quickly
    // shutdown by the group manager.
    _deferred.emplace_back([this] {
        recovery_throttle
          .invoke_on_all(&raft::coordinated_recovery_throttle::shutdown)
          .get();
        raft_group_manager.stop().get();
        recovery_throttle.stop().get();
    });

    // Cloud IO and storage services.
    wire_up_services_cloud_io(app_signal, bucket_name);

    syschecks::systemd_message("Initializing producer state manager").get();
    construct_service(
      producer_manager,
      ss::sharded_parameter([]() {
          return config::shard_local_cfg().max_concurrent_producer_ids.bind();
      }),
      ss::sharded_parameter([]() {
          return config::shard_local_cfg()
            .transactional_id_expiration_ms.bind();
      }),
      ss::sharded_parameter([]() {
          return config::shard_local_cfg()
            .virtual_cluster_min_producer_ids.bind();
      }))
      .get();

    producer_manager.invoke_on_all(&cluster::tx::producer_state_manager::start)
      .get();

    if (config::shard_local_cfg().cloud_topics_enabled()) {
        vassert(
          archival_storage_enabled(),
          "cloud topics currently requires archival storage to be enabled");
        syschecks::systemd_message("Initializing cloud topics subsystems")
          .get();

        // Initialize the cloud topics app to be able to pass it around to the
        // partition manager.
        // NOTE: this only instantiates the app; underlying services are
        // constructed separately once more of the subsystems are available.
        cloud_topics_app = std::make_unique<cloud_topics::app>(
          fmt::format("{}/cloud_topics", _log.name()));
    }
    syschecks::systemd_message("Adding partition manager").get();
    construct_service(
      partition_manager,
      std::ref(storage),
      std::ref(raft_group_manager),
      std::ref(partition_recovery_manager),
      std::ref(cloud_storage_api),
      std::ref(shadow_index_cache),
      ss::sharded_parameter(
        [sg = scheduling_groups::instance().archival_upload(),
         enabled = archival_storage_enabled()]()
          -> ss::lw_shared_ptr<archival::configuration> {
            if (enabled) {
                return ss::make_lw_shared<archival::configuration>(
                  archival::get_archival_service_config(sg));
            } else {
                return nullptr;
            }
        }),
      std::ref(feature_table),
      std::ref(archival_upload_housekeeping),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .partition_manager_shutdown_watchdog_timeout.bind();
      }),
      cloud_topics_app ? cloud_topics_app->get_state() : nullptr)
      .get();
    vlog(_log.info, "Partition manager started");
    construct_service(
      offsets_lookup,
      node_id,
      std::ref(partition_manager),
      std::ref(shard_table))
      .get();

    construct_service(node_status_table, node_id).get();
    // controller
    syschecks::systemd_message("Creating cluster::controller").get();

    construct_single_service(
      controller,
      std::move(_config_preload),
      _connection_cache,
      partition_manager,
      shard_table,
      storage,
      local_monitor,
      std::ref(raft_group_manager),
      std::ref(feature_table),
      std::ref(cloud_storage_api),
      std::ref(shadow_index_cache),
      std::ref(node_status_table),
      std::ref(metadata_cache),
      scheduling_groups::instance().cluster_sg());
    controller->wire_up().get();

    if (config::node().recovery_mode_enabled()) {
        construct_single_service(
          _tx_manager_migrator,
          std::ref(controller->get_topics_frontend()),
          std::ref(controller->get_api()),
          std::ref(controller->get_topics_state()),
          std::ref(controller->get_partition_manager()),
          std::ref(controller->get_shard_table()),
          std::ref(metadata_cache),
          std::ref(_connection_cache),
          std::ref(controller->get_partition_leaders()),
          config::node().node_id().value(),
          config::shard_local_cfg().internal_topic_replication_factor(),
          config::shard_local_cfg().transaction_coordinator_partitions.bind(),
          std::ref(_as.local()));
    }

    // Cloud archival services (purger, archiver manager).
    wire_up_services_cloud_archival(node_id);

    construct_single_service_sharded(
      self_test_backend,
      node_id,
      std::ref(local_monitor),
      std::ref(_connection_cache),
      std::ref(cloud_storage_api),
      scheduling_groups::instance().self_test_sg())
      .get();

    construct_single_service_sharded(
      self_test_frontend,
      node_id,
      std::ref(controller->get_members_table()),
      std::ref(self_test_backend),
      std::ref(_connection_cache))
      .get();

    construct_single_service_sharded(
      node_status_backend,
      node_id,
      std::ref(controller->get_members_table()),
      std::ref(feature_table),
      std::ref(node_status_table),
      ss::sharded_parameter(
        [] { return config::shard_local_cfg().node_status_interval.bind(); }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .node_status_reconnect_max_backoff_ms.bind();
      }),
      std::ref(_as))
      .get();

    syschecks::systemd_message("Creating kafka metadata cache").get();
    construct_service(
      metadata_cache,
      std::ref(controller->get_topics_state()),
      std::ref(controller->get_data_migrated_resources()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller->get_health_monitor()))
      .get();

    syschecks::systemd_message("Creating isolation node watcher").get();
    construct_single_service(
      _node_isolation_watcher,
      metadata_cache,
      controller->get_health_monitor(),
      node_status_table);

    // Kafka data RPC, quotas, audit, metadata dissemination.
    wire_up_services_kafka_data(node_id);

    auto fs_avail
      = ss::fs_avail(config::node().data_directory().path.string()).get();

    // Shadow index cache, upload controller, topic recovery.
    wire_up_services_cloud_cache(node_id, fs_avail);

    // Inventory service.
    wire_up_services_cloud_inventory(bucket_name);

    construct_single_service(
      space_manager,
      config::shard_local_cfg().space_management_enable.bind(),
      config::shard_local_cfg().space_management_enable_override.bind(),
      config::shard_local_cfg().retention_local_target_capacity_bytes.bind(),
      config::shard_local_cfg().retention_local_target_capacity_percent.bind(),
      config::shard_local_cfg().disk_reservation_percent.bind(),
      &local_monitor,
      &storage,
      &storage_node,
      &shadow_index_cache,
      &partition_manager);

    if (cloud_topics_app) {
        syschecks::systemd_message("Starting cloud topics subsystems").get();
        cloud_topics_app
          ->construct(
            node_id,
            controller.get(),
            &controller->get_partition_leaders(),
            &controller->get_shard_table(),
            &cloud_io,
            &shadow_index_cache,
            &metadata_cache,
            &_connection_cache,
            bucket_name.value(),
            &storage,
            ct_test_cfg.skip_flush_loop,
            ct_test_cfg.skip_level_zero_gc)
          .get();
    }

    // Group management, tx frontends.
    wire_up_services_core_2(node_id);

    // Kafka server, compaction controller.
    wire_up_services_kafka_server(fs_avail);
}
