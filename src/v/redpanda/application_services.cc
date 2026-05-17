// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_io/cache_service.h"
#include "cloud_io/remote.h"
#include "cloud_storage/configuration.h"
#include "cloud_storage/inventory/inv_ops.h"
#include "cloud_storage/inventory/types.h"
#include "cloud_storage/remote.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/upstream_registry.h"
#include "cloud_topics/app.h"
#include "cluster/archival/archiver_manager.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/purger.h"
#include "cluster/archival/upload_controller.h"
#include "cluster/archival/upload_housekeeping_service.h"
#include "cluster/cloud_metadata/offsets_lookup.h"
#include "cluster/cloud_metadata/offsets_recoverer.h"
#include "cluster/cloud_metadata/offsets_recovery_manager.h"
#include "cluster/cloud_metadata/offsets_recovery_router.h"
#include "cluster/cloud_metadata/offsets_upload_router.h"
#include "cluster/cloud_metadata/offsets_uploader.h"
#include "cluster/cloud_metadata/producer_id_recovery_manager.h"
#include "cluster/controller.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/inventory_service.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/migrations/tx_manager_migrator.h"
#include "cluster/node_isolation_watcher.h"
#include "cluster/partition_manager.h"
#include "cluster/partition_recovery_manager.h"
#include "cluster/producer_state_manager.h"
#include "cluster/rm_partition_frontend.h"
#include "cluster/topic_recovery_service.h"
#include "cluster/topic_recovery_status_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/tx_topic_manager.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/service.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/data_migration_group_proxy_impl.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/queue_depth_monitor_config.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/rm_group_frontend.h"
#include "kafka/server/snc_quota_manager.h"
#include "net/dns.h"
#include "net/tls_certificate_probe.h"
#include "raft/coordinated_recovery_throttle.h"
#include "raft/group_manager.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "resource_mgmt/storage.h"
#include "rpc/rpc_utils.h"
#include "security/audit/audit_log_manager.h"
#include "storage/compaction_controller.h"
#include "syschecks/syschecks.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

// Forward declarations (defined in application_config.cc)
storage::backlog_controller_config
compaction_controller_config(ss::scheduling_group sg, uint64_t fs_avail);

storage::backlog_controller_config
make_upload_controller_config(ss::scheduling_group sg, uint64_t fs_avail);

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

    model::cloud_storage_backend backend{model::cloud_storage_backend::unknown};
    std::optional<cloud_storage::configuration> cloud_config;
    if (requires_cloud_io()) {
        syschecks::systemd_message("Starting cloud IO").get();
        cloud_config = cloud_storage::configuration::get_config().get();
        backend = cloud_storage_clients::infer_backend_from_configuration(
          cloud_config->client_config, cloud_config->cloud_credentials_source);
        construct_service(upstreams, cloud_config->client_config).get();
        upstreams
          .invoke_on_all(&cloud_storage_clients::upstream_registry::start)
          .get();
        upstreams
          .invoke_on_all(
            &cloud_storage_clients::upstream_registry::start_evictor,
            /*interval=*/30s,
            /*max_idle_time=*/300s)
          .get();
        construct_service(
          cloud_storage_clients,
          ss::sharded_parameter([this] { return std::ref(upstreams.local()); }),
          cloud_config->connection_limit,
          cloud_config->client_config,
          cloud_config->scheduler,
          cloud_storage_clients::client_pool_overdraft_policy::borrow_if_empty)
          .get();
        cloud_storage_clients
          .invoke_on_all(
            &cloud_storage_clients::client_pool::start,
            ss::sharded_parameter(
              [&app_signal]()
                -> std::optional<std::reference_wrapper<::stop_signal>> {
                  if (
                    ss::this_shard_id()
                    == cloud_storage_clients::self_config_shard) {
                      return std::ref(app_signal);
                  }

                  return std::nullopt;
              }))
          .get();
        construct_service(
          cloud_io,
          std::ref(cloud_storage_clients),
          cloud_config->client_config,
          cloud_config->cloud_credentials_source,
          ss::sharded_parameter(
            [] { return scheduling_groups::instance().ts_read_sg(); }))
          .get();
        cloud_io.invoke_on_all(&cloud_io::remote::start).get();
        bucket_name = cloud_config->bucket_name;
    }

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting cloud storage api").get();
        construct_service(
          cloud_storage_api, std::ref(cloud_io), cloud_config.value())
          .get();
        cloud_storage_api.invoke_on_all(&cloud_storage::remote::start).get();

        construct_service(
          partition_recovery_manager,
          cloud_config->bucket_name,
          std::ref(cloud_storage_api))
          .get();

        construct_service(
          archival_upload_housekeeping,
          std::ref(cloud_storage_api),
          ss::sharded_parameter(
            [sg = scheduling_groups::instance().archival_upload()] {
                return sg;
            }))
          .get();
        archival_upload_housekeeping
          .invoke_on_all(&archival::upload_housekeeping_service::start)
          .get();

        construct_service(
          offsets_uploader,
          cloud_config->bucket_name,
          std::ref(_group_manager),
          std::ref(cloud_storage_api))
          .get();
    }

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

    if (
      config::shard_local_cfg().cloud_storage_enabled()
      && !ct_test_cfg.disable_cloud_topics) {
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

    if (archival_storage_enabled() && !config::node().recovery_mode_enabled()) {
        construct_service(
          offsets_upload_router,
          std::ref(offsets_uploader),
          std::ref(shard_table),
          std::ref(metadata_cache),
          std::ref(_connection_cache),
          std::ref(controller->get_partition_leaders()),
          node_id)
          .get();

        construct_service(
          _archival_purger,
          ss::sharded_parameter(
            [&api = cloud_storage_api]() { return std::ref(api.local()); }),
          ss::sharded_parameter([&t = controller->get_topics_state()]() {
              return std::ref(t.local());
          }),
          std::ref(controller->get_topics_frontend()),
          std::ref(controller->get_members_table()))
          .get();

        _archival_purger
          .invoke_on_all(
            [&housekeeping = archival_upload_housekeeping](
              archival::purger& s) { housekeeping.local().register_jobs({s}); })
          .get();

        _deferred.emplace_back([this] {
            _archival_purger
              .invoke_on_all([&housekeeping = archival_upload_housekeeping,
                              this](archival::purger& s) {
                  vlog(_log.debug, "Deregistering purger housekeeping jobs");
                  housekeeping.local().deregister_jobs({s});
              })
              .get();
        });
    }

    vlog(
      _log.info,
      "Archiver service setup, cloud_storage_enabled: {}, "
      "legacy_upload_mode_enabled: {}",
      archival_storage_enabled(),
      config::shard_local_cfg().cloud_storage_disable_archiver_manager.value());
    if (
      archival_storage_enabled()
      && !config::shard_local_cfg()
            .cloud_storage_disable_archiver_manager.value()) {
        construct_service(
          archiver_manager,
          node_id,
          std::ref(partition_manager),
          std::ref(raft_group_manager),
          std::ref(cloud_storage_api),
          std::ref(shadow_index_cache),
          std::ref(archival_upload_housekeeping),
          ss::sharded_parameter(
            [sg = scheduling_groups::instance().archival_upload(),
             enabled = archival_storage_enabled()]()
              -> ss::lw_shared_ptr<const archival::configuration> {
                if (enabled) {
                    return ss::make_lw_shared<const archival::configuration>(
                      archival::get_archival_service_config(sg));
                } else {
                    return nullptr;
                }
            }))
          .get();
    }

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

    // metrics and quota management
    syschecks::systemd_message("Adding kafka quota managers").get();
    construct_service(quota_mgr, std::ref(controller->get_quota_store())).get();
    construct_service(snc_quota_mgr, std::ref(snc_node_quota)).get();

    // TODO(oren): separate service group for kafka data
    construct_service(
      _kafka_data_rpc_service,
      ss::sharded_parameter([this] {
          return kafka::data::rpc::topic_metadata_cache::make_default(
            &metadata_cache);
      }),
      ss::sharded_parameter([this] {
          return kafka::data::rpc::partition_manager::make_default(
            &shard_table,
            &partition_manager,
            smp_service_groups.transform_smp_sg());
      }),
      ss::sharded_parameter([this] {
          return kafka::data::rpc::shadow_link_registry::make_default(
            &controller->get_cluster_link_frontend());
      }))
      .get();

    construct_service(
      _kafka_data_rpc_client,
      node_id,
      ss::sharded_parameter([this] {
          return kafka::data::rpc::partition_leader_cache::make_default(
            &controller->get_partition_leaders());
      }),
      ss::sharded_parameter([this] {
          return kafka::data::rpc::topic_creator::make_default(
            controller.get());
      }),
      ss::sharded_parameter([this] {
          return kafka::data::rpc::topic_metadata_cache::make_default(
            &metadata_cache);
      }),
      &_connection_cache,
      &_kafka_data_rpc_service)
      .get();

    syschecks::systemd_message("Creating auditing subsystem").get();
    if (!_audit_log_client_config.has_value()) {
        _audit_log_client_config.emplace();
    }
    construct_service(
      audit_mgr,
      node_id,
      controller.get(),
      &metadata_cache,
      &_kafka_data_rpc_client,
      std::ref(_audit_log_client_config.value()))
      .get();

    syschecks::systemd_message("Creating metadata dissemination service").get();
    construct_service(
      md_dissemination_service,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_topics_state()),
      std::ref(_connection_cache),
      std::ref(controller->get_health_monitor()),
      std::ref(feature_table))
      .get();

    auto fs_avail
      = ss::fs_avail(config::node().data_directory().path.string()).get();
    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting shadow indexing cache").get();
        auto redpanda_dir = config::node().data_directory.value();
        construct_service(
          shadow_index_cache,
          config::node().cloud_storage_cache_path(),
          local_monitor.local().get_state_cached().get_cache_disk().total,
          ss::sharded_parameter([] {
              return config::shard_local_cfg().disk_reservation_percent.bind();
          }),
          ss::sharded_parameter([] {
              return config::shard_local_cfg().cloud_storage_cache_size.bind();
          }),
          ss::sharded_parameter([] {
              return config::shard_local_cfg()
                .cloud_storage_cache_size_percent.bind();
          }),
          ss::sharded_parameter([] {
              return config::shard_local_cfg()
                .cloud_storage_cache_max_objects.bind();
          }),
          ss::sharded_parameter([] {
              return config::shard_local_cfg()
                .cloud_storage_cache_trim_walk_concurrency.bind();
          }))
          .get();

        // Hook up local_monitor to update storage_resources when disk state
        // changes
        auto cloud_storage_cache_disk_notification
          = storage_node.local().register_disk_notification(
            storage::node::disk_type::cache,
            [this](storage::node::disk_space_info info) {
                return shadow_index_cache.local().notify_disk_status(
                  info.total, info.free, info.alert);
            });
        _deferred.emplace_back([this, cloud_storage_cache_disk_notification] {
            storage_node.local().unregister_disk_notification(
              storage::node::disk_type::cache,
              cloud_storage_cache_disk_notification);
        });

        shadow_index_cache
          .invoke_on_all([](cloud_io::cache& cache) { return cache.start(); })
          .get();

        construct_service(
          _archival_upload_controller,
          std::ref(partition_manager),
          ss::sharded_parameter(
            [sg = scheduling_groups::instance().archival_upload(), fs_avail] {
                return make_upload_controller_config(sg, fs_avail);
            }))
          .get();

        construct_service(
          topic_recovery_status_frontend,
          node_id,
          std::ref(_connection_cache),
          std::ref(controller->get_members_table()))
          .get();

        construct_service(
          topic_recovery_service,
          std::ref(cloud_storage_api),
          std::ref(controller->get_topics_state()),
          std::ref(controller->get_topics_frontend()),
          std::ref(topic_recovery_status_frontend))
          .get();

        partition_recovery_manager
          .invoke_on_all(
            [this](cloud_storage::partition_recovery_manager& prm) {
                prm.set_topic_recovery_components(
                  topic_recovery_status_frontend, topic_recovery_service);
            })
          .get();

        if (
          config::shard_local_cfg()
            .cloud_storage_inventory_based_scrub_enabled()
          && config::shard_local_cfg().cloud_storage_enable_scrubbing()) {
            const auto manual_setup
              = config::shard_local_cfg()
                  .cloud_storage_inventory_self_managed_report_config();
            const auto supported = cloud_storage::inventory::
              validate_backend_supported_for_inventory_scrub(backend);
            if (!manual_setup && !supported) {
                throw std::runtime_error(
                  fmt::format(
                    "cloud storage backend inferred as {} which is "
                    "not supported for inventory based scrubbing",
                    backend));
            }

            std::shared_ptr<cluster::leaders_provider> leaders_provider
              = std::make_shared<cluster::default_leaders_provider>(
                controller->get_partition_leaders());
            std::shared_ptr<cluster::remote_provider> remote_provider
              = std::make_shared<cluster::default_remote_provider>(
                cloud_storage_api);
            auto inv_ops
              = cloud_storage::inventory::make_inv_ops(
                  bucket_name.value(),
                  cloud_storage::inventory::inventory_config_id{
                    config::shard_local_cfg().cloud_storage_inventory_id()},
                  config::shard_local_cfg()
                    .cloud_storage_inventory_reports_prefix())
                  .get();
            const auto report_check_interval
              = config::shard_local_cfg()
                  .cloud_storage_inventory_report_check_interval_ms();
            // If the self-managed flag is enabled, do not create report
            // schedule
            const auto should_create_report_config
              = !config::shard_local_cfg()
                   .cloud_storage_inventory_self_managed_report_config();
            construct_single_service_sharded(
              inventory_service,
              config::node().cloud_storage_inventory_hash_path(),
              leaders_provider,
              remote_provider,
              std::move(inv_ops),
              report_check_interval,
              should_create_report_config)
              .get();
            inventory_service
              .invoke_on(
                cluster::inventory_service::shard_id,
                &cluster::inventory_service::start)
              .get();
        }
    }

    construct_single_service(
      space_manager,
      config::shard_local_cfg().space_management_enable.bind(),
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

    // group membership
    syschecks::systemd_message("Creating kafka group manager").get();
    construct_service(
      _group_manager,
      model::kafka_consumer_offsets_nt,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_topics_state()),
      std::ref(tx_gateway_frontend),
      std::ref(controller->get_feature_table()),
      std::ref(controller->get_health_monitor()))
      .get();
    construct_service(
      offsets_recoverer,
      node_id,
      std::ref(cloud_storage_api),
      std::ref(shadow_index_cache),
      std::ref(offsets_lookup),
      std::ref(controller->get_partition_leaders()),
      std::ref(_connection_cache),
      std::ref(_group_manager))
      .get();
    construct_service(
      offsets_recovery_router,
      std::ref(offsets_recoverer),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      node_id)
      .get();

    syschecks::systemd_message("Creating kafka group shard mapper").get();
    construct_service(
      coordinator_ntp_mapper,
      std::ref(metadata_cache),
      model::kafka_consumer_offsets_nt)
      .get();
    construct_service(
      group_initializer,
      ref_to_local(coordinator_ntp_mapper),
      std::ref(controller->get_topics_frontend()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_api()))
      .get();
    construct_service(
      _data_migrations_group_proxy, ss::sharded_parameter([&]() {
          return std::make_unique<kafka::data_migration_group_proxy_impl>(
            coordinator_ntp_mapper.local(),
            _group_manager.local(),
            group_initializer.local());
      }))
      .get();

    offsets_recovery_manager
      = ss::make_shared<cluster::cloud_metadata::offsets_recovery_manager>(
        std::ref(offsets_recovery_router), group_initializer.local());

    syschecks::systemd_message("Creating kafka group router").get();
    construct_service(
      group_router,
      scheduling_groups::instance().kafka_sg(),
      smp_service_groups.kafka_smp_sg(),
      std::ref(_group_manager),
      std::ref(shard_table),
      std::ref(coordinator_ntp_mapper),
      ref_to_local(group_initializer))
      .get();

    syschecks::systemd_message("Creating tx coordinator mapper").get();
    construct_service(tx_coordinator_ntp_mapper, std::ref(metadata_cache))
      .get();

    syschecks::systemd_message("Creating id allocator frontend").get();
    construct_service(
      id_allocator_frontend,
      smp_service_groups.raft_smp_sg(),
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      node_id,
      std::ref(controller))
      .get();

    producer_id_recovery_manager
      = ss::make_shared<cluster::cloud_metadata::producer_id_recovery_manager>(
        std::ref(controller->get_members_table()),
        std::ref(_connection_cache),
        std::ref(id_allocator_frontend));

    syschecks::systemd_message("Creating group resource manager frontend")
      .get();

    construct_service(
      rm_group_frontend,
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      controller.get(),
      std::ref(group_router),
      ref_to_local(group_initializer))
      .get();

    _rm_group_proxy = std::make_unique<kafka::rm_group_proxy_impl>(
      std::ref(rm_group_frontend));

    syschecks::systemd_message("Creating partition resource manager frontend")
      .get();
    construct_service(
      rm_partition_frontend,
      smp_service_groups.raft_smp_sg(),
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      controller.get())
      .get();

    syschecks::systemd_message("Creating tx coordinator frontend").get();
    construct_single_service_sharded(
      tx_topic_manager,
      std::ref(*controller),
      config::shard_local_cfg().transaction_coordinator_partitions.bind(),
      config::shard_local_cfg().transaction_coordinator_log_segment_size.bind(),
      config::shard_local_cfg()
        .transaction_coordinator_delete_retention_ms.bind())
      .get();
    tx_topic_manager
      .invoke_on(
        cluster::tx_topic_manager::shard, &cluster::tx_topic_manager::start)
      .get();
    // usually it'a an anti-pattern to let the same object be accessed
    // from different cores without precautionary measures like foreign
    // ptr. we treat exceptions on the case by case basis validating the
    // access patterns, sharing sharded service with only `.local()' uses
    // is a safe bet, sharing _rm_group_proxy is fine because it wraps
    // sharded service with only `.local()' access
    construct_service(
      tx_gateway_frontend,
      smp_service_groups.raft_smp_sg(),
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      node_id,
      std::ref(id_allocator_frontend),
      _rm_group_proxy.get(),
      std::ref(rm_partition_frontend),
      std::ref(feature_table),
      std::ref(tx_topic_manager),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .max_transactions_per_coordinator.bind();
      }))
      .get();
    _kafka_conn_quotas
      .start(
        []() {
            return net::conn_quota_config{
              .max_connections
              = config::shard_local_cfg().kafka_connections_max.bind(),
              .max_connections_per_ip
              = config::shard_local_cfg().kafka_connections_max_per_ip.bind(),
              .max_connections_overrides
              = config::shard_local_cfg()
                  .kafka_connections_max_overrides.bind(),
            };
        },
        &kafka::klog)
      .get();

    ss::sharded<net::server_configuration> kafka_cfg;
    kafka_cfg.start(ss::sstring("kafka_rpc")).get();
    auto kafka_cfg_cleanup = ss::defer(
      [&kafka_cfg]() { kafka_cfg.stop().get(); });
    kafka_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              c.conn_quotas = std::ref(_kafka_conn_quotas);
              c.max_service_memory_per_core = int64_t(
                memory_groups().kafka_total_memory());
              c.listen_backlog
                = config::shard_local_cfg().rpc_server_listen_backlog;
              if (config::shard_local_cfg().kafka_rpc_server_tcp_recv_buf()) {
                  c.tcp_recv_buf
                    = config::shard_local_cfg().kafka_rpc_server_tcp_recv_buf;
              } else {
                  // Backward compat: prior to Redpanda 22.2, rpc_server_*
                  // settings applied to both Kafka and Internal RPC listeners.
                  c.tcp_recv_buf
                    = config::shard_local_cfg().rpc_server_tcp_recv_buf;
              };
              if (config::shard_local_cfg().kafka_rpc_server_tcp_send_buf()) {
                  c.tcp_send_buf
                    = config::shard_local_cfg().kafka_rpc_server_tcp_send_buf;
              } else {
                  // Backward compat: prior to Redpanda 22.2, rpc_server_*
                  // settings applied to both Kafka and Internal RPC listeners.
                  c.tcp_send_buf
                    = config::shard_local_cfg().rpc_server_tcp_send_buf;
              }

              c.stream_recv_buf
                = config::shard_local_cfg().kafka_rpc_server_stream_recv_buf;
              auto& tls_config = config::node().kafka_api_tls.value();
              for (const auto& ep : config::node().kafka_api()) {
                  ss::shared_ptr<ss::tls::server_credentials> credentials
                    = nullptr;
                  // find credentials for this endpoint
                  auto it = find_if(
                    tls_config.begin(),
                    tls_config.end(),
                    [&ep](const config::endpoint_tls_config& cfg) {
                        return cfg.name == ep.name;
                    });
                  // if tls is configured for this endpoint build reloadable
                  // credentials
                  if (it != tls_config.end()) {
                      syschecks::systemd_message(
                        "Building TLS credentials for kafka")
                        .get();
                      credentials
                        = net::build_reloadable_server_credentials_with_probe(
                            it->config,
                            "kafka",
                            it->name,
                            [this](
                              const std::unordered_set<ss::sstring>& updated,
                              const std::exception_ptr& eptr) {
                                rpc::log_certificate_reload_event(
                                  _log, "Kafka RPC TLS", updated, eptr);
                            })
                            .get();
                  }

                  c.addrs.emplace_back(
                    ep.name, net::resolve_dns(ep.address).get(), credentials);
              }

              c.disable_metrics = net::metrics_disabled(
                config::shard_local_cfg().disable_metrics());
              c.disable_public_metrics = net::public_metrics_disabled(
                config::shard_local_cfg().disable_public_metrics());

              net::config_connection_rate_bindings bindings{
                .config_general_rate
                = config::shard_local_cfg().kafka_connection_rate_limit.bind(),
                .config_overrides_rate
                = config::shard_local_cfg()
                    .kafka_connection_rate_limit_overrides.bind(),
              };

              c.connection_rate_bindings.emplace(std::move(bindings));

              c.tcp_keepalive_bindings.emplace(
                net::tcp_keepalive_bindings{
                  .keepalive_idle_time
                  = config::shard_local_cfg()
                      .kafka_tcp_keepalive_idle_timeout_seconds.bind(),
                  .keepalive_interval
                  = config::shard_local_cfg()
                      .kafka_tcp_keepalive_probe_interval_seconds.bind(),
                  .keepalive_probes
                  = config::shard_local_cfg().kafka_tcp_keepalive_probes.bind(),
                });
          });
      })
      .get();
    std::optional<kafka::qdc_monitor_config> qdc_config;
    if (config::shard_local_cfg().kafka_qdc_enable()) {
        qdc_config = kafka::qdc_monitor_config{
          .latency_alpha = config::shard_local_cfg().kafka_qdc_latency_alpha(),
          .max_latency = config::shard_local_cfg().kafka_qdc_max_latency_ms(),
          .window_count = config::shard_local_cfg().kafka_qdc_window_count(),
          .window_size = config::shard_local_cfg().kafka_qdc_window_size_ms(),
          .depth_alpha = config::shard_local_cfg().kafka_qdc_depth_alpha(),
          .idle_depth = config::shard_local_cfg().kafka_qdc_idle_depth(),
          .min_depth = config::shard_local_cfg().kafka_qdc_min_depth(),
          .max_depth = config::shard_local_cfg().kafka_qdc_max_depth(),
          .depth_update_freq
          = config::shard_local_cfg().kafka_qdc_depth_update_ms(),
        };
    }
    syschecks::systemd_message("Starting kafka RPC {}", kafka_cfg.local())
      .get();
    _kafka_server
      .init(
        &kafka_cfg,
        smp_service_groups.kafka_smp_sg(),
        scheduling_groups::instance().fetch_sg(),
        scheduling_groups::instance().produce_sg(),
        scheduling_groups::instance().kafka_sg(),
        std::ref(metadata_cache),
        std::ref(controller->get_topics_frontend()),
        std::ref(controller->get_config_frontend()),
        std::ref(controller->get_feature_table()),
        std::ref(controller->get_quota_frontend()),
        std::ref(controller->get_quota_store()),
        std::ref(quota_mgr),
        std::ref(snc_quota_mgr),
        std::ref(group_router),
        std::ref(usage_manager),
        std::ref(shard_table),
        std::ref(partition_manager),
        std::ref(id_allocator_frontend),
        std::ref(controller->get_credential_store()),
        std::ref(controller->get_authorizer()),
        std::ref(audit_mgr),
        std::ref(controller->get_oidc_service()),
        std::ref(controller->get_security_frontend()),
        std::ref(controller->get_api()),
        std::ref(tx_gateway_frontend),
        std::ref(datalake_throttle_manager),
        std::ref(controller->get_cluster_link_frontend()),
        qdc_config,
        std::ref(*thread_worker),
        std::ref(_schema_registry))
      .get();
    construct_service(
      _compaction_controller,
      std::ref(storage),
      ss::sharded_parameter(
        [sg = scheduling_groups::instance().compaction_sg(), fs_avail] {
            return compaction_controller_config(sg, fs_avail);
        }))
      .get();
}
