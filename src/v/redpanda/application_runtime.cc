// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage_clients/types.h"
#include "cloud_topics/app.h"
#include "cluster/controller.h"
#include "cluster/utils/partition_change_notifier_impl.h"
#include "cluster_link/service.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "datalake/coordinator/catalog_factory.h"
#include "datalake/coordinator/coordinator_manager.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/credential_manager.h"
#include "datalake/datalake_manager.h"
#include "debug_bundle/debug_bundle_service.h"
#include "kafka/data/rpc/client.h"
#include "kafka/server/usage_manager.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/schema_registry/api.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "syschecks/syschecks.h"
#include "transform/api.h"
#include "transform/rpc/client.h"
#include "transform/rpc/service.h"
#include "wasm/cache.h"
#include "wasm/impl.h"

#include <seastar/core/metrics.hh>

void application::wire_up_runtime_services(
  model::node_id node_id,
  ::stop_signal& app_signal,
  cloud_topics::test_fixture_cfg ct_test_cfg) {
    std::optional<cloud_storage_clients::bucket_name> bucket;
    wire_up_redpanda_services(node_id, app_signal, bucket, ct_test_cfg);
    if (_proxy_config) {
        construct_single_service(
          _proxy,
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups().kafka_total_memory(),
          *_proxy_client_config,
          *_proxy_config,
          controller.get(),
          _datalake_coordinator_fe);
    }
    if (_schema_reg_config) {
        construct_single_service(
          _schema_registry,
          node_id,
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups().kafka_total_memory(),
          *_schema_reg_client_config,
          *_schema_reg_config,
          &metadata_cache,
          std::reference_wrapper(controller),
          std::ref(audit_mgr));
    }

    if (wasm_data_transforms_enabled()) {
        syschecks::systemd_message("Starting wasm runtime").get();
        auto base_runtime = wasm::create_default_runtime(
          _schema_registry.get());
        construct_single_service(_wasm_runtime, std::move(base_runtime));

        syschecks::systemd_message("Starting data transforms").get();
        construct_service(
          _transform_rpc_service,
          ss::sharded_parameter([this] {
              return kafka::data::rpc::topic_metadata_cache::make_default(
                &metadata_cache);
          }),
          ss::sharded_parameter([this] {
              return transform::rpc::partition_manager::make_default(
                &shard_table,
                &partition_manager,
                smp_service_groups.transform_smp_sg());
          }),
          ss::sharded_parameter([this] {
              return transform::service::create_reporter(&_transform_service);
          }),
          ss::sharded_parameter([this] {
              return kafka::data::rpc::shadow_link_registry::make_default(
                &controller->get_cluster_link_frontend());
          }))
          .get();
        construct_service(
          _transform_rpc_client,
          node_id,
          ss::sharded_parameter([this] {
              return kafka::data::rpc::partition_leader_cache::make_default(
                &controller->get_partition_leaders());
          }),
          ss::sharded_parameter([this] {
              return kafka::data::rpc::topic_metadata_cache::make_default(
                &metadata_cache);
          }),
          ss::sharded_parameter([this] {
              return transform::rpc::cluster_members_cache::make_default(
                &controller->get_members_table());
          }),
          &_connection_cache,
          &_transform_rpc_service,
          &_kafka_data_rpc_client,
          &controller->get_feature_table(),
          ss::sharded_parameter([] {
              return config::shard_local_cfg()
                .data_transforms_binary_max_size.bind();
          }))
          .get();

        construct_service(
          _transform_service,
          _wasm_runtime.get(),
          node_id,
          &controller->get_plugin_frontend(),
          &controller->get_feature_table(),
          ss::sharded_parameter([this] {
              return cluster::partition_change_notifier_impl::make_default(
                raft_group_manager,
                partition_manager,
                controller->get_topics_state());
          }),
          &controller->get_topics_state(),
          &partition_manager,
          &_transform_rpc_client,
          &metadata_cache,
          scheduling_groups::instance().transforms_sg(),
          memory_groups().data_transforms_max_memory())
          .get();
    }

    if (datalake_enabled()) {
        vassert(
          bucket.has_value(),
          "Bucket should have been set when configuring cloud IO");
        // Construct datalake subsystems, now that dependencies are
        // already constructed.
        syschecks::systemd_message("Starting datalake services").get();

        // Start credential manager first to provide shared credentials
        construct_service(_datalake_credential_mgr).get();
        _datalake_credential_mgr
          .invoke_on_all(&datalake::credential_manager::start)
          .get();
        construct_service(
          _datalake_coordinator_mgr,
          node_id,
          std::ref(storage),
          std::ref(raft_group_manager),
          std::ref(partition_manager),
          std::ref(controller->get_topics_state()),
          std::ref(controller->get_topics_frontend()),
          _schema_registry.get(),
          ss::sharded_parameter(
            [bucket](
              cloud_io::remote& remote, datalake::credential_manager& cred_mgr)
              -> std::unique_ptr<datalake::coordinator::catalog_factory> {
                return datalake::coordinator::get_catalog_factory(
                  config::shard_local_cfg(),
                  remote,
                  *bucket,
                  ss::metrics::label_instance{"role", "coordinator"},
                  cred_mgr);
            },
            std::ref(cloud_io),
            std::ref(_datalake_credential_mgr)),
          std::ref(cloud_io),
          std::ref(*bucket),
          ss::sharded_parameter([this] { return &feature_table.local(); }))
          .get();
        construct_service(
          _datalake_coordinator_fe,
          node_id,
          &_datalake_coordinator_mgr,
          &raft_group_manager,
          &partition_manager,
          &controller->get_topics_frontend(),
          &metadata_cache,
          &controller->get_partition_leaders(),
          &controller->get_shard_table(),
          &_connection_cache)
          .get();

        construct_service(
          _datalake_manager,
          node_id,
          ss::sharded_parameter([this] {
              return cluster::partition_change_notifier_impl::make_default(
                raft_group_manager,
                partition_manager,
                controller->get_topics_state());
          }),
          &partition_manager,
          &controller->get_topics_state(),
          &feature_table,
          &_datalake_coordinator_fe,
          &cloud_io,
          ss::sharded_parameter(
            [bucket](
              cloud_io::remote& remote, datalake::credential_manager& cred_mgr)
              -> std::unique_ptr<datalake::coordinator::catalog_factory> {
                return datalake::coordinator::get_catalog_factory(
                  config::shard_local_cfg(),
                  remote,
                  *bucket,
                  ss::metrics::label_instance{"role", "translator"},
                  cred_mgr);
            },
            std::ref(cloud_io),
            std::ref(_datalake_credential_mgr)),
          _schema_registry.get(),
          &_as,
          *bucket,
          scheduling_groups::instance().datalake_sg(),
          memory_groups().datalake_max_memory())
          .get();
        datalake::datalake_manager::prepare_staging_directory(
          config::node().datalake_staging_path())
          .get();
        _datalake_manager.invoke_on_all(&datalake::datalake_manager::start)
          .get();

        construct_service(
          datalake_throttle_manager,
          [&mgr = _datalake_manager] {
              return ssx::now(
                kafka::datalake_throttle_manager::status{
                  .max_shares_assigned = mgr.local().max_shares_assigned(),
                  .total_translation_backlog
                  = mgr.local().total_translation_backlog(),

                });
          },
          std::ref(storage_node),
          ss::sharded_parameter([] {
              return config::shard_local_cfg()
                .max_kafka_throttle_delay_ms.bind();
          }),
          ss::sharded_parameter([] {
              return config::shard_local_cfg().quota_manager_gc_sec.bind();
          }),
          ss::sharded_parameter([] {
              return config::shard_local_cfg()
                .iceberg_throttle_backlog_size_ratio.bind();
          }))
          .get();

        datalake_throttle_manager
          .invoke_on_all(&kafka::datalake_throttle_manager::start)
          .get();
    }

    construct_service(
      _cluster_link_service,
      node_id,
      ss::sharded_parameter([]() {
          return config::shard_local_cfg().enable_shadow_linking.bind();
      }),
      &controller->get_cluster_link_frontend(),
      ss::sharded_parameter([this] {
          return cluster::partition_change_notifier_impl::make_default(
            raft_group_manager,
            partition_manager,
            controller->get_topics_state());
      }),
      &partition_manager,
      &controller->get_partition_leaders(),
      &controller->get_shard_table(),
      &metadata_cache,
      &_connection_cache,
      controller.get(),
      &group_router,
      &snc_quota_mgr,
      &controller->get_health_monitor(),
      &controller->get_security_frontend(),
      &_kafka_data_rpc_client,
      &id_allocator_frontend,
      smp_service_groups.cluster_link_smp_sg(),
      scheduling_groups::instance().cluster_linking_sg())
      .get();

    syschecks::systemd_message("Creating kafka usage manager frontend").get();
    construct_service(
      usage_manager,
      controller.get(),
      std::ref(controller->get_health_monitor()),
      std::ref(storage),
      ss::sharded_parameter(
        [this] { return make_datalake_usage_aggregator(); }))
      .get();

    construct_single_service(_monitor_unsafe, std::ref(feature_table));

    construct_service(_debug_bundle_service, &storage.local().kvs()).get();

    auto data_dir = config::node().data_directory().as_sstring();
    auto cache_dir = ss::sstring(
      config::node().cloud_storage_cache_path().string());
    construct_single_service(
      _host_metrics_watcher, std::ref(_log), data_dir, cache_dir);

    construct_service(_kafka_connections_service, std::ref(_kafka_server.ref()))
      .get();

    configure_admin_server(node_id);
}
