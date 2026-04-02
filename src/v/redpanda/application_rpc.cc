// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/app.h"
#include "cloud_topics/level_one/metastore/service.h"
#include "cluster/cloud_metadata/offsets_recovery_service.h"
#include "cluster/controller.h"
#include "cluster/data_migration_service_handler.h"
#include "cluster/ephemeral_credential_service.h"
#include "cluster/id_allocator.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/migrations/tx_manager_migrator_handler.h"
#include "cluster/node_status_rpc_handler.h"
#include "cluster/partition_balancer_rpc_handler.h"
#include "cluster/partition_manager.h"
#include "cluster/self_test_rpc_handler.h"
#include "cluster/service.h"
#include "cluster/topic_recovery_status_rpc_handler.h"
#include "cluster/tx_gateway.h"
#include "cluster_link/rpc_service.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "datalake/coordinator/service.h"
#include "kafka/data/rpc/service.h"
#include "kafka/server/rm_group_frontend.h"
#include "raft/service.h"
#include "redpanda/admin/proxy/service.h"
#include "redpanda/admin/server.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "transform/rpc/service.h"

void application::add_runtime_rpc_services(
  rpc::rpc_server& s, bool start_raft_rpc_early) {
    std::vector<std::unique_ptr<rpc::service>> runtime_services;
    runtime_services.push_back(
      std::make_unique<cluster::cloud_metadata::offsets_recovery_rpc_service>(
        scheduling_groups::instance().archival_upload(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(offsets_lookup),
        std::ref(offsets_recovery_router),
        std::ref(offsets_upload_router)));
    runtime_services.push_back(
      std::make_unique<cluster::id_allocator>(
        scheduling_groups::instance().raft_recv_sg(),
        smp_service_groups.raft_smp_sg(),
        std::ref(id_allocator_frontend)));
    // _rm_group_proxy is wrap around a sharded service with only
    // `.local()' access so it's ok to share without foreign_ptr
    runtime_services.push_back(
      std::make_unique<cluster::tx_gateway>(
        scheduling_groups::instance().raft_recv_sg(),
        smp_service_groups.raft_smp_sg(),
        std::ref(tx_gateway_frontend),
        _rm_group_proxy.get(),
        std::ref(rm_partition_frontend)));

    if (!start_raft_rpc_early) {
        runtime_services.push_back(
          std::make_unique<
            raft::service<cluster::partition_manager, cluster::shard_table>>(
            scheduling_groups::instance().raft_recv_sg(),
            smp_service_groups.raft_smp_sg(),
            scheduling_groups::instance().raft_heartbeats(),
            partition_manager,
            shard_table.local(),
            config::shard_local_cfg().raft_heartbeat_interval_ms(),
            config::node().node_id().value()));
    }

    runtime_services.push_back(
      std::make_unique<cluster::service>(
        scheduling_groups::instance().cluster_sg(),
        smp_service_groups.cluster_smp_sg(),
        controller.get(),
        std::ref(controller->get_topics_frontend()),
        std::ref(controller->get_plugin_frontend()),
        std::ref(controller->get_members_manager()),
        std::ref(metadata_cache),
        std::ref(controller->get_security_frontend()),
        std::ref(controller->get_api()),
        std::ref(controller->get_members_frontend()),
        std::ref(controller->get_config_frontend()),
        std::ref(controller->get_config_manager()),
        std::ref(controller->get_feature_manager()),
        std::ref(controller->get_feature_table()),
        std::ref(controller->get_health_monitor()),
        std::ref(_connection_cache),
        std::ref(controller->get_partition_manager()),
        std::ref(node_status_backend),
        std::ref(controller->get_quota_frontend()),
        std::ref(controller->get_cluster_link_frontend())));
    runtime_services.push_back(
      std::make_unique<cluster::metadata_dissemination_handler>(
        scheduling_groups::instance().cluster_sg(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(controller->get_partition_leaders())));

    runtime_services.push_back(
      std::make_unique<cluster::node_status_rpc_handler>(
        scheduling_groups::instance().raft_heartbeats(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(node_status_backend)));

    runtime_services.push_back(
      std::make_unique<cluster::self_test_rpc_handler>(
        scheduling_groups::instance().raft_heartbeats(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(self_test_backend)));

    runtime_services.push_back(
      std::make_unique<cluster::partition_balancer_rpc_handler>(
        scheduling_groups::instance().cluster_sg(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(controller->get_partition_balancer())));

    runtime_services.push_back(
      std::make_unique<cluster::ephemeral_credential_service>(
        scheduling_groups::instance().cluster_sg(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(controller->get_ephemeral_credential_frontend())));

    runtime_services.push_back(
      std::make_unique<kafka::data::rpc::network_service>(
        scheduling_groups::instance().transforms_sg(),
        smp_service_groups.transform_smp_sg(),
        &_kafka_data_rpc_service,
        kafka::data::rpc::network_service::memory_config{
          .memory = &s.memory(),
          .total = s.cfg.max_service_memory_per_core,
        }));

    if (wasm_data_transforms_enabled()) {
        runtime_services.push_back(
          std::make_unique<transform::rpc::network_service>(
            scheduling_groups::instance().transforms_sg(),
            smp_service_groups.transform_smp_sg(),
            &_transform_rpc_service));
    }

    runtime_services.push_back(
      std::make_unique<cluster::topic_recovery_status_rpc_handler>(
        scheduling_groups::instance().cluster_sg(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(topic_recovery_service)));

    if (config::node().recovery_mode_enabled()) {
        runtime_services.push_back(
          std::make_unique<cluster::tx_manager_migrator_handler>(
            scheduling_groups::instance().cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_partition_manager()),
            std::ref(controller->get_shard_table()),
            std::ref(metadata_cache),
            std::ref(_connection_cache),
            std::ref(controller->get_partition_leaders()),
            config::node().node_id().value(),
            _as.local()));
    }
    runtime_services.push_back(
      std::make_unique<cluster::data_migrations::service_handler>(
        scheduling_groups::instance().cluster_sg(),
        smp_service_groups.cluster_smp_sg(),
        std::ref(controller->get_data_migration_frontend()),
        std::ref(controller->get_data_migration_irpc_frontend()),
        std::ref(controller->get_data_migration_router())));
    if (datalake_enabled()) {
        runtime_services.push_back(
          std::make_unique<datalake::coordinator::rpc::service>(
            scheduling_groups::instance().datalake_sg(),
            smp_service_groups.datalake_sg(),
            &_datalake_coordinator_fe));
    }
    if (config::shard_local_cfg().cloud_topics_enabled() && cloud_topics_app) {
        runtime_services.push_back(
          std::make_unique<cloud_topics::l1::rpc::service>(
            scheduling_groups::instance().cloud_topics_metastore_sg(),
            smp_service_groups.cloud_topics_metastore_smp_sg(),
            cloud_topics_app->get_sharded_l1_metastore_router()));
    }
    runtime_services.push_back(
      std::make_unique<admin::proxy::service_impl>(
        scheduling_groups::instance().admin_sg(),
        smp_service_groups.cluster_smp_sg(),
        [this](serde::pb::rpc::context ctx, iobuf buf) {
            if (!_admin.local_is_initialized()) {
                throw std::runtime_error("admin service is not initialized");
            }
            return _admin.local().handle_rpc_request(
              std::move(ctx), std::move(buf));
        }));

    runtime_services.push_back(
      std::make_unique<cluster_link::rpc::service_impl>(
        scheduling_groups::instance().cluster_sg(),
        smp_service_groups.cluster_smp_sg(),
        _cluster_link_service));

    s.add_services(std::move(runtime_services));

    // Done! Disallow unknown method errors.
    s.set_all_services_added();
}
