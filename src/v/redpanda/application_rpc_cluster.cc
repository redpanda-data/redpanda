// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cloud_metadata/offsets_recovery_service.h"
#include "cluster/controller.h"
#include "cluster/id_allocator.h"
#include "cluster/partition_manager.h"
#include "cluster/service.h"
#include "cluster/tx_gateway.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/server/rm_group_frontend.h"
#include "raft/service.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "rpc/rpc_server.h"

void application::add_cluster_rpc_services(
  rpc::rpc_server& server, bool start_raft_rpc_early) {
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

    server.add_services(std::move(runtime_services));
}
