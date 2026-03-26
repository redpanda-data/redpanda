// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "cluster/data_migration_service_handler.h"
#include "cluster/migrations/tx_manager_migrator_handler.h"
#include "cluster/topic_recovery_status_rpc_handler.h"
#include "config/node_config.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"

void application::add_cluster_rpc_services_3(
  std::vector<std::unique_ptr<rpc::service>>& runtime_services) {
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
}
