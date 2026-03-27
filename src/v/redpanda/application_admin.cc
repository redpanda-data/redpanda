// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/app.h"
#include "cluster/controller.h"
#include "config/node_config.h"
#include "redpanda/admin/proxy/client.h"
#include "redpanda/admin/server.h"
#include "redpanda/admin/services/cluster.h"
#include "redpanda/admin/services/datalake/datalake.h"
#include "redpanda/admin/services/internal/breakglass.h"
#include "redpanda/admin/services/internal/debug.h"
#include "redpanda/admin/services/internal/level_zero.h"
#include "redpanda/admin/services/internal/metastore.h"
#include "redpanda/admin/services/internal/shadow_link_internal.h"
#include "redpanda/admin/services/security.h"
#include "redpanda/admin/services/shadow_link/shadow_link.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "syschecks/syschecks.h"

static admin_server_cfg
admin_server_cfg_from_global_cfg(scheduling_groups& sgs) {
    return admin_server_cfg{
      .endpoints = config::node().admin(),
      .endpoints_tls = config::node().admin_api_tls(),
      .admin_api_docs_dir = config::node().admin_api_doc_dir(),
      .sg = sgs.admin_sg(),
      .max_memory_usage_bytes = memory_groups().admin_max_memory(),
    };
}

void application::configure_admin_server(model::node_id node_id) {
    if (config::node().admin().empty()) {
        return;
    }

    syschecks::systemd_message("constructing http server").get();
    construct_service(
      _admin,
      admin_server_cfg_from_global_cfg(scheduling_groups::instance()),
      std::ref(stress_fiber_manager),
      std::ref(partition_manager),
      std::ref(raft_group_manager),
      controller.get(),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(node_status_table),
      std::ref(self_test_frontend),
      std::ref(usage_manager),
      _proxy.get(),
      _schema_registry.get(),
      std::ref(topic_recovery_service),
      std::ref(topic_recovery_status_frontend),
      std::ref(storage_node),
      std::ref(_memory_sampling),
      std::ref(shadow_index_cache),
      std::ref(_cpu_profiler),
      &_transform_service,
      std::ref(audit_mgr),
      std::ref(_tx_manager_migrator),
      std::ref(_kafka_server.ref()),
      std::ref(tx_gateway_frontend),
      std::ref(_debug_bundle_service),
      std::ref(_kafka_connections_service))
      .get();
    _admin
      .invoke_on_all([this, node_id](admin_server& s) {
          auto create_client = [node_id, this]() {
              return admin::proxy::client(node_id, &_connection_cache, [this] {
                  return controller->get_members_table().local().node_ids();
              });
          };
          // Add RPC services
          s.add_service(
            std::make_unique<admin::shadow_link_service_impl>(
              create_client(), &_cluster_link_service, &metadata_cache));
          s.add_service(
            std::make_unique<admin::debug_service_impl>(
              create_client(), stress_fiber_manager));
          s.add_service(
            std::make_unique<admin::datalake_service_impl>(
              create_client(), &_datalake_coordinator_fe));
          s.add_service(
            std::make_unique<admin::cluster_service_impl>(
              create_client(),
              std::ref(_kafka_connections_service),
              controller->get_feature_table()));
          s.add_service(
            std::make_unique<admin::internal::breakglass_service_impl>(
              controller.get()));
          if (cloud_topics_app) {
              s.add_service(
                std::make_unique<admin::metastore_service_impl>(
                  create_client(),
                  cloud_topics_app->get_sharded_replicated_metastore(),
                  &controller->get_topics_state(),
                  &metadata_cache,
                  &controller->get_shard_table(),
                  cloud_topics_app->get_sharded_l1_domain_supervisor(),
                  cloud_topics_app->get_sharded_l1_metastore_router()));
              s.add_service(
                std::make_unique<admin::level_zero_service_impl>(
                  node_id,
                  create_client(),
                  cloud_topics_app->get_level_zero_gc(),
                  &controller->get_members_table(),
                  &controller->get_partition_manager(),
                  &controller->get_partition_leaders(),
                  &controller->get_shard_table()));
          }
          s.add_service(
            std::make_unique<
              admin::internal::shadow_link_internal_service_impl>(
              create_client(), &_cluster_link_service, &metadata_cache));
          s.add_service(
            std::make_unique<admin::security_service_impl>(
              create_client(),
              controller.get(),
              _kafka_server.ref(),
              std::ref(metadata_cache)));
      })
      .get();
}
