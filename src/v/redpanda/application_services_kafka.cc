// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "cluster/metadata_dissemination_service.h"
#include "config/configuration.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/service.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/snc_quota_manager.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "security/audit/audit_log_manager.h"
#include "syschecks/syschecks.h"

void application::wire_up_services_kafka_data(model::node_id node_id) {
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
}
