// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cloud_metadata/offsets_lookup.h"
#include "cluster/cloud_metadata/offsets_recoverer.h"
#include "cluster/cloud_metadata/offsets_recovery_manager.h"
#include "cluster/cloud_metadata/offsets_recovery_router.h"
#include "cluster/cloud_metadata/producer_id_recovery_manager.h"
#include "cluster/controller.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/node_isolation_watcher.h"
#include "cluster/partition_manager.h"
#include "cluster/rm_partition_frontend.h"
#include "cluster/tx_coordinator_mapper.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/tx_topic_manager.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/data_migration_group_proxy_impl.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/rm_group_frontend.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "syschecks/syschecks.h"

void application::wire_up_services_core_2(model::node_id node_id) {
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
}
