// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/configuration.h"
#include "cloud_storage/inventory/inv_ops.h"
#include "cloud_storage/inventory/types.h"
#include "cloud_storage_clients/configuration.h"
#include "cluster/controller.h"
#include "cluster/inventory_service.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "redpanda/application.h"
#include "resource_mgmt/storage.h"
#include "syschecks/syschecks.h"

void application::wire_up_services_cloud_inventory(
  std::optional<cloud_storage_clients::bucket_name>& bucket_name) {
    if (
      !archival_storage_enabled()
      || !config::shard_local_cfg()
            .cloud_storage_inventory_based_scrub_enabled()
      || !config::shard_local_cfg().cloud_storage_enable_scrubbing()) {
        return;
    }

    auto cloud_config = cloud_storage::configuration::get_config().get();
    auto backend = cloud_storage_clients::infer_backend_from_configuration(
      cloud_config.client_config, cloud_config.cloud_credentials_source);

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
      = std::make_shared<cluster::default_remote_provider>(cloud_storage_api);
    auto inv_ops
      = cloud_storage::inventory::make_inv_ops(
          bucket_name.value(),
          cloud_storage::inventory::inventory_config_id{
            config::shard_local_cfg().cloud_storage_inventory_id()},
          config::shard_local_cfg().cloud_storage_inventory_reports_prefix())
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
