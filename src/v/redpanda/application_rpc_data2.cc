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
#include "cluster_link/rpc_service.h"
#include "config/configuration.h"
#include "redpanda/admin/proxy/service.h"
#include "redpanda/admin/server.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"

void application::add_data_rpc_services_2(
  std::vector<std::unique_ptr<rpc::service>>& runtime_services) {
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
}
