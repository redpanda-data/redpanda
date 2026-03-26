// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "cluster/ephemeral_credential_service.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/node_status_rpc_handler.h"
#include "cluster/partition_balancer_rpc_handler.h"
#include "cluster/self_test_rpc_handler.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"

void application::add_cluster_rpc_services_2(
  std::vector<std::unique_ptr<rpc::service>>& runtime_services) {
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
}
