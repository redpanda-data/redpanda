// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "datalake/coordinator/service.h"
#include "kafka/data/rpc/service.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "transform/rpc/service.h"

void application::add_data_rpc_services(
  std::vector<std::unique_ptr<rpc::service>>& runtime_services) {
    runtime_services.push_back(
      std::make_unique<kafka::data::rpc::network_service>(
        scheduling_groups::instance().transforms_sg(),
        smp_service_groups.transform_smp_sg(),
        &_kafka_data_rpc_service));

    if (wasm_data_transforms_enabled()) {
        runtime_services.push_back(
          std::make_unique<transform::rpc::network_service>(
            scheduling_groups::instance().transforms_sg(),
            smp_service_groups.transform_smp_sg(),
            &_transform_rpc_service));
    }

    if (datalake_enabled()) {
        runtime_services.push_back(
          std::make_unique<datalake::coordinator::rpc::service>(
            scheduling_groups::instance().datalake_sg(),
            smp_service_groups.datalake_sg(),
            &_datalake_coordinator_fe));
    }
}
