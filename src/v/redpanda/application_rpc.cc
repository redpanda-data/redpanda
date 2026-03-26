// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"
#include "rpc/rpc_server.h"

void application::add_runtime_rpc_services(
  rpc::rpc_server& s, bool start_raft_rpc_early) {
    // Cluster RPC services (split across multiple compilation units).
    add_cluster_rpc_services(s, start_raft_rpc_early);

    std::vector<std::unique_ptr<rpc::service>> runtime_services;
    add_cluster_rpc_services_2(runtime_services);
    add_cluster_rpc_services_3(runtime_services);

    // Data RPC services (split across multiple compilation units).
    add_data_rpc_services(runtime_services);
    add_data_rpc_services_2(runtime_services);

    s.add_services(std::move(runtime_services));

    // Done! Disallow unknown method errors.
    s.set_all_services_added();
}
