/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/smp.hh>

// manage SMP scheduling groups. These scheduling groups are global, so one
// instance of this class can be created at the top level and passed down into
// any server and any shard that needs to schedule continuations into a given
// group.
class smp_groups {
public:
    static constexpr unsigned default_max_nonlocal_requests = 5000;
    struct config {
        uint32_t kafka_group_max_non_local_requests
          = default_max_nonlocal_requests;
        uint32_t raft_group_max_non_local_requests
          = default_max_nonlocal_requests;
        uint32_t cluster_group_max_non_local_requests
          = default_max_nonlocal_requests;
        uint32_t proxy_group_max_non_local_requests
          = default_max_nonlocal_requests;
        uint32_t transform_group_max_non_local_requests
          = default_max_nonlocal_requests;
        uint32_t datalake_group_max_non_local_requests
          = default_max_nonlocal_requests;
    };

    smp_groups() = default;
    ss::future<> create_groups(config cfg);

    ss::smp_service_group raft_smp_sg() { return *_raft; }
    ss::smp_service_group kafka_smp_sg() { return *_kafka; }
    ss::smp_service_group cluster_smp_sg() { return *_cluster; }
    ss::smp_service_group proxy_smp_sg() { return *_proxy; }
    ss::smp_service_group transform_smp_sg() { return *_transform; }
    ss::smp_service_group datalake_sg() { return *_datalake; }

    ss::future<> destroy_groups();

    static uint32_t
    default_raft_non_local_requests(uint32_t max_partitions_per_core);

private:
    ss::future<std::unique_ptr<ss::smp_service_group>>
    create_service_group(unsigned max_non_local_requests);

    std::unique_ptr<ss::smp_service_group> _raft;
    std::unique_ptr<ss::smp_service_group> _kafka;
    std::unique_ptr<ss::smp_service_group> _cluster;
    std::unique_ptr<ss::smp_service_group> _proxy;
    std::unique_ptr<ss::smp_service_group> _transform;
    std::unique_ptr<ss::smp_service_group> _datalake;
};
