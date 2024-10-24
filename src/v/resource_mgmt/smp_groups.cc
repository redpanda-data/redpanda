/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "resource_mgmt/smp_groups.h"

#include <seastar/core/coroutine.hh>

ss::future<> smp_groups::create_groups(config cfg) {
    _raft = co_await create_service_group(
      cfg.raft_group_max_non_local_requests);
    _kafka = co_await create_service_group(
      cfg.kafka_group_max_non_local_requests);
    _cluster = co_await create_service_group(
      cfg.cluster_group_max_non_local_requests);
    _proxy = co_await create_service_group(
      cfg.proxy_group_max_non_local_requests);
    _transform = co_await create_service_group(
      cfg.transform_group_max_non_local_requests);
    _datalake = co_await create_service_group(
      cfg.datalake_group_max_non_local_requests);
}

ss::future<> smp_groups::destroy_groups() {
    co_await destroy_smp_service_group(*_kafka);
    co_await destroy_smp_service_group(*_raft);
    co_await destroy_smp_service_group(*_cluster);
    co_await destroy_smp_service_group(*_proxy);
    co_await destroy_smp_service_group(*_transform);
    co_await destroy_smp_service_group(*_datalake);
}

uint32_t
smp_groups::default_raft_non_local_requests(uint32_t max_partitions_per_core) {
    /**
     * raft max non local requests
     * - up to 7000 groups per core
     * - up to 256 concurrent append entries per group
     * - additional requests like (vote, snapshot, timeout now)
     *
     * All the values have to be multiplied by core count minus one since
     * part of the requests will be core local
     *
     * 7000*256 * (number of cores-1) + 10 * 7000 * (number of cores-1)
     *         ^                                 ^
     * append entries requests          additional requests
     */

    static constexpr uint32_t max_append_requests_per_follower = 256;
    static constexpr uint32_t additional_requests_per_follower = 10;

    return max_partitions_per_core
           * (max_append_requests_per_follower + additional_requests_per_follower)
           * (ss::smp::count - 1);
}

ss::future<std::unique_ptr<ss::smp_service_group>>
smp_groups::create_service_group(unsigned max_non_local_requests) {
    ss::smp_service_group_config smp_sg_config{
      .max_nonlocal_requests = max_non_local_requests};
    auto sg = co_await create_smp_service_group(smp_sg_config);
    co_return std::make_unique<ss::smp_service_group>(sg);
}
