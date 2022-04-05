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

#include "seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>

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
        uint32_t coproc_group_max_non_local_requests
          = default_max_nonlocal_requests;
        uint32_t proxy_group_max_non_local_requests
          = default_max_nonlocal_requests;
    };

    smp_groups() = default;
    ss::future<> create_groups(config cfg) {
        _raft = co_await create_service_group(
          cfg.raft_group_max_non_local_requests);
        _kafka = co_await create_service_group(
          cfg.kafka_group_max_non_local_requests);
        _cluster = co_await create_service_group(
          cfg.cluster_group_max_non_local_requests);
        _coproc = co_await create_service_group(
          cfg.coproc_group_max_non_local_requests);
        _proxy = co_await create_service_group(
          cfg.proxy_group_max_non_local_requests);
    }

    ss::smp_service_group raft_smp_sg() { return *_raft; }
    ss::smp_service_group kafka_smp_sg() { return *_kafka; }
    ss::smp_service_group cluster_smp_sg() { return *_cluster; }
    ss::smp_service_group coproc_smp_sg() { return *_coproc; }
    ss::smp_service_group proxy_smp_sg() { return *_proxy; }

    ss::future<> destroy_groups() {
        return destroy_smp_service_group(*_kafka)
          .then([this] { return destroy_smp_service_group(*_raft); })
          .then([this] { return destroy_smp_service_group(*_cluster); })
          .then([this] { return destroy_smp_service_group(*_coproc); })
          .then([this] { return destroy_smp_service_group(*_proxy); });
    }

private:
    ss::future<std::unique_ptr<ss::smp_service_group>>
    create_service_group(unsigned max_non_local_requests) {
        ss::smp_service_group_config smp_sg_config{
          .max_nonlocal_requests = max_non_local_requests};
        auto sg = co_await create_smp_service_group(smp_sg_config);

        co_return std::make_unique<ss::smp_service_group>(sg);
    }

    std::unique_ptr<ss::smp_service_group> _raft;
    std::unique_ptr<ss::smp_service_group> _kafka;
    std::unique_ptr<ss::smp_service_group> _cluster;
    std::unique_ptr<ss::smp_service_group> _coproc;
    std::unique_ptr<ss::smp_service_group> _proxy;
};
