/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/tests/randoms.h"
#include "compat/cluster_generator.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "test_utils/randoms.h"

namespace compat {

EMPTY_COMPAT_GENERATOR(cluster::partition_balancer_overview_request);

template<>
struct instance_generator<cluster::change_reason> {
    static cluster::change_reason random() {
        return random_generators::random_choice({
          cluster::change_reason::rack_constraint_repair,
          cluster::change_reason::partition_count_rebalancing,
          cluster::change_reason::node_decommissioning,
          cluster::change_reason::node_unavailable,
          cluster::change_reason::disk_full,
        });
    }

    static std::vector<cluster::change_reason> limits() { return {}; }
};
template<>
struct instance_generator<cluster::reallocation_error> {
    static cluster::reallocation_error random() {
        return random_generators::random_choice({
          cluster::reallocation_error::missing_partition_size_info,
          cluster::reallocation_error::no_eligible_node_found,
          cluster::reallocation_error::over_partition_fd_limit,
          cluster::reallocation_error::over_partition_memory_limit,
          cluster::reallocation_error::over_partition_core_limit,
          cluster::reallocation_error::no_quorum,
          cluster::reallocation_error::reconfiguration_in_progress,
          cluster::reallocation_error::partition_disabled,
          cluster::reallocation_error::unknown_error,
        });
    }

    static std::vector<cluster::reallocation_error> limits() { return {}; }
};

template<>
struct instance_generator<cluster::partition_balancer_overview_reply> {
    static cluster::partition_balancer_overview_reply random() {
        auto generator = [] {
            return std::make_pair(
              tests::random_named_int<model::node_id>(),
              tests::random_btree_set<model::ntp>(model::random_ntp));
        };

        auto failure_details_generator = [] {
            return std::make_pair(
              model::random_ntp(),
              cluster::reallocation_failure_details{
                .replica_to_move = tests::random_named_int<model::node_id>(),
                .reason = instance_generator<cluster::change_reason>::random(),
                .error
                = instance_generator<cluster::reallocation_error>::random(),
              });
        };
        cluster::partition_balancer_overview_reply reply;

        reply.error = instance_generator<cluster::errc>::random(),
        reply.last_tick_time = model::timestamp(
          random_generators::get_int<int64_t>()),
        reply.status = tests::random_balancer_status(),
        reply.violations = tests::random_partition_balancer_violations(),
        reply.decommission_realloc_failures = tests::
          random_flat_hash_map<model::node_id, absl::btree_set<model::ntp>>(
            std::move(generator)),
        reply.partitions_pending_force_recovery_count
          = random_generators::get_int<size_t>(),
        reply.partitions_pending_force_recovery_sample = tests::random_vector(
          model::random_ntp);
        reply.reallocation_failures = tests::random_chunked_hash_map<
          model::ntp,
          cluster::reallocation_failure_details>(
          std::move(failure_details_generator));
        return reply;
    }

    static std::vector<cluster::partition_balancer_overview_reply> limits() {
        return {};
    }
};

} // namespace compat
