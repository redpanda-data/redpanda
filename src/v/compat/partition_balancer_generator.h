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
#include "compat/cluster_generator.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "test_utils/randoms.h"

namespace compat {

EMPTY_COMPAT_GENERATOR(cluster::partition_balancer_overview_request);

template<>
struct instance_generator<cluster::partition_balancer_overview_reply> {
    static cluster::partition_balancer_overview_reply random() {
        auto generator = [] {
            return std::make_pair(
              tests::random_named_int<model::node_id>(),
              tests::random_btree_set<model::ntp>(model::random_ntp));
        };
        return {
          .error = instance_generator<cluster::errc>::random(),
          .last_tick_time = model::timestamp(
            random_generators::get_int<int64_t>()),
          .status = tests::random_balancer_status(),
          .violations = tests::random_partition_balancer_violations(),
          .decommission_realloc_failures = tests::
            random_flat_hash_map<model::node_id, absl::btree_set<model::ntp>>(
              std::move(generator)),
          .partitions_pending_force_recovery_count
          = random_generators::get_int<size_t>(),
          .partitions_pending_force_recovery_sample = tests::random_vector(
            model::random_ntp)};
    }

    static std::vector<cluster::partition_balancer_overview_reply> limits() {
        return {{}};
    }
};

} // namespace compat
