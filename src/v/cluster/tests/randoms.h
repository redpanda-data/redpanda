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

#include "cluster/health_monitor_types.h"
#include "model/tests/randoms.h"
#include "random/generators.h"
#include "storage/tests/randoms.h"
#include "test_utils/randoms.h"

namespace cluster::node {

inline local_state random_local_state() {
    auto r = local_state{
      {},
      tests::random_named_string<application_version>(),
      tests::random_named_int<cluster::cluster_version>(),
      tests::random_duration<std::chrono::milliseconds>()};
    r.set_disks({storage::random_disk()});
    return r;
}

} // namespace cluster::node

namespace cluster {

inline errc random_failed_errc() {
    return errc(random_generators::get_int<int16_t>(
      static_cast<int16_t>(errc::notification_wait_timeout),
      static_cast<int16_t>(errc::unknown_update_interruption_error)));
}

inline node_state random_node_state() {
    return node_state{
      {},
      tests::random_named_int<model::node_id>(),
      model::random_membership_state(),
      cluster::alive(tests::random_bool())};
}

inline partition_status random_partition_status() {
    return partition_status{
      {},
      tests::random_named_int<model::partition_id>(),
      tests::random_named_int<model::term_id>(),
      tests::random_optional(
        [] { return tests::random_named_int<model::node_id>(); }),
      tests::random_named_int<model::revision_id>(),
      random_generators::get_int<size_t>()};
}

inline topic_status random_topic_status() {
    return topic_status{
      {},
      model::random_topic_namespace(),
      tests::random_vector(random_partition_status)};
}

inline drain_manager::drain_status random_drain_status() {
    return drain_manager::drain_status{
      {},
      tests::random_bool(),
      tests::random_bool(),
      tests::random_optional(
        [] { return random_generators::get_int<size_t>(); }),
      tests::random_optional(
        [] { return random_generators::get_int<size_t>(); }),
      tests::random_optional(
        [] { return random_generators::get_int<size_t>(); }),
      tests::random_optional(
        [] { return random_generators::get_int<size_t>(); })};
}

inline node_health_report random_node_health_report() {
    auto random_ds = tests::random_optional(
      [] { return random_drain_status(); });

    return node_health_report{
      {},
      tests::random_named_int<model::node_id>(),
      node::random_local_state(),
      tests::random_vector(random_topic_status),
      random_ds.has_value(),
      random_ds};
}

inline cluster_health_report random_cluster_health_report() {
    return cluster_health_report{
      {},
      tests::random_optional(
        [] { return tests::random_named_int<model::node_id>(); }),
      tests::random_vector(random_node_state),
      tests::random_vector(random_node_health_report)};
}

inline partitions_filter random_partitions_filter() {
    auto parition_set_gen = [] {
        return tests::random_node_hash_set<model::partition_id>(
          tests::random_named_int<model::partition_id>);
    };
    auto topic_kv_gen = [&] {
        return std::pair{
          tests::random_named_string<model::topic>(), parition_set_gen()};
    };
    auto topic_map_gen = [&] {
        return tests::random_node_hash_map<
          model::topic,
          partitions_filter::partitions_set_t>(topic_kv_gen);
    };
    auto ns_kv_gen = [&] {
        return std::pair{
          tests::random_named_string<model::ns>(), topic_map_gen()};
    };
    auto ns_map_gen = [&] {
        return tests::
          random_node_hash_map<model::ns, partitions_filter::topic_map_t>(
            ns_kv_gen);
    };

    return partitions_filter{{}, ns_map_gen()};
}

inline node_report_filter random_node_report_filter() {
    return node_report_filter{
      {},
      include_partitions_info(tests::random_bool()),
      random_partitions_filter()};
}

inline cluster_report_filter random_cluster_report_filter() {
    return cluster_report_filter{
      {}, random_node_report_filter(), tests::random_vector([] {
          return tests::random_named_int<model::node_id>();
      })};
}

} // namespace cluster
