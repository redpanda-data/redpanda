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
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/producer_state.h"
#include "cluster/rm_stm_types.h"
#include "model/tests/randoms.h"
#include "random/generators.h"
#include "storage/tests/randoms.h"
#include "test_utils/randoms.h"
#include "utils/prefix_logger.h"

#include <iterator>

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
    return {
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
    auto d = tests::random_vector(random_partition_status);
    cluster::partition_statuses_t partitions;
    std::move(d.begin(), d.end(), std::back_inserter(partitions));
    return {model::random_topic_namespace(), std::move(partitions)};
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

    return {
      tests::random_named_int<model::node_id>(),
      node::random_local_state(),
      tests::random_chunked_vector(random_topic_status),
      random_ds};
}

inline cluster_health_report random_cluster_health_report() {
    return cluster_health_report{
      {},
      tests::random_optional(
        [] { return tests::random_named_int<model::node_id>(); }),
      tests::random_vector(random_node_state),
      tests::random_vector([] {
          return ss::make_foreign(
            ss::make_lw_shared<const cluster::node_health_report>(
              random_node_health_report()));
      })};
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

inline cluster::tx::tx_data_snapshot random_tx_data_snapshot() {
    return {
      model::random_producer_identity(),
      tests::random_named_int<model::tx_seq>(),
      tests::random_named_int<model::partition_id>()};
}

inline cluster::tx::expiration_snapshot random_expiration_snapshot() {
    return {
      model::random_producer_identity(),
      tests::random_duration<cluster::tx::duration_type>()};
}

inline cluster::tx::prepare_marker random_prepare_marker() {
    return {
      tests::random_named_int<model::partition_id>(),
      tests::random_named_int<model::tx_seq>(),
      model::random_producer_identity()};
}

inline cluster::tx::abort_index random_abort_index() {
    return tx::abort_index{model::random_offset(), model::random_offset()};
}

inline cluster::tx::deprecated_seq_entry::deprecated_seq_cache_entry
random_seq_cache_entry() {
    return {
      random_generators::get_int<int32_t>(),
      tests::random_named_int<kafka::offset>()};
}

inline cluster::tx::deprecated_seq_entry random_seq_entry() {
    cluster::tx::deprecated_seq_entry entry;
    entry.pid = model::random_producer_identity(),
    entry.seq = random_generators::get_int<int32_t>(),
    entry.last_offset = tests::random_named_int<kafka::offset>(),
    entry.seq_cache = tests::random_circular_buffer(random_seq_cache_entry),
    entry.last_write_timestamp = random_generators::get_int<int64_t>();
    return entry;
}

} // namespace cluster

namespace tests {

inline cluster::tx::producer_ptr random_producer_state(prefix_logger& logger) {
    return ss::make_lw_shared<cluster::tx::producer_state>(
      logger,
      model::producer_identity{
        random_generators::get_int<int64_t>(),
        random_generators::get_int<int16_t>()},
      random_named_int<raft::group_id>(),
      ss::noncopyable_function<void()>{});
}

inline cluster::partition_balancer_status random_balancer_status() {
    return random_generators::random_choice({
      cluster::partition_balancer_status::off,
      cluster::partition_balancer_status::starting,
      cluster::partition_balancer_status::ready,
      cluster::partition_balancer_status::in_progress,
      cluster::partition_balancer_status::stalled,
    });
}

inline cluster::partition_balancer_violations::unavailable_node
random_unavailable_node() {
    return {
      tests::random_named_int<model::node_id>(),
      model::timestamp(random_generators::get_int<int64_t>())};
}

inline cluster::partition_balancer_violations::full_node random_full_node() {
    return {
      tests::random_named_int<model::node_id>(),
      random_generators::get_int<uint32_t>()};
}

inline cluster::partition_balancer_violations
random_partition_balancer_violations() {
    auto random_un_gen = tests::random_vector(
      []() { return random_unavailable_node(); });
    auto random_fn_gen = tests::random_vector(
      []() { return random_full_node(); });
    return {std::move(random_un_gen), std::move(random_fn_gen)};
}

} // namespace tests
