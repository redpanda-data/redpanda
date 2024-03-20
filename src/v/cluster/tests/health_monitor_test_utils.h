// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/health_monitor_backend.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

namespace cluster {
// needed only for access to private members
struct health_report_accessor {
    // the original cap on leaderless and urp partitions
    static constexpr size_t original_limit = 128;

    using aggregated_report = health_monitor_backend::aggregated_report;
    using report_cache_t = health_monitor_backend::report_cache_t;

    static auto aggregate(report_cache_t& reports) {
        return health_monitor_backend::aggregate_reports(reports);
    }
};

chunked_vector<cluster::partition_status>
make_partition_statues(size_t num_partitions) {
    chunked_vector<cluster::partition_status> ret;
    if (num_partitions == 0) {
        return ret;
    }
    ret.reserve(num_partitions);

    for (size_t i = 0; i < num_partitions; ++i) {
        cluster::partition_status part_status;

        part_status.id = model::partition_id(i);
        part_status.leader_id = tests::random_optional(
          []() { return model::node_id(random_generators::get_int(20)); });
        part_status.reclaimable_size_bytes = random_generators::get_int<size_t>(
          20000000);
        part_status.revision_id = model::revision_id(
          random_generators::get_int<size_t>(100));
        part_status.term = model::term_id(
          random_generators::get_int<size_t>(200));
        part_status.size_bytes = random_generators::get_int<size_t>(20000000);
        part_status.under_replicated_replicas = tests::random_optional(
          [] { return random_generators::get_int<uint8_t>(5); });
        if (tests::random_bool()) {
            part_status.shard = random_generators::get_int<uint32_t>(128);
        } else {
            part_status.shard = uint32_t(-1);
        }

        ret.push_back(part_status);
    }

    return ret;
}

static const std::vector<model::ns> namespaces{
  model::kafka_namespace, model::kafka_internal_namespace, model::redpanda_ns};

model::topic_namespace make_tp_ns() {
    return {
      random_generators::random_choice(namespaces),
      model::topic("topic-" + random_generators::gen_alphanum_string(10))};
}

cluster::topic_status make_topic_status(size_t num_partitions) {
    cluster::topic_status ts;
    ts.tp_ns = make_tp_ns();
    ts.partitions = make_partition_statues(num_partitions);
    return ts;
}

columnar_node_health_report make_columnar_node_health_report(
  size_t num_topics, size_t partitions_per_topic) {
    columnar_node_health_report report;
    report.id = model::node_id(1);

    report.local_state.redpanda_version = cluster::node::application_version(
      "v23.3.1");
    report.local_state.logical_version = cluster::cluster_version(10);
    report.local_state.uptime = std::chrono::milliseconds(100);

    report.local_state.data_disk.path
      = "/bar/baz/foo/foo/foo/foo/foo/foo/foo/bar";
    report.local_state.data_disk.total = 1000;
    report.local_state.data_disk.free = 500;

    for (size_t i = 0; i < num_topics; ++i) {
        report.topics.append(
          make_tp_ns(), make_partition_statues(partitions_per_topic));
    }

    return report;
}

} // namespace cluster
