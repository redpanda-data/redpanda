// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_types.h"

#include <seastar/testing/perf_tests.hh>

cluster::topic_status make_topic_status(size_t id, size_t num_partitions) {
    cluster::topic_status ts;
    ts.tp_ns = model::topic_namespace(
      model::ns("foo"), model::topic("bar" + std::to_string(id)));

    for (size_t i = 0; i < num_partitions; ++i) {
        cluster::partition_status part_status;
        part_status.id = model::partition_id(i);
        part_status.leader_id = model::node_id(1);
        part_status.reclaimable_size_bytes = 100;
        part_status.revision_id = model::revision_id(1);
        part_status.term = model::term_id(1);

        ts.partitions.push_back(part_status);
    }

    return ts;
}

cluster::node_health_report_serde
make_node_health_report(size_t num_topics, size_t partitions_per_topic) {
    model::node_id id = model::node_id(1);

    cluster::node::local_state local_state;
    local_state.redpanda_version = cluster::node::application_version(
      "v21.1.1");
    local_state.logical_version = cluster::cluster_version(1);
    local_state.uptime = std::chrono::milliseconds(100);

    local_state.data_disk.path = "/bar/baz/foo/foo/foo/foo/foo/foo/foo/bar";
    local_state.data_disk.total = 1000;
    local_state.data_disk.free = 500;

    chunked_vector<cluster::topic_status> topics;
    for (size_t i = 0; i < num_topics; ++i) {
        topics.push_back(make_topic_status(i, partitions_per_topic));
    }

    return {id, local_state, std::move(topics), std::nullopt};
}

template<typename T>
[[gnu::noinline]] void
do_bench_serialize_node_health_report(iobuf& buf, T& hr) {
    return serde::write(buf, std::move(hr));
}

template<typename GenFunc>
void bench_serialize_node_health_report(
  size_t num_topics, size_t partitions_per_topic, GenFunc& f) {
    auto hr = f(num_topics, partitions_per_topic);
    auto buf = iobuf();

    perf_tests::start_measuring_time();
    do_bench_serialize_node_health_report(buf, hr);
    perf_tests::do_not_optimize(buf);
    perf_tests::stop_measuring_time();
}

PERF_TEST(node_health_report, serialize_many_partitions) {
    bench_serialize_node_health_report(10, 5000, make_node_health_report);
}

PERF_TEST(node_health_report, serialize_many_topics) {
    bench_serialize_node_health_report(50000, 1, make_node_health_report);
}

PERF_TEST(node_health_report, serialize_many_topics_replicated_partitions) {
    bench_serialize_node_health_report(50000, 3, make_node_health_report);
}
template<typename T>
[[gnu::noinline]] T do_bench_deserialize_node_health_report(iobuf buf) {
    return serde::from_iobuf<T>(std::move(buf));
}
template<typename GenFunc>
void bench_deserialize_node_health_report(
  size_t num_topics, size_t partitions_per_topic, GenFunc& f) {
    auto hr = make_node_health_report(num_topics, partitions_per_topic);
    auto buf = iobuf();
    serde::write(buf, std::move(hr));

    perf_tests::start_measuring_time();
    auto result = do_bench_deserialize_node_health_report<decltype(hr)>(
      std::move(buf));
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

PERF_TEST(node_health_report, deserialize_many_partitions) {
    bench_deserialize_node_health_report(10, 5000, make_node_health_report);
}

PERF_TEST(node_health_report, deserialize_many_topics) {
    bench_deserialize_node_health_report(50000, 1, make_node_health_report);
}

PERF_TEST(node_health_report, deserialize_many_topics_replicated_partitions) {
    bench_deserialize_node_health_report(50000, 3, make_node_health_report);
}
