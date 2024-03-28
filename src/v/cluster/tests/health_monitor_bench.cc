// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_types.h"
#include "cluster/tests/health_monitor_test_utils.h"
#include "container/fragmented_vector.h"
#include "model/metadata.h"

#include <seastar/testing/perf_tests.hh>
using namespace cluster;

cluster::node_health_report
make_node_health_report(size_t num_topics, size_t partitions_per_topic) {
    cluster::node_health_report hr;
    hr.id = model::node_id(1);

    hr.local_state.redpanda_version = cluster::node::application_version(
      "v21.1.1");
    hr.local_state.logical_version = cluster::cluster_version(1);
    hr.local_state.uptime = std::chrono::milliseconds(100);

    hr.local_state.data_disk.path = "/bar/baz/foo/foo/foo/foo/foo/foo/foo/bar";
    hr.local_state.data_disk.total = 1000;
    hr.local_state.data_disk.free = 500;

    for (size_t i = 0; i < num_topics; ++i) {
        hr.topics.push_back(make_topic_status(partitions_per_topic));
    }

    return hr;
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
PERF_TEST(columnar_node_health_report, serialize_many_partitions) {
    bench_serialize_node_health_report(
      10, 5000, make_columnar_node_health_report);
}

PERF_TEST(columnar_node_health_report, serialize_many_topics) {
    bench_serialize_node_health_report(
      50000, 1, make_columnar_node_health_report);
}

PERF_TEST(
  columnar_node_health_report, serialize_many_topics_replicated_partitions) {
    bench_serialize_node_health_report(
      50000, 3, make_columnar_node_health_report);
}

template<typename T>
[[gnu::noinline]] T do_bench_deserialize_node_health_report(iobuf buf) {
    return serde::from_iobuf<T>(std::move(buf));
}
template<typename GenFunc>
void bench_deserialize_node_health_report(
  size_t num_topics, size_t partitions_per_topic, GenFunc& f) {
    auto hr = f(num_topics, partitions_per_topic);
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

PERF_TEST(columnar_node_health_report, deserialize_many_partitions) {
    bench_deserialize_node_health_report(
      10, 5000, make_columnar_node_health_report);
}

PERF_TEST(columnar_node_health_report, deserialize_many_topics) {
    bench_deserialize_node_health_report(
      50000, 1, make_columnar_node_health_report);
}

PERF_TEST(
  columnar_node_health_report, deserialize_many_topics_replicated_partitions) {
    bench_deserialize_node_health_report(
      50000, 3, make_columnar_node_health_report);
}

template<typename GenFunc>
size_t bench_iterate_health_report(
  size_t num_topics, size_t partitions_per_topic, GenFunc& f) {
    auto hr = f(num_topics, partitions_per_topic);

    perf_tests::start_measuring_time();
    for (const auto& t : hr.topics) {
        for (const auto& p : t.partitions) {
            perf_tests::do_not_optimize(p);
        }
    }
    perf_tests::stop_measuring_time();
    return num_topics * partitions_per_topic;
}

PERF_TEST(node_health_report, iterate_health_report_many_partitions) {
    return bench_iterate_health_report(30, 5000, make_node_health_report);
}
PERF_TEST(columnar_node_health_report, iterate_health_report_many_partitions) {
    return bench_iterate_health_report(
      30, 5000, make_columnar_node_health_report);
}

PERF_TEST(node_health_report, iterate_health_report_many_topics) {
    return bench_iterate_health_report(50000, 3, make_node_health_report);
}

PERF_TEST(columnar_node_health_report, iterate_health_report_many_topics) {
    return bench_iterate_health_report(
      50000, 3, make_columnar_node_health_report);
}
