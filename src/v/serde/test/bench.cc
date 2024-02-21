// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "cluster/health_monitor_types.h"
#include "cluster/node/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/serde.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

struct small_t
  : public serde::
      envelope<small_t, serde::version<3>, serde::compat_version<2>> {
    int8_t a = 1;
    // char __a_padding;
    int16_t b = 2;
    int32_t c = 3;
    int64_t d = 4;
};
static_assert(sizeof(small_t) == 16, "one more byte for padding");

PERF_TEST(small, serialize) {
    perf_tests::start_measuring_time();
    auto o = serde::to_iobuf(small_t{});
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}
PERF_TEST(small, deserialize) {
    auto b = serde::to_iobuf(small_t{});
    perf_tests::start_measuring_time();
    auto result = serde::from_iobuf<small_t>(std::move(b));
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

struct big_t
  : public serde::envelope<big_t, serde::version<3>, serde::compat_version<2>> {
    small_t s;
    iobuf data;
};

inline big_t gen_big(size_t data_size, size_t chunk_size) {
    const size_t chunks = data_size / chunk_size;
    big_t ret{.s = small_t{}};
    for (size_t i = 0; i < chunks; ++i) {
        auto c = ss::temporary_buffer<char>(chunk_size);
        ret.data.append(std::move(c));
    }
    return ret;
}

inline void serialize_big(size_t data_size, size_t chunk_size) {
    big_t b = gen_big(data_size, chunk_size);
    auto o = iobuf();
    perf_tests::start_measuring_time();
    serde::write(o, std::move(b));
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

inline void deserialize_big(size_t data_size, size_t chunk_size) {
    big_t b = gen_big(data_size, chunk_size);
    auto o = iobuf();
    serde::write(o, std::move(b));
    perf_tests::start_measuring_time();
    auto result = serde::from_iobuf<big_t>(std::move(o));
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

PERF_TEST(big_1mb, serialize) {
    serialize_big(1 << 20 /*1MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_1mb, deserialize) {
    return deserialize_big(1 << 20 /*1MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_10mb, serialize) {
    serialize_big(10 << 20 /*10MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_10mb, deserialize) {
    return deserialize_big(10 << 20 /*10MB*/, 1 << 15 /*32KB*/);
}

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
        hr.topics.push_back(make_topic_status(i, partitions_per_topic));
    }

    return hr;
}

[[gnu::noinline]] void do_bench_serialize_node_health_report(
  iobuf& buf, cluster::node_health_report& hr) {
    return serde::write(buf, std::move(hr));
}

void bench_serialize_node_health_report(
  size_t num_topics, size_t partitions_per_topic) {
    auto hr = make_node_health_report(num_topics, partitions_per_topic);
    auto buf = iobuf();

    perf_tests::start_measuring_time();
    do_bench_serialize_node_health_report(buf, hr);
    perf_tests::do_not_optimize(buf);
    perf_tests::stop_measuring_time();
}

PERF_TEST(node_health_report, serialize_many_partitions) {
    bench_serialize_node_health_report(10, 5000);
}

PERF_TEST(node_health_report, serialize_many_topics) {
    bench_serialize_node_health_report(50000, 1);
}

PERF_TEST(node_health_report, serialize_many_topics_replicated_partitions) {
    bench_serialize_node_health_report(50000, 3);
}

[[gnu::noinline]] cluster::node_health_report
do_bench_deserialize_node_health_report(iobuf buf) {
    return serde::from_iobuf<cluster::node_health_report>(std::move(buf));
}

void bench_deserialize_node_health_report(
  size_t num_topics, size_t partitions_per_topic) {
    auto hr = make_node_health_report(num_topics, partitions_per_topic);
    auto buf = iobuf();
    serde::write(buf, std::move(hr));

    perf_tests::start_measuring_time();
    auto result = do_bench_deserialize_node_health_report(std::move(buf));
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

PERF_TEST(node_health_report, deserialize_many_partitions) {
    bench_deserialize_node_health_report(10, 5000);
}

PERF_TEST(node_health_report, deserialize_many_topics) {
    bench_deserialize_node_health_report(50000, 1);
}

PERF_TEST(node_health_report, deserialize_many_topics_replicated_partitions) {
    bench_deserialize_node_health_report(50000, 3);
}
