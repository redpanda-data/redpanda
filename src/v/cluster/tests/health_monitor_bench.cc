// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_types.h"
#include "serde/async.h"

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
        part_status.cloud_topic_max_gc_eligible_epoch = 101;
        part_status.revision_id = model::revision_id(1);
        part_status.term = model::term_id(1);
        if (i % 3 == 0) { // leaders
            if (i % 30) { // 10% of leaders to have faulty partitions
                part_status.under_replicated_replicas.emplace(2);
                part_status.followers_stats.emplace(
                  cluster::followers_stats{
                    .in_sync = 2,
                    .out_of_sync = {model::node_id(1), model::node_id(2)},
                    .down = {model::node_id(3), model::node_id(4)}});
            } else {
                part_status.under_replicated_replicas.emplace(0);
                part_status.followers_stats.emplace();
            }
        }

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

    return {
      id,
      local_state,
      std::move(topics),
      /* drain status */ std::nullopt,
      cluster::node_liveness_report{}};
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
void bench_deserialize_node_health_report(
  size_t num_topics, size_t partitions_per_topic) {
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
    bench_deserialize_node_health_report(10, 5000);
}

PERF_TEST(node_health_report, deserialize_many_topics) {
    bench_deserialize_node_health_report(50000, 1);
}

PERF_TEST(node_health_report, deserialize_many_topics_replicated_partitions) {
    bench_deserialize_node_health_report(50000, 3);
}

// --- New dissemination types benchmarks ---

namespace {

using namespace cluster::health;

topic_partition_data_map
make_data_map(size_t num_topics, size_t partitions_per_topic) {
    topic_partition_data_map data;
    for (size_t t = 0; t < num_topics; ++t) {
        auto tp_ns = model::topic_namespace(
          model::ns("foo"), model::topic("bar" + std::to_string(t)));
        chunked_hash_map<model::partition_id, partition_data> parts;
        for (size_t p = 0; p < partitions_per_topic; ++p) {
            partition_data pd;
            pd.size_bytes = 100000 + p;
            pd.high_watermark = kafka::offset(1000 + p);
            pd.reclaimable_size_bytes = 50;
            pd.cloud_topic_max_gc_eligible_epoch = 10;
            parts.emplace(model::partition_id(p), pd);
        }
        data.emplace(tp_ns, std::move(parts));
    }
    return data;
}

topic_partition_metadata_map
make_metadata_map(size_t num_topics, size_t partitions_per_topic) {
    topic_partition_metadata_map meta;
    for (size_t t = 0; t < num_topics; ++t) {
        auto tp_ns = model::topic_namespace(
          model::ns("foo"), model::topic("bar" + std::to_string(t)));
        chunked_hash_map<model::partition_id, partition_metadata> parts;
        for (size_t p = 0; p < partitions_per_topic; ++p) {
            partition_metadata pm;
            pm.term = model::term_id(1);
            pm.leader_id = model::node_id(1);
            pm.revision_id = model::revision_id(1);
            pm.shard = 0;
            if (p % 10 == 0) {
                pm.under_replicated_replicas = 2;
                pm.followers_stats = cluster::followers_stats{
                  .in_sync = 2,
                  .out_of_sync = {model::node_id(1)},
                  .down = {model::node_id(2)}};
            }
            parts.emplace(model::partition_id(p), pm);
        }
        meta.emplace(tp_ns, std::move(parts));
    }
    return meta;
}

topic_partition_metadata_diff
make_metadata_diff(size_t num_topics, size_t partitions_per_topic) {
    topic_partition_metadata_diff diff;
    for (size_t t = 0; t < num_topics; ++t) {
        auto tp_ns = model::topic_namespace(
          model::ns("foo"), model::topic("bar" + std::to_string(t)));
        partition_metadata_diff_list parts;
        // ~5% of partitions have metadata changes
        for (size_t p = 0; p < partitions_per_topic; ++p) {
            if (p % 20 == 0) {
                partition_metadata pm;
                pm.term = model::term_id(2);
                pm.leader_id = model::node_id(3);
                pm.revision_id = model::revision_id(1);
                pm.shard = 1;
                parts.emplace_back(model::partition_id(p), std::move(pm));
            }
        }
        if (!parts.empty()) {
            diff.emplace_back(tp_ns, std::move(parts));
        }
    }
    return diff;
}

health_snapshot make_snapshot(size_t num_topics, size_t partitions_per_topic) {
    health_snapshot snap;
    snap.src_timestamp = approx_timestamp{model::timeout_clock::now()};
    snap.local_state.redpanda_version = cluster::node::application_version(
      "v26.2.1");
    snap.local_state.logical_version = cluster::cluster_version(10);
    snap.local_state.uptime = std::chrono::milliseconds(100);
    snap.local_state.data_disk.path = "/var/lib/redpanda/data";
    snap.local_state.data_disk.total = 1000000000;
    snap.local_state.data_disk.free = 500000000;
    snap.data = make_data_map(num_topics, partitions_per_topic);
    return snap;
}

diff_entry_serde
make_diff_serde(size_t num_topics, size_t partitions_per_topic) {
    diff_entry_serde d;
    d.start = node_health_version(1);
    d.end = node_health_version(2);
    d.snapshot = value_or_foreign<health_snapshot>(
      make_snapshot(num_topics, partitions_per_topic));
    d.metadata_diff = value_or_foreign<topic_partition_metadata_diff>(
      make_metadata_diff(num_topics, partitions_per_topic));
    return d;
}

versioned_report_serde
make_report_serde(size_t num_topics, size_t partitions_per_topic) {
    versioned_report_serde r;
    r.version = node_health_version(1);
    r.snapshot = value_or_foreign<health_snapshot>(
      make_snapshot(num_topics, partitions_per_topic));
    r.metadata = value_or_foreign<topic_partition_metadata_map>(
      make_metadata_map(num_topics, partitions_per_topic));
    return r;
}

struct dissemination_bench {};

ss::future<> bench_serialize_diff(size_t num_topics, size_t parts_per_topic) {
    auto d = make_diff_serde(num_topics, parts_per_topic);
    auto buf = iobuf();
    perf_tests::start_measuring_time();
    co_await serde::write_async(buf, std::move(d));
    perf_tests::do_not_optimize(buf);
    perf_tests::stop_measuring_time();
}

ss::future<> bench_deserialize_diff(size_t num_topics, size_t parts_per_topic) {
    auto d = make_diff_serde(num_topics, parts_per_topic);
    auto buf = iobuf();
    co_await serde::write_async(buf, std::move(d));
    auto parser = iobuf_parser{std::move(buf)};
    perf_tests::start_measuring_time();
    auto result = co_await serde::read_async_nested<diff_entry_serde>(
      parser, 0);
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

ss::future<> bench_serialize_report(size_t num_topics, size_t parts_per_topic) {
    auto r = make_report_serde(num_topics, parts_per_topic);
    auto buf = iobuf();
    perf_tests::start_measuring_time();
    co_await serde::write_async(buf, std::move(r));
    perf_tests::do_not_optimize(buf);
    perf_tests::stop_measuring_time();
}

ss::future<>
bench_deserialize_report(size_t num_topics, size_t parts_per_topic) {
    auto r = make_report_serde(num_topics, parts_per_topic);
    auto buf = iobuf();
    co_await serde::write_async(buf, std::move(r));
    auto parser = iobuf_parser{std::move(buf)};
    perf_tests::start_measuring_time();
    auto result = co_await serde::read_async_nested<versioned_report_serde>(
      parser, 0);
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

} // namespace

PERF_TEST_C(dissemination_bench, serialize_diff_many_partitions) {
    co_await bench_serialize_diff(10, 5000);
}

PERF_TEST_C(dissemination_bench, serialize_diff_many_topics) {
    co_await bench_serialize_diff(50000, 1);
}

PERF_TEST_C(dissemination_bench, deserialize_diff_many_partitions) {
    co_await bench_deserialize_diff(10, 5000);
}

PERF_TEST_C(dissemination_bench, deserialize_diff_many_topics) {
    co_await bench_deserialize_diff(50000, 1);
}

PERF_TEST_C(dissemination_bench, serialize_report_many_partitions) {
    co_await bench_serialize_report(10, 5000);
}

PERF_TEST_C(dissemination_bench, serialize_report_many_topics) {
    co_await bench_serialize_report(50000, 1);
}

PERF_TEST_C(dissemination_bench, deserialize_report_many_partitions) {
    co_await bench_deserialize_report(10, 5000);
}

PERF_TEST_C(dissemination_bench, deserialize_report_many_topics) {
    co_await bench_deserialize_report(50000, 1);
}
