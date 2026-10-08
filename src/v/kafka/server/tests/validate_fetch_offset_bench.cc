/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/vassert.h"
#include "cluster/partition.h"
#include "kafka/data/partition_proxy.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/testing/perf_tests.hh>

using namespace std::chrono_literals; // NOLINT

// Microbenchmarks for the offset validation step of the fetch path. Every
// fetched partition runs partition_proxy::validate_fetch_offset before any
// data is read, so its steady-state cost (leader, eviction stm already synced
// within the current term) is pure per-fetch overhead. It is measured
// directly here to catch regressions that whole-fetch benchmarks would hide
// in the noise.

struct validate_fetch_offset_fixture : redpanda_thread_fixture {
    static constexpr size_t topic_name_length = 30;

    validate_fetch_offset_fixture() {
        wait_for_controller_leadership().get();

        // Disable as many background processes as possible to reduce noise.
        test_local_cfg.get("log_disable_housekeeping_for_tests")
          .set_value(true);
        test_local_cfg.get("disable_cluster_recovery_loop_for_tests")
          .set_value(true);
        test_local_cfg.get("enable_metrics_reporter").set_value(false);
    }

    ss::future<model::ntp> initialize_single_partition_topic() {
        auto t = model::topic(
          random_generators::gen_alphanum_string(topic_name_length));
        co_await add_topic(
          model::topic_namespace_view(model::kafka_namespace, t), 1);
        auto ntp = make_default_ntp(t, model::partition_id(0));
        co_await wait_for_leader(ntp);
        co_return ntp;
    }

    scoped_config test_local_cfg;
};

namespace {
constexpr size_t num_calls_per_run = 1000;
} // namespace

PERF_TEST_CN(validate_fetch_offset_fixture, leader_steady_state) {
    static model::ntp ntp = co_await initialize_single_partition_topic();

    auto partition = app.partition_manager.local().get(ntp);
    vassert(partition, "partition {} must exist", ntp);
    auto proxy = kafka::make_partition_proxy(partition);

    auto deadline = model::timeout_clock::now() + 10min;

    // The first call after a leadership change takes the suspending slow path
    // (it syncs the log eviction stm and, with archival enabled, the archival
    // stm). Do it outside the measured region so the measurement covers the
    // steady state that every subsequent fetch hits.
    auto warmup_ec = co_await proxy.validate_fetch_offset(
      model::offset(0), false, deadline);
    vassert(
      warmup_ec == kafka::error_code::none, "unexpected error: {}", warmup_ec);

    // Drain task queue before running the measured region in order to
    // reduce noise from unrelated tasks.
    co_await tests::drain_task_queue();

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < num_calls_per_run; i++) {
        auto ec = co_await proxy.validate_fetch_offset(
          model::offset(0), false, deadline);
        perf_tests::do_not_optimize(ec);
    }
    perf_tests::stop_measuring_time();

    co_return num_calls_per_run;
}

// The dominant historical cost inside validate_fetch_offset:
// partition::sync_kafka_start_offset_override. Measured on its own to
// pinpoint regressions in this specific call.
PERF_TEST_CN(validate_fetch_offset_fixture, sync_start_offset_override) {
    static model::ntp ntp = co_await initialize_single_partition_topic();

    auto partition = app.partition_manager.local().get(ntp);
    vassert(partition, "partition {} must exist", ntp);

    auto warmup_res = co_await partition->sync_kafka_start_offset_override(5s);
    vassert(!warmup_res.has_failure(), "sync must succeed on the leader");

    // Drain task queue before running the measured region in order to
    // reduce noise from unrelated tasks.
    co_await tests::drain_task_queue();

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < num_calls_per_run; i++) {
        auto res = co_await partition->sync_kafka_start_offset_override(5s);
        perf_tests::do_not_optimize(res);
    }
    perf_tests::stop_measuring_time();

    co_return num_calls_per_run;
}
