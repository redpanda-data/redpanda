/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/fixtures/coproc_bench_fixture.h"

#include "coproc/tests/utils/batch_utils.h"

#include <chrono>

coproc_bench_fixture::router_test_plan::plan_t
coproc_bench_fixture::build_simple_opts(
  log_layout_map data, std::size_t records_per_partition) {
    using rtp = coproc_bench_fixture::router_test_plan;
    rtp::plan_t results;
    for (auto& [topic, n_partitions] : data) {
        for (decltype(n_partitions) i = 0; i < n_partitions; ++i) {
            model::ntp ntp(
              model::kafka_namespace, topic, model::partition_id(i));
            results.emplace(std::move(ntp), records_per_partition);
        }
    }
    return results;
}

ss::future<coproc_bench_fixture::result_t>
coproc_bench_fixture::start_benchmark(router_test_plan plan) {
    co_await push_all(std::move(plan.inputs));
    co_return co_await consume_all(std::move(plan.outputs));
}

ss::future<> coproc_bench_fixture::push_all(router_test_plan::plan_t inputs) {
    for (const auto& [ntp, records_per_partition] : inputs) {
        co_await produce(ntp, make_random_batch(records_per_partition));
    }
}

ss::future<coproc_bench_fixture::result_t>
coproc_bench_fixture::consume_all(router_test_plan::plan_t outputs) {
    result_t results;
    for (auto& [ntp, limit] : outputs) {
        auto timeout = model::timeout_clock::now() + std::chrono::minutes(1);
        auto data = co_await consume(ntp, limit, model::offset{0}, timeout);
        results.emplace(std::move(ntp), num_records(data));
    }
    co_return results;
}
