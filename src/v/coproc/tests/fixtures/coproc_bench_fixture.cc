/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/fixtures/coproc_bench_fixture.h"

#include <chrono>

coproc_bench_fixture::router_test_plan::input_type
coproc_bench_fixture::build_simple_opts(
  log_layout_map data, std::size_t batch_size, std::size_t n_batches) {
    using rtp = coproc_bench_fixture::router_test_plan;
    rtp::input_type results;
    for (auto& [topic, n_partitions] : data) {
        for (decltype(n_partitions) i = 0; i < n_partitions; ++i) {
            model::ntp ntp(
              model::kafka_namespace, topic, model::partition_id(i));
            results.emplace(
              std::move(ntp),
              rtp::options{
                .batch_size = batch_size, .number_of_batches = n_batches});
        }
    }
    return results;
}

absl::flat_hash_set<model::ntp>
coproc_bench_fixture::expand(log_layout_map llm) {
    absl::flat_hash_set<model::ntp> ntps;
    for (auto& [topic, n_partitions] : llm) {
        for (decltype(n_partitions) i = 0; i < n_partitions; ++i) {
            ntps.emplace(model::kafka_namespace, topic, model::partition_id(i));
        }
    }
    return ntps;
}

ss::future<absl::flat_hash_map<model::ntp, std::size_t>>
coproc_bench_fixture::start_benchmark(router_test_plan plan) {
    co_await push_all(std::move(plan.inputs));
    co_return co_await consume_all(std::move(plan.outputs));
}

ss::future<> coproc_bench_fixture::push_all(
  absl::flat_hash_map<model::ntp, router_test_plan::options> inputs) {
    for (const auto& [ntp, opts] : inputs) {
        co_await produce(
          ntp,
          storage::test::make_random_memory_record_batch_reader(
            model::offset{0}, opts.batch_size, opts.number_of_batches, false));
    }
}

ss::future<absl::flat_hash_map<model::ntp, std::size_t>>
coproc_bench_fixture::consume_all(absl::flat_hash_set<model::ntp> outputs) {
    absl::flat_hash_map<model::ntp, std::size_t> results;
    for (auto& ntp : outputs) {
        auto timeout = model::timeout_clock::now() + std::chrono::minutes(1);
        auto data = co_await consume(
          ntp,
          model::offset{0},
          model::model_limits<model::offset>::max(),
          timeout);
        results.emplace(std::move(ntp), data.size());
    }
    co_return results;
}
