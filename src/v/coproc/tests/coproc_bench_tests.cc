/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_bench_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/types.h"
#include "model/fundamental.h"

#include <seastar/core/when_all.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

using copro_typeid = coproc::registry::type_identifier;

FIXTURE_TEST(test_coproc_router_multi_route, coproc_bench_fixture) {
    /// Topics that coprocessors will consume from
    const model::topic input_one("one");
    const model::topic input_two("two");
    /// Expected topics coprocessors will produce onto
    const model::topic even_mt_one = to_materialized_topic(
      input_one, two_way_split_copro::even);
    const model::topic odd_mt_one = to_materialized_topic(
      input_one, two_way_split_copro::odd);
    const model::topic even_mt_two = to_materialized_topic(
      input_two, two_way_split_copro::even);
    const model::topic odd_mt_two = to_materialized_topic(
      input_two, two_way_split_copro::odd);

    /// Deploy coprocessors and prime v::storage with logs
    log_layout_map inputs = {{input_one, 1}, {input_two, 1}};
    log_layout_map outputs = {
      {even_mt_one, 1}, {odd_mt_one, 1}, {even_mt_two, 1}, {odd_mt_two, 1}};
    setup(inputs).get();

    enable_coprocessors(
      {{.id = 9111,
        .data{
          .tid = copro_typeid::two_way_split_copro,
          .topics = {std::make_pair<>(
            input_one, coproc::topic_ingestion_policy::stored)}}},
       {.id = 4517,
        .data{
          .tid = copro_typeid::two_way_split_copro,
          .topics = {std::make_pair<>(
            input_two, coproc::topic_ingestion_policy::stored)}}}})
      .get();

    /// Run the test
    router_test_plan test_plan{
      .inputs = build_simple_opts(std::move(inputs), 500),
      .outputs = build_simple_opts(std::move(outputs), 250)};
    auto drain_results = start_benchmark(std::move(test_plan)).get();

    /// Expect all 4 partitions to exist and verify the exact number of
    // record  batches accross all
    BOOST_REQUIRE_EQUAL(drain_results.size(), 4);
    for (const auto& [_, size] : drain_results) {
        BOOST_REQUIRE_EQUAL(size, 250);
    }
}

std::size_t total_records_across_partitions(
  const coproc_bench_fixture::result_t& result_set) {
    using value = coproc_bench_fixture::result_t::value_type;
    return std::accumulate(
      result_set.begin(),
      result_set.end(),
      std::size_t(0),
      [](std::size_t acc, const value& vt) { return acc += vt.second; });
}

FIXTURE_TEST(test_coproc_router_giant_fanin, coproc_bench_fixture) {
    const std::size_t n_copros = 50;
    const std::size_t n_partitions = 10;
    const model::topic source_topic("sole_input");
    const model::topic output_topic = to_materialized_topic(
      source_topic, identity_coprocessor::identity_topic);
    std::vector<coproc_test_fixture::deploy> deploys;
    for (uint64_t i = 0; i < n_copros; ++i) {
        deploys.push_back(
          {.id = i,
           .data{
             .tid = copro_typeid::identity_coprocessor,
             .topics = {std::make_pair<>(
               source_topic, coproc::topic_ingestion_policy::stored)}}});
    }
    log_layout_map inputs = {{source_topic, n_partitions}};
    log_layout_map outputs = {{output_topic, n_partitions}};
    setup(inputs).get();
    enable_coprocessors(std::move(deploys)).get();

    router_test_plan test_plan{
      .inputs = build_simple_opts(std::move(inputs), 50),
      .outputs = build_simple_opts(std::move(outputs), 50 * n_copros)};
    auto consume_results = start_benchmark(std::move(test_plan)).get();
    const std::size_t expected_record_batches = 50 * n_copros * n_partitions;
    BOOST_CHECK_EQUAL(
      total_records_across_partitions(consume_results),
      expected_record_batches);
}

FIXTURE_TEST(test_coproc_router_giant_one_to_many, coproc_bench_fixture) {
    const std::size_t n_copros = 25;
    const std::size_t n_partitions = 5;
    const model::topic source_topic("input");
    std::vector<coproc_test_fixture::deploy> deploys;
    for (uint64_t i = 0; i < n_copros; ++i) {
        deploys.push_back(
          {.id = i,
           .data{
             .tid = copro_typeid::unique_identity_coprocessor,
             .topics = {std::make_pair<>(
               source_topic, coproc::topic_ingestion_policy::stored)}}});
    }
    log_layout_map inputs = {{source_topic, n_partitions}};
    log_layout_map outputs;
    for (std::size_t i = 0; i < n_copros; ++i) {
        auto materialized_topic = to_materialized_topic(
          source_topic, model::topic(ssx::sformat("identity_topic_{}", i)));
        outputs.emplace(materialized_topic, n_partitions);
    }
    setup(inputs).get();
    enable_coprocessors(std::move(deploys)).get();

    router_test_plan test_plan{
      .inputs = build_simple_opts(std::move(inputs), 50),
      .outputs = build_simple_opts(std::move(outputs), 50)};
    auto consume_results = start_benchmark(std::move(test_plan)).get();
    const std::size_t expected_record_batches = 50 * n_copros * n_partitions;
    BOOST_CHECK_EQUAL(
      total_records_across_partitions(consume_results),
      expected_record_batches);
}
