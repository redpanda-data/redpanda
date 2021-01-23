/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/utils/coproc_test_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/helpers.h"
#include "coproc/tests/utils/router_test_fixture.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

ss::future<std::size_t> number_of_logs(redpanda_thread_fixture* rtf) {
    return rtf->app.storage.map_reduce0(
      [](storage::api& api) { return api.log_mgr().size(); },
      std::size_t(0),
      std::plus<>());
}

using namespace std::literals;

FIXTURE_TEST(test_coproc_router_no_results, router_test_fixture) {
    // Note the original number of logs
    const std::size_t n_logs = number_of_logs(this).get0();
    // Router has 2 coprocessors, one subscribed to 'foo' the other 'bar'
    add_copro<null_coprocessor>(2222, {{"bar", l}}).get();
    add_copro<null_coprocessor>(7777, {{"foo", l}}).get();
    // Storage has 10 ntps, 8 of topic 'bar' and 2 of 'foo'
    startup({{make_ts("foo"), 2}, {make_ts("bar"), 8}}).get();

    // Test -> Start pushing to registered topics and check that NO
    // materialized logs have been created
    model::ntp input_ntp(
      model::kafka_namespace, model::topic("foo"), model::partition_id(0));
    push(
      input_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset(0), 40, 1))
      .get();

    // Wait for any side-effects, ...expecting that none occur
    ss::sleep(1s).get();
    // Expecting 10, because "foo(2)" and "bar(8)" were loaded at startup
    const std::size_t final_n_logs = number_of_logs(this).get0() - n_logs;
    BOOST_REQUIRE_EQUAL(final_n_logs, 10);
}

/// Tests a simple case of two coprocessors registered to the same output topic
/// producing onto the same materialized topic. Should not crash and have a
/// doubling effect on output size
FIXTURE_TEST(test_coproc_router_double, router_test_fixture) {
    // Supervisor has 3 registered transforms, of the same type
    add_copro<identity_coprocessor>(8888, {{"foo", l}}).get();
    add_copro<identity_coprocessor>(9159, {{"foo", l}}).get();
    add_copro<identity_coprocessor>(4444, {{"bar", l}}).get();
    // Storage has 5 ntps, 4 of topic 'foo' and 1 of 'bar'
    startup({{make_ts("foo"), 4}, {make_ts("bar"), 1}}).get();

    model::topic src_topic("foo");
    model::ntp input_ntp(
      model::kafka_namespace, src_topic, model::partition_id(0));
    model::ntp output_ntp(
      model::kafka_namespace,
      model::to_materialized_topic(
        src_topic, identity_coprocessor::identity_topic),
      model::partition_id(0));

    auto f1 = push(
      input_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset(0), 50, 1));
    auto f2 = drain(output_ntp, 100);
    auto read_batches
      = ss::when_all_succeed(std::move(f1), std::move(f2)).get();

    /// The identity coprocessor should not have modified the number of record
    /// batches from the source log onto the output log
    BOOST_REQUIRE(std::get<1>(read_batches).has_value());
    const model::record_batch_reader::data_t& data = *std::get<1>(read_batches);
    BOOST_CHECK_EQUAL(data.size(), 100);
}

FIXTURE_TEST(test_coproc_router_multi_route, router_test_fixture) {
    /// Topics that coprocessors will consume from
    const model::topic input_one("one");
    const model::topic input_two("two");
    /// Expected topics coprocessors will produce onto
    const model::topic even_mt_one = model::to_materialized_topic(
      input_one, two_way_split_copro::even);
    const model::topic odd_mt_one = model::to_materialized_topic(
      input_one, two_way_split_copro::odd);
    const model::topic even_mt_two = model::to_materialized_topic(
      input_two, two_way_split_copro::even);
    const model::topic odd_mt_two = model::to_materialized_topic(
      input_two, two_way_split_copro::odd);

    /// Deploy coprocessors and prime v::storage with logs
    add_copro<two_way_split_copro>(9111, {{"one", l}}).get();
    add_copro<two_way_split_copro>(4517, {{"two", l}}).get();
    log_layout_map inputs = {{make_ts(input_one), 1}, {make_ts(input_two), 1}};
    log_layout_map outputs = {
      {make_ts(even_mt_one), 1},
      {make_ts(odd_mt_one), 1},
      {make_ts(even_mt_two), 1},
      {make_ts(odd_mt_two), 1}};
    startup(inputs).get();

    /// Run the test
    router_test_plan test_plan{
      .input = build_simple_opts(inputs, 100),
      .output = build_simple_opts(outputs, 50)};
    auto result_tuple = start_benchmark(std::move(test_plan)).get0();
    const auto& [push_results, drain_results] = result_tuple;

    /// Expect all 4 partitions to exist and verify the exact number of record
    /// batches accross all
    BOOST_REQUIRE_EQUAL(push_results.size(), 2);
    BOOST_REQUIRE_EQUAL(drain_results.size(), 4);
    for (const auto& [_, pair] : drain_results) {
        const auto& [__, n_batches] = pair;
        BOOST_REQUIRE_EQUAL(n_batches, 50);
    }
}

FIXTURE_TEST(test_coproc_router_giant_fanin, router_test_fixture) {
    const std::size_t n_copros = 50;
    const std::size_t n_partitions = 10;
    const model::topic source_topic("sole_input");
    const model::topic output_topic = model::to_materialized_topic(
      source_topic, identity_coprocessor::identity_topic);
    const auto range = boost::irange<std::size_t>(0, n_copros);
    ss::do_for_each(range, [this, source_topic](std::size_t i) {
        return add_copro<identity_coprocessor>(i, {{source_topic(), l}});
    }).get();
    log_layout_map inputs = {{make_ts(source_topic), n_partitions}};
    log_layout_map outputs = {{make_ts(output_topic), n_partitions}};
    startup(inputs).get();

    router_test_plan test_plan{
      .input = build_simple_opts(inputs, 10),
      .output = build_simple_opts(outputs, 500)};
    auto result_tuple = start_benchmark(std::move(test_plan)).get0();
    const auto& [push_results, drain_results] = result_tuple;
    const std::size_t n_record_batches = std::accumulate(
      drain_results.begin(),
      drain_results.end(),
      std::size_t(0),
      [](std::size_t acc, const auto& kv_pair) {
          return acc += kv_pair.second.second;
      });
    const std::size_t expected_record_batches = 10 * n_copros * n_partitions;
    BOOST_CHECK_EQUAL(n_record_batches, expected_record_batches);
}

FIXTURE_TEST(test_coproc_router_giant_one_to_many, router_test_fixture) {
    const std::size_t n_copros = 25;
    const std::size_t n_partitions = 5;
    const model::topic source_topic("input");
    const auto range = boost::irange<std::size_t>(0, n_copros);
    ss::do_for_each(range, [this, source_topic](std::size_t i) {
        return add_copro<unique_identity_coprocessor>(i, {{source_topic(), l}});
    }).get();
    log_layout_map inputs = {{make_ts(source_topic), n_partitions}};
    log_layout_map outputs;
    for (auto i = 0; i < n_copros; ++i) {
        auto materialized_topic = model::to_materialized_topic(
          source_topic, model::topic(fmt::format("identity_topic_{}", i)));
        outputs.emplace(make_ts(materialized_topic), n_partitions);
    }
    startup(inputs).get();

    router_test_plan test_plan{
      .input = build_simple_opts(inputs, 10),
      .output = build_simple_opts(outputs, 10)};
    auto result_tuple = start_benchmark(std::move(test_plan)).get0();
    const auto& [push_results, drain_results] = result_tuple;
    const std::size_t n_record_batches = std::accumulate(
      drain_results.begin(),
      drain_results.end(),
      std::size_t(0),
      [](std::size_t acc, const auto& kv_pair) {
          return acc += kv_pair.second.second;
      });
    const std::size_t expected_record_batches = 10 * n_copros * n_partitions;
    BOOST_CHECK_EQUAL(n_record_batches, expected_record_batches);
}
