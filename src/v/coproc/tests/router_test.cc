/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/router_test_fixture.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "storage/tests/utils/random_batch.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

static const auto e = coproc::topic_ingestion_policy::earliest;
static const auto s = coproc::topic_ingestion_policy::stored;
static const auto l = coproc::topic_ingestion_policy::latest;

using copro_typeid = coproc::registry::type_identifier;

ss::future<std::size_t> number_of_logs(redpanda_thread_fixture* rtf) {
    return rtf->app.storage.map_reduce0(
      [](storage::api& api) { return api.log_mgr().size(); },
      std::size_t(0),
      std::plus<>());
}

FIXTURE_TEST(test_coproc_router_no_results, router_test_fixture) {
    // Note the original number of logs
    const std::size_t n_logs = number_of_logs(this).get0();
    // Storage has 10 ntps, 8 of topic 'bar' and 2 of 'foo'
    model::topic foo("foo");
    model::topic bar("bar");
    setup({{foo, 2}, {bar, 8}}).get();
    // Router has 2 coprocessors, one subscribed to 'foo' the other 'bar'
    enable_coprocessors(
      {{.id = 2222,
        .data{.tid = copro_typeid::null_coprocessor, .topics = {bar}}},
       {.id = 7777,
        .data{.tid = copro_typeid::null_coprocessor, .topics = {foo}}}})
      .get();

    // Test -> Start pushing to registered topics and check that NO
    // materialized logs have been created
    model::ntp input_ntp(model::kafka_namespace, foo, model::partition_id(0));
    push(
      input_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset(0), 40, 1))
      .get();

    // Wait for any side-effects, ...expecting that none occur
    ss::sleep(1s).get();
    // Expecting 10, because "foo(2)" and "bar(8)" were loaded at startup
    const std::size_t final_n_logs = number_of_logs(this).get0() - n_logs;
    /// .. but total should be exactly 11 due to the introducion of the
    /// coprocessor_internal_topic
    BOOST_REQUIRE_EQUAL(final_n_logs, 11);
}

model::record_batch_reader single_record_record_batch_reader() {
    model::record_batch_reader::data_t batches;
    batches.push_back(
      storage::test::make_random_batch(model::offset(0), 1, false));
    return model::make_memory_record_batch_reader(std::move(batches));
}

/// Tests an off-by-one error where producing recordbatches of size 1 on the
/// input log wouldn't produce onto the materialized log
FIXTURE_TEST(test_coproc_router_off_by_one, router_test_fixture) {
    model::topic src_topic("obo");
    model::ntp input_ntp(
      model::kafka_namespace, src_topic, model::partition_id(0));
    model::ntp output_ntp(
      model::kafka_namespace,
      model::to_materialized_topic(
        src_topic, identity_coprocessor::identity_topic),
      model::partition_id(0));
    setup({{src_topic, 1}}).get();
    enable_coprocessors(
      {{.id = 12345678,
        .data{
          .tid = copro_typeid::identity_coprocessor, .topics = {src_topic}}}})
      .get();
    auto fn = [this, input_ntp, output_ntp]() -> ss::future<size_t> {
        return push(input_ntp, single_record_record_batch_reader())
          .then([this, input_ntp, output_ntp](auto) {
              return drain(output_ntp, 1)
                .then([input_ntp, output_ntp](opt_reader_data_t r) {
                    BOOST_CHECK(r);
                    return r->size();
                });
          });
    };
    // Perform push/drain twice
    size_t result = fn()
                      .then([&fn](size_t sz) {
                          return fn().then(
                            [sz](size_t sz2) { return sz + sz2; });
                      })
                      .get0();
    BOOST_CHECK_EQUAL(result, 2);
}

FIXTURE_TEST(test_coproc_router_double, router_test_fixture) {
    model::topic foo("foo");
    model::topic bar("bar");
    // Storage has 5 ntps, 4 of topic 'foo' and 1 of 'bar'
    setup({{foo, 4}, {bar, 1}}).get();
    // Supervisor has 3 registered transforms, of the same type
    enable_coprocessors(
      {{.id = 8888,
        .data{.tid = copro_typeid::identity_coprocessor, .topics = {foo}}},
       {.id = 9159,
        .data{.tid = copro_typeid::identity_coprocessor, .topics = {foo}}}})
      .get();

    model::ntp input_ntp(model::kafka_namespace, foo, model::partition_id(0));
    model::ntp output_ntp(
      model::kafka_namespace,
      model::to_materialized_topic(foo, identity_coprocessor::identity_topic),
      model::partition_id(0));

    auto f1 = push(
      input_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset(0), 50, 1));
    auto f2 = drain(output_ntp, 100);
    auto read_batches
      = ss::when_all_succeed(std::move(f1), std::move(f2)).get();

    /// The identity coprocessor should not have modified the number of
    /// record batches from the source log onto the output log
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
    log_layout_map inputs = {{input_one, 1}, {input_two, 1}};
    log_layout_map outputs = {
      {even_mt_one, 1}, {odd_mt_one, 1}, {even_mt_two, 1}, {odd_mt_two, 1}};
    setup(inputs).get();

    enable_coprocessors(
      {{.id = 9111,
        .data{.tid = copro_typeid::two_way_split_copro, .topics = {input_one}}},
       {.id = 4517,
        .data{
          .tid = copro_typeid::two_way_split_copro, .topics = {input_two}}}})
      .get();

    /// Run the test
    router_test_plan test_plan{
      .input = build_simple_opts(inputs, 50),
      .output = build_simple_opts(outputs, 25)};
    auto result_tuple = start_benchmark(std::move(test_plan)).get0();
    const auto& [push_results, drain_results] = result_tuple;

    /// Expect all 4 partitions to exist and verify the exact number of
    // record  batches accross all
    BOOST_REQUIRE_EQUAL(push_results.size(), 2);
    BOOST_REQUIRE_EQUAL(drain_results.size(), 4);
    for (const auto& [_, pair] : drain_results) {
        const auto& [__, n_batches] = pair;
        BOOST_REQUIRE_EQUAL(n_batches, 25);
    }
}

FIXTURE_TEST(test_coproc_router_giant_fanin, router_test_fixture) {
    const std::size_t n_copros = 50;
    const std::size_t n_partitions = 10;
    const model::topic source_topic("sole_input");
    const model::topic output_topic = model::to_materialized_topic(
      source_topic, identity_coprocessor::identity_topic);
    std::vector<coproc::wasm::event_publisher::deploy> deploys;
    for (uint64_t i = 0; i < n_copros; ++i) {
        deploys.push_back(
          {.id = i,
           .data{
             .tid = copro_typeid::identity_coprocessor,
             .topics = {source_topic}}});
    }
    log_layout_map inputs = {{source_topic, n_partitions}};
    log_layout_map outputs = {{output_topic, n_partitions}};
    setup(inputs).get();
    enable_coprocessors(std::move(deploys)).get();

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
    std::vector<coproc::wasm::event_publisher::deploy> deploys;
    for (uint64_t i = 0; i < n_copros; ++i) {
        deploys.push_back(
          {.id = i,
           .data{
             .tid = copro_typeid::unique_identity_coprocessor,
             .topics = {source_topic}}});
    }
    log_layout_map inputs = {{source_topic, n_partitions}};
    log_layout_map outputs;
    for (std::size_t i = 0; i < n_copros; ++i) {
        auto materialized_topic = model::to_materialized_topic(
          source_topic, model::topic(fmt::format("identity_topic_{}", i)));
        outputs.emplace(materialized_topic, n_partitions);
    }
    setup(inputs).get();
    enable_coprocessors(std::move(deploys)).get();

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
