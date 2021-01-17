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

size_t number_of_logs(redpanda_thread_fixture* rtf) {
    return rtf->app.storage
      .map_reduce0(
        [](storage::api& api) { return api.log_mgr().size(); },
        size_t(0),
        std::plus<>())
      .get0();
}

using namespace std::literals;

FIXTURE_TEST(test_coproc_router_no_results, router_test_fixture) {
    // Note the original number of logs
    const size_t n_logs = number_of_logs(this);
    // Router has 2 coprocessors, one subscribed to 'foo' the other 'bar'
    add_copro<null_coprocessor>(2222, {{"bar", l}}).get();
    add_copro<null_coprocessor>(7777, {{"foo", l}}).get();
    // Storage has 10 ntps, 8 of topic 'bar' and 2 of 'foo'
    startup({{make_ts("foo"), 2}, {make_ts("bar"), 8}}).get();

    // Test -> Start pushing to registered topics and check that NO
    // materialized logs have been created
    model::ntp input_ntp(
      model::kafka_namespace, model::topic("foo"), model::partition_id(0));
    auto batches = storage::test::make_random_batches(
      model::offset(0), 4, false);
    push(input_ntp, model::make_memory_record_batch_reader(std::move(batches)))
      .get();

    // Wait for any side-effects, ...expecting that none occur
    ss::sleep(1s).get();
    // Expecting 10, because "foo(2)" and "bar(8)" were loaded at startup
    BOOST_REQUIRE_EQUAL((number_of_logs(this) - n_logs), 10);
}

FIXTURE_TEST(test_coproc_router_simple, router_test_fixture) {
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

    auto batches = storage::test::make_random_batches(
      model::offset(0), 100, false);

    auto f1 = push(
      input_ntp, model::make_memory_record_batch_reader(std::move(batches)));
    auto f2 = drain(output_ntp, 100, model::timeout_clock::now() + 5s);
    auto read_batches
      = ss::when_all_succeed(std::move(f1), std::move(f2)).get();

    /// The identity coprocessor should not have modified the number of record
    /// batches from the source log onto the output log
    BOOST_REQUIRE(std::get<1>(read_batches).has_value());
    const model::record_batch_reader::data_t& data = *std::get<1>(read_batches);
    BOOST_CHECK_EQUAL(data.size(), 100);
}

FIXTURE_TEST(test_coproc_router_multi_route, router_test_fixture) {
    // Create and initialize the environment
    // Starts with 4 parititons of logs managing topic "sole_input"
    const model::topic tt("sole_input");
    const std::size_t n_partitions = 4;
    // and one coprocessor that transforms this topic
    add_copro<two_way_split_copro>(9111, {{"sole_input", l}}).get();
    startup({{make_ts(tt), n_partitions}}).get();

    // Iterating over all ntps, create random data and push them onto
    // their respective logs
    std::vector<ss::future<model::offset>> pushes;
    pushes.reserve(4);
    for (auto i = 0; i < n_partitions; ++i) {
        model::ntp new_ntp(model::kafka_namespace, tt, model::partition_id(i));
        model::record_batch_reader::data_t data
          = storage::test::make_random_batches(model::offset(0), 4, false);
        pushes.emplace_back(push(
          std::move(new_ntp),
          model::make_memory_record_batch_reader(std::move(data))));
    }

    // Knowing what the apply::two_way_split method will do, setup listeners
    // for the materialized topics that are known to eventually exist
    std::vector<ss::future<opt_reader_data_t>> drains;
    drains.reserve(n_partitions * 2);
    auto timeout = model::timeout_clock::now() + 10s;
    for (auto i = 0; i < n_partitions; ++i) {
        model::ntp even(
          model::kafka_namespace,
          model::to_materialized_topic(tt, model::topic("even")),
          model::partition_id(i));
        model::ntp odd(
          model::kafka_namespace,
          model::to_materialized_topic(tt, model::topic("odd")),
          model::partition_id(i));
        drains.emplace_back(drain(even, 4, timeout));
        drains.emplace_back(drain(odd, 4, timeout));
    }

    // Wait on all asynchronous actions to finish
    uint32_t n_batches = ss::when_all_succeed(drains.begin(), drains.end())
                           .then([](std::vector<opt_reader_data_t> results) {
                               return std::accumulate(
                                 results.begin(),
                                 results.end(),
                                 0,
                                 [](uint32_t acc, const opt_reader_data_t& x) {
                                     return acc + (0 ? !x : x->size());
                                 });
                           })
                           .get0();
    BOOST_CHECK_EQUAL(n_batches, 4 * n_partitions);
}
