// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "coproc/tests/coprocessor.h"
#include "coproc/tests/router_test_fixture.h"
#include "coproc/tests/two_way_split_copro.h"
#include "coproc/tests/utils.h"
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

FIXTURE_TEST(test_coproc_router_no_results, router_test_fixture) {
    auto client = make_client();
    client.connect().get();
    // Note the original number of logs
    const size_t n_logs = number_of_logs(this);
    // Router has 2 coprocessors, one subscribed to 'foo' the other 'bar'
    add_copro<null_coprocessor>(321, {{"bar", l}}).get();
    add_copro<null_coprocessor>(1234, {{"foo", l}}).get();
    // Storage has 10 ntps, 8 of topic 'bar' and 2 of 'foo'
    startup({{make_ts("foo"), 2}, {make_ts("bar"), 8}}, client)
      .then([&client] { return client.stop(); })
      .get();

    // Test -> Start pushing to registered topics and check that NO
    // materialized logs have been created
    model::ntp input_ntp(
      default_ns, model::topic("foo"), model::partition_id(0));
    auto batches = storage::test::make_random_batches(
      model::offset(0), 4, false);
    const auto n_records = sum_records(batches);
    push(input_ntp, model::make_memory_record_batch_reader(std::move(batches)))
      .get();

    // Wait for any side-effects, ...expecting that none occur
    using namespace std::literals;
    ss::sleep(1s).get();
    // Expecting 10, because "foo(2)" and "bar(8)" were loaded at startup
    BOOST_REQUIRE_EQUAL((number_of_logs(this) - n_logs), 10);
}

FIXTURE_TEST(test_coproc_router_simple, router_test_fixture) {
    auto client = make_client();
    client.connect().get();
    // Supervisor has 3 registered transforms, of the same type
    add_copro<identity_coprocessor>(1234, {{"foo", l}}).get();
    add_copro<identity_coprocessor>(121, {{"foo", l}}).get();
    add_copro<identity_coprocessor>(321, {{"bar", l}}).get();
    // Storage has 5 ntps, 4 of topic 'foo' and 1 of 'bar'
    startup({{make_ts("foo"), 4}, {make_ts("bar"), 1}}, client)
      .then([&client] { return client.stop(); })
      .get();

    model::topic src_topic("foo");
    model::ntp input_ntp(default_ns, src_topic, model::partition_id(0));
    model::ntp output_ntp(
      default_ns,
      model::to_materialized_topic(
        src_topic, identity_coprocessor::identity_topic),
      model::partition_id(0));

    auto batches = storage::test::make_random_batches(
      model::offset(0), 4, false);
    const auto pre_batch_size = sum_records(batches) * 2;

    using namespace std::literals;
    auto f1 = push(
      input_ntp, model::make_memory_record_batch_reader(std::move(batches)));
    auto f2 = drain(
      output_ntp, pre_batch_size, model::timeout_clock::now() + 5s);
    auto read_batches
      = ss::when_all_succeed(std::move(f1), std::move(f2)).get();

    BOOST_REQUIRE(std::get<1>(read_batches).has_value());
    const model::record_batch_reader::data_t& data = *std::get<1>(read_batches);
    BOOST_CHECK_EQUAL(sum_records(data), pre_batch_size);
}

FIXTURE_TEST(test_coproc_router_multi_route, router_test_fixture) {
    auto client = make_client();
    client.connect().get();
    // Create and initialize the environment
    // Starts with 4 parititons of logs managing topic "sole_input"
    const model::topic tt("sole_input");
    const std::size_t n_partitions = 4;
    // and one coprocessor that transforms this topic
    add_copro<two_way_split_copro>(4444, {{"sole_input", l}}).get();
    startup({{make_ts(tt), n_partitions}}, client)
      .then([&client] { return client.stop(); })
      .get();

    // Iterating over all ntps, create random data and push them onto
    // their respective logs
    std::vector<ss::future<model::offset>> pushes;
    pushes.reserve(4);
    std::vector<two_way_split_stats> twss;
    twss.reserve(4);
    for (auto i = 0; i < n_partitions; ++i) {
        model::ntp new_ntp(default_ns, tt, model::partition_id(i));
        model::record_batch_reader::data_t data
          = storage::test::make_random_batches(model::offset(0), 4, false);
        twss.emplace_back(two_way_split_stats(data));
        pushes.emplace_back(push(
          std::move(new_ntp),
          model::make_memory_record_batch_reader(std::move(data))));
    }

    // Knowing what the apply::two_way_split method will do, setup listeners
    // for the materialized topics that are known to eventually exist
    std::vector<ss::future<two_way_split_stats>> drains;
    drains.reserve(n_partitions * 2);
    using namespace std::literals;
    auto timeout = model::timeout_clock::now() + 10s;
    for (auto i = 0; i < n_partitions; ++i) {
        model::ntp even(
          default_ns,
          model::to_materialized_topic(tt, model::topic("even")),
          model::partition_id(i));
        model::ntp odd(
          default_ns,
          model::to_materialized_topic(tt, model::topic("odd")),
          model::partition_id(i));
        drains.emplace_back(
          drain(even, twss[i].n_even, timeout).then(&map_stats));
        drains.emplace_back(
          drain(odd, twss[i].n_odd, timeout).then(&map_stats));
    }

    // Wait on all asynchronous actions to finish
    ss::when_all_succeed(pushes.begin(), pushes.end()).get();
    std::vector<two_way_split_stats> all_drained
      = ss::when_all_succeed(drains.begin(), drains.end()).get0();

    // Calculate recieved records across all ntps and compare to known
    const auto known_totals = aggregate_totals(twss.cbegin(), twss.cend());
    const auto observed_totals = aggregate_totals(
      all_drained.cbegin(), all_drained.cend());
    BOOST_CHECK_EQUAL(known_totals, observed_totals);
}
