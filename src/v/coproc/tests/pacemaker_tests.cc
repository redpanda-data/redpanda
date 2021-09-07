/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/core/when_all.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

using copro_typeid = coproc::registry::type_identifier;

ss::future<std::size_t> number_of_logs(redpanda_thread_fixture* rtf) {
    return rtf->app.storage.map_reduce0(
      [](storage::api& api) { return api.log_mgr().size(); },
      std::size_t(0),
      std::plus<>());
}

FIXTURE_TEST(test_coproc_router_no_results, coproc_test_fixture) {
    // Note the original number of logs
    const std::size_t n_logs = number_of_logs(root_fixture()).get0();
    // Storage has 10 ntps, 8 of topic 'bar' and 2 of 'foo'
    model::topic foo("foo");
    model::topic bar("bar");
    setup({{foo, 2}, {bar, 8}}).get();
    // Router has 2 coprocessors, one subscribed to 'foo' the other 'bar'
    enable_coprocessors({{.id = 2222,
                          .data{
                            .tid = copro_typeid::null_coprocessor,
                            .topics = {std::make_pair<>(
                              bar, coproc::topic_ingestion_policy::stored)}}},
                         {.id = 7777,
                          .data{
                            .tid = copro_typeid::null_coprocessor,
                            .topics = {std::make_pair<>(
                              foo, coproc::topic_ingestion_policy::stored)}}}})
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
    const std::size_t final_n_logs = number_of_logs(root_fixture()).get0()
                                     - n_logs;
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
FIXTURE_TEST(test_coproc_router_off_by_one, coproc_test_fixture) {
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
          .tid = copro_typeid::identity_coprocessor,
          .topics = {std::make_pair<>(
            src_topic, coproc::topic_ingestion_policy::stored)}}}})
      .get();
    auto fn = [this, input_ntp, output_ntp]() -> ss::future<size_t> {
        return push(input_ntp, single_record_record_batch_reader())
          .then([this, input_ntp, output_ntp](auto) {
              return drain(output_ntp, 1)
                .then([input_ntp, output_ntp](
                        std::optional<model::record_batch_reader::data_t> r) {
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

FIXTURE_TEST(test_coproc_router_double, coproc_test_fixture) {
    model::topic foo("foo");
    model::topic bar("bar");
    // Storage has 5 ntps, 4 of topic 'foo' and 1 of 'bar'
    setup({{foo, 4}, {bar, 1}}).get();
    // Supervisor has 3 registered transforms, of the same type
    enable_coprocessors({{.id = 8888,
                          .data{
                            .tid = copro_typeid::identity_coprocessor,
                            .topics = {std::make_pair<>(
                              foo, coproc::topic_ingestion_policy::stored)}}},
                         {.id = 9159,
                          .data{
                            .tid = copro_typeid::identity_coprocessor,
                            .topics = {std::make_pair<>(
                              foo, coproc::topic_ingestion_policy::stored)}}}})
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

FIXTURE_TEST(test_copro_auto_deregister_function, coproc_test_fixture) {
    model::topic foo("foo");
    setup({{foo, 24}}).get();
    auto id = coproc::script_id(497563);
    // Register a coprocessor that throws
    enable_coprocessors({{.id = id(),
                          .data{
                            .tid = copro_typeid::throwing_coprocessor,
                            .topics = {std::make_pair<>(
                              foo, coproc::topic_ingestion_policy::stored)}}}})
      .get();

    // Push some data across input topic....
    std::vector<ss::future<model::offset>> fs;
    for (auto i = 0; i < 24; ++i) {
        fs.emplace_back(push(
          model::ntp(model::kafka_namespace, foo, model::partition_id(i)),
          storage::test::make_random_memory_record_batch_reader(
            model::offset(0), 10, 1)));
    }
    ss::when_all_succeed(fs.begin(), fs.end()).get();

    /// Assert that the coproc does not exist in memory
    auto n_registered = root_fixture()
                          ->app.pacemaker
                          .map_reduce0(
                            [id](coproc::pacemaker& p) {
                                return p.local_script_id_exists(id);
                            },
                            false,
                            std::logical_or<>())
                          .get();
    BOOST_CHECK_EQUAL(n_registered, false);
}
