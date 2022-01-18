/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/api.h"
#include "coproc/pacemaker.h"
#include "coproc/partition_manager.h"
#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/core/coroutine.hh>
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
    produce(input_ntp, make_random_batch(40)).get();

    // Wait for any side-effects, ...expecting that none occur
    ss::sleep(1s).get();
    // Expecting 10, because "foo(2)" and "bar(8)" were loaded at startup
    const std::size_t final_n_logs = number_of_logs(root_fixture()).get0()
                                     - n_logs;
    /// .. but total should be exactly 11 due to the introducion of the
    /// coprocessor_internal_topic
    BOOST_REQUIRE_EQUAL(final_n_logs, 11);
}

/// Tests an off-by-one error where producing recordbatches of size 1 on the
/// input log wouldn't produce onto the materialized log
FIXTURE_TEST(test_coproc_router_off_by_one, coproc_test_fixture) {
    model::topic src_topic("obo");
    model::ntp input_ntp(
      model::kafka_namespace, src_topic, model::partition_id(0));
    model::ntp output_ntp(
      model::kafka_namespace,
      to_materialized_topic(src_topic, identity_coprocessor::identity_topic),
      model::partition_id(0));
    setup({{src_topic, 1}}).get();
    enable_coprocessors(
      {{.id = 12345678,
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {std::make_pair<>(
            src_topic, coproc::topic_ingestion_policy::stored)}}}})
      .get();
    auto fn =
      [this, input_ntp, output_ntp](model::offset start) -> ss::future<size_t> {
        co_await produce(input_ntp, make_random_batch(1));
        auto r = co_await consume(output_ntp, 1, start);
        co_return num_records(r);
    };
    // Perform push/consume twice
    size_t wr = fn(model::offset(0)).get();
    BOOST_CHECK_EQUAL(wr, 1);
    size_t wr2 = fn(model::offset(1)).get();
    BOOST_CHECK_EQUAL(wr2, 1);
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
      to_materialized_topic(foo, identity_coprocessor::identity_topic),
      model::partition_id(0));

    produce(input_ntp, make_random_batch(50)).get();
    auto read_batches = consume(output_ntp, 100).get0();

    /// The identity coprocessor should not have modified the number of
    /// record batches from the source log onto the output log
    BOOST_CHECK_EQUAL(num_records(read_batches), 100);
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
    std::vector<ss::future<>> fs;
    for (auto i = 0; i < 24; ++i) {
        fs.emplace_back(produce(
          model::ntp(model::kafka_namespace, foo, model::partition_id(i)),
          make_random_batch(100)));
    }
    ss::when_all_succeed(fs.begin(), fs.end()).get();

    /// Wait a max of 5s to observe that copro with id 497563 has been
    /// deregistered, throws ss::timed_on_error on failure
    tests::cooperative_spin_wait_with_timeout(5s, [this, id] {
        /// Assert that the coproc does not exist in memory
        auto map = [id](coproc::pacemaker& p) {
            return !p.local_script_id_exists(id);
        };
        return root_fixture()->app.coprocessing->get_pacemaker().map_reduce0(
          std::move(map), true, std::logical_and<>());
    }).get();
}

/// Ensures that a deletion of a materialized topic doesn't conflict
/// with an active coprocessor producing onto it. Furthermore asserts the topic
/// exists at tests end and contains all expected records.
FIXTURE_TEST(test_copro_delete_topic, coproc_test_fixture) {
    model::topic_namespace tbd(model::kafka_namespace, model::topic("tbd"));
    setup({{tbd.tp, 4}}).get();
    auto id = coproc::script_id(59);
    // Use the earliest policy so that when the topic is re-created processing
    // begins at 0, so that the full amount of records can be asserted at tests
    // end
    enable_coprocessors(
      {{.id = id(),
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {{tbd.tp, coproc::topic_ingestion_policy::earliest}}}}})
      .get();

    /// Find shard of one of the partitions
    model::partition_id target_pid(0);
    auto shard = root_fixture()->app.shard_table.local().shard_for(
      model::ntp(tbd.ns, tbd.tp, target_pid));
    BOOST_CHECK(shard.has_value());
    info("On shard: {}", *shard);

    /// Setup listeners to inject the deletion
    model::ntp target(
      tbd.ns,
      to_materialized_topic(tbd.tp, identity_coprocessor::identity_topic),
      target_pid);
    auto f = root_fixture()
               ->app.cp_partition_manager
               .invoke_on(
                 *shard,
                 [this, target](coproc::partition_manager& pm) {
                     ss::promise<> p;
                     auto f = p.get_future();
                     auto id = pm.register_manage_notification(
                       target.ns,
                       target.tp.topic,
                       [this, p = std::move(p)](
                         ss::lw_shared_ptr<coproc::partition>) mutable {
                           p.set_value();
                       });
                     return f.then(
                       [id, &pm] { pm.unregister_manage_notification(id); });
                 })
               .then([this, target] {
                   model::topic_namespace tp{target.ns, target.tp.topic};
                   info("Deleting topic: {} ", tp);
                   std::vector<model::topic_namespace> topics{tp};
                   return root_fixture()
                     ->app.controller->get_topics_frontend()
                     .local()
                     .delete_topics(std::move(topics), model::no_timeout)
                     .then([this, tp](auto) { info("Topic {} deleted", tp); });
               });

    // Push some data across input topic....
    const std::size_t total_records = 10000;
    std::vector<ss::future<>> fs;
    fs.reserve(4);
    for (auto i = 0; i < 4; ++i) {
        info("Producing onto: {}-{}", tbd, i);
        fs.emplace_back(produce(
          model::ntp(tbd.ns, tbd.tp, model::partition_id(i)),
          make_random_batch(total_records)));
    }
    ss::when_all_succeed(fs.begin(), fs.end()).get();
    info("All produced onto: {}", tbd);
    f.handle_exception(
       [this](std::exception_ptr e) { info("Exception detected: {}", e); })
      .get();
    info("Background delete topic request resolved: {}", tbd);

    /// Wait until all processing has completed for all fibers, choosing a large
    /// timeout in the event a delete occurs between writes, it may take some
    /// additional time for the system to establish barriers, complete work,
    /// bring them down, then continue processing
    tests::cooperative_spin_wait_with_timeout(30s, [this]() {
        return root_fixture()->app.coprocessing->get_pacemaker().map_reduce0(
          [this](coproc::pacemaker& p) { return p.is_up_to_date(); },
          true,
          std::logical_and<>());
    }).get();
    info("All fibers have completed");

    auto in_storage = root_fixture()->app.storage.invoke_on(
      *shard,
      [target](storage::api& api) { return (bool)api.log_mgr().get(target); });
    if (!in_storage.get()) {
        /// In the event the delete occurred after all writes completed, it
        /// should be asserted that no evidence of the partition has been left
        /// behind
        info("Materialized topic {} was never re-created", target);
        auto in_pm = root_fixture()->app.cp_partition_manager.invoke_on(
          *shard, [target](coproc::partition_manager& pm) {
              return (bool)pm.get(target);
          });
        bool in_stble = root_fixture()
                          ->app.shard_table.local()
                          .shard_for(target)
                          .has_value();
        BOOST_CHECK(!in_pm.get());
        BOOST_CHECK(!in_stble);
    } else {
        /// Otherwise assert that all data exists since the input topic was
        /// marked with the 'earliest' offset, all records should exist since
        /// re-processing should have began at offset 0.
        info("Materialized topic {} was re-created", target);
        for (auto i = 0; i < 4; ++i) {
            auto results = consume(
                             model::ntp(
                               tbd.ns,
                               to_materialized_topic(
                                 tbd.tp, identity_coprocessor::identity_topic),
                               model::partition_id(i)),
                             total_records)
                             .get();
            BOOST_CHECK_EQUAL(num_records(results), total_records);
        }
    }
}
