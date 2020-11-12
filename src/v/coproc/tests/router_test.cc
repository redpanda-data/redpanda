// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "coproc/tests/coproc_fixture_base.h"
#include "coproc/tests/coproc_test_fixture.h"
#include "coproc/tests/coprocessor.h"
#include "coproc/tests/supervisor.h"
#include "coproc/tests/two_way_split_copro.h"
#include "coproc/tests/utils.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"
#include "vassert.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <chrono>
#include <iterator>

// Non-sharded rpc_service to emmulate the javascript engine
class router_test_fixture
  : public coproc_test_fixture<rpc_sharded_service_tag> {
public:
    using copro_map = coproc::supervisor::copro_map;

    using simple_input_set
      = std::vector<std::pair<ss::sstring, coproc::topic_ingestion_policy>>;

    // Non-sharded rpc service to emulate the JS engine which is single threaded
    router_test_fixture()
      : coproc_test_fixture<rpc_sharded_service_tag>(43189) {
        _coprocessors.start().get();
        register_service<coproc::supervisor>(std::ref(_coprocessors));
    }

    ~router_test_fixture() override { _coprocessors.stop().get(); }

    template<typename CoprocessorType>
    ss::future<> add_copro(uint32_t sid, simple_input_set&& input) {
        return _coprocessors.invoke_on_all(
          [sid, input = std::move(input)](copro_map& coprocessors) {
              coproc::script_id asid(sid);
              vassert(
                coprocessors.find(asid) == coprocessors.end(),
                "Cannot double insert coprocessor with same cp_id");
              coprocessor::input_set iset;
              iset.reserve(input.size());
              std::transform(
                input.begin(),
                input.end(),
                std::back_inserter(iset),
                [](auto& p) {
                    return std::make_pair(
                      model::topic(std::move(p.first)), std::move(p.second));
                });
              coprocessors.emplace(
                asid, std::make_unique<CoprocessorType>(asid, std::move(iset)));
          });
    }

    void startup(log_layout_map&& llm) {
        // assemble the active_copros from the '_coprocessors' map
        active_copros rlayout;
        _coprocessors
          .invoke_on(
            ss::this_shard_id(),
            [&rlayout](copro_map& coprocessors) {
                std::transform(
                  coprocessors.begin(),
                  coprocessors.end(),
                  std::inserter(rlayout, rlayout.end()),
                  [](const auto& p) {
                      return coproc::enable_copros_request::data{
                        .id = p.first, .topics = p.second->get_input_topics()};
                  });
            })
          .get();
        // Data on all shards is identical
        coproc_test_fixture<rpc_sharded_service_tag>::startup(
          std::move(llm), std::move(rlayout));
    }

private:
    ss::sharded<copro_map> _coprocessors;
};

FIXTURE_TEST(test_coproc_router_no_results, router_test_fixture) {
    // Storage has 10 ntps, 8 of topic 'bar' and 2 of 'foo'
    log_layout_map storage_layout = {{make_ts("foo"), 2}, {make_ts("bar"), 8}};
    // Router has 2 coprocessors, one subscribed to 'foo' the other 'bar'
    add_copro<null_coprocessor>(321, {{"bar", l}}).get();
    add_copro<null_coprocessor>(1234, {{"foo", l}}).get();
    startup(std::move(storage_layout));

    // Test -> Start pushing to registered topics and check that NO
    // materialized logs have been created
    model::ntp input_ntp(
      default_ns, model::topic("foo"), model::partition_id(0));
    auto batches = storage::test::make_random_batches(
      model::offset(0), 4, false);
    const auto n_records = sum_records(batches);
    push(input_ntp, std::move(batches)).get();

    // Wait for any side-effects
    using namespace std::literals;
    ss::sleep(1s).get();
    // Expect that no side-effects have been produced (i.e.
    // materialized_logs)
    const auto n_logs = get_api()
                          .map_reduce0(
                            [](storage::api& api) {
                                return api.log_mgr().size();
                            },
                            size_t(0),
                            std::plus<>())
                          .get0();
    // Expecting 2, because "foo(2)" and "bar(8)" were loaded at startup
    BOOST_REQUIRE_EQUAL(n_logs, 10);
}

FIXTURE_TEST(test_coproc_router_simple, router_test_fixture) {
    // Storage has 16 ntps, 4 of topic 'bar' and 12 of 'foo'
    log_layout_map storage_layout = {{make_ts("foo"), 12}, {make_ts("bar"), 4}};
    // Supervisor has 3 registered transforms, of the same type
    add_copro<identity_coprocessor>(1234, {{"foo", l}}).get();
    add_copro<identity_coprocessor>(121, {{"foo", l}}).get();
    add_copro<identity_coprocessor>(321, {{"bar", l}}).get();
    startup(std::move(storage_layout));

    model::topic src_topic("foo");
    model::ntp input_ntp(default_ns, src_topic, model::partition_id(0));
    model::ntp output_ntp(
      default_ns,
      coproc::to_materialized_topic(
        src_topic, identity_coprocessor::identity_topic),
      model::partition_id(0));

    auto batches = storage::test::make_random_batches(
      model::offset(0), 4, false);
    const auto pre_batch_size = sum_records(batches) * 2;

    using namespace std::literals;
    auto f1 = push(input_ntp, std::move(batches));
    auto f2 = drain(
      output_ntp, pre_batch_size, model::timeout_clock::now() + 5s);
    auto read_batches
      = ss::when_all_succeed(std::move(f1), std::move(f2)).get();

    BOOST_REQUIRE(std::get<0>(read_batches).has_value());
    const model::record_batch_reader::data_t& data = *std::get<0>(read_batches);
    BOOST_CHECK_EQUAL(sum_records(data), pre_batch_size);
}

FIXTURE_TEST(test_coproc_router_multi_route, router_test_fixture) {
    // Create and initialize the environment
    // Starts with 4 parititons of logs managing topic "sole_input"
    const model::topic tt("sole_input");
    const std::size_t n_partitions = 4;
    // and one coprocessor that transforms this topic
    add_copro<two_way_split_copro>(4444, {{"sole_input", l}}).get();
    startup({{make_ts(tt), n_partitions}});

    // Iterating over all ntps, create random data and push them onto
    // their respective logs
    std::vector<ss::future<>> pushes;
    pushes.reserve(4);
    std::vector<two_way_split_stats> twss;
    twss.reserve(4);
    for (auto i = 0; i < n_partitions; ++i) {
        model::ntp new_ntp(default_ns, tt, model::partition_id(i));
        model::record_batch_reader::data_t data
          = storage::test::make_random_batches(model::offset(0), 4, false);
        twss.emplace_back(two_way_split_stats(data));
        pushes.emplace_back(push(std::move(new_ntp), std::move(data)));
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
          coproc::to_materialized_topic(tt, model::topic("even")),
          model::partition_id(i));
        model::ntp odd(
          default_ns,
          coproc::to_materialized_topic(tt, model::topic("odd")),
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
    BOOST_CHECK_EQUAL(known_totals.n_even, observed_totals.n_even);
    BOOST_CHECK_EQUAL(known_totals.n_odd, observed_totals.n_odd);
}
