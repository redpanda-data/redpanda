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
#include "kafka/client/types.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/types.h"
#include "kafka/server/fetch_session.h"
#include "kafka/server/fetch_session_cache.h"
#include "kafka/server/handlers/fetch.h"
#include "kafka/server/handlers/fetch/fetch_planner.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/interface.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>
#include <fmt/ostream.h>

#include <tuple>

static ss::logger fpt_logger("fpt_test");

using namespace std::chrono_literals; // NOLINT
struct fixture {
    static kafka::fetch_request::topic
    make_fetch_request_topic(model::topic tp, int partitions_count) {
        kafka::fetch_request::topic fetch_topic{
          .name = std::move(tp),
          .fetch_partitions = {},
        };

        for (int i = 0; i < partitions_count; ++i) {
            fetch_topic.fetch_partitions.push_back(
              kafka::fetch_request::partition{
                .partition_index = model::partition_id(i),
                .fetch_offset = model::offset(i * 10),
                .max_bytes = 100_KiB,
              });
        }
        return fetch_topic;
    }
};

struct fetch_plan_fixture : redpanda_thread_fixture {
    static constexpr size_t topic_name_length = 30;
    static constexpr size_t total_partition_count = 800;
    static constexpr size_t session_partition_count = 100;

    model::topic t;

    fetch_plan_fixture() {
        BOOST_TEST_CHECKPOINT("before leadership");

        wait_for_controller_leadership().get();

        BOOST_TEST_CHECKPOINT("HERE");

        t = model::topic(
          random_generators::gen_alphanum_string(topic_name_length));
        auto tp = model::topic_partition(t, model::partition_id(0));
        add_topic(
          model::topic_namespace_view(model::kafka_namespace, t),
          total_partition_count)
          .get();

        BOOST_TEST_CHECKPOINT("HERE");
    }
};

PERF_TEST_F(fetch_plan_fixture, test_fetch_plan) {
    auto make_fetch_req = [this]() {
        // make the fetch topic
        kafka::fetch_topic ft;
        ft.name = t;

        // add the partitions to the fetch request
        for (int pid = 0; pid < session_partition_count; pid++) {
            kafka::fetch_partition fp;
            fp.partition_index = model::partition_id(pid);
            fp.fetch_offset = model::offset(0);
            fp.current_leader_epoch = kafka::leader_epoch(-1);
            fp.log_start_offset = model::offset(-1);
            fp.max_bytes = 1048576;
            ft.fetch_partitions.push_back(std::move(fp));
        }

        BOOST_TEST_CHECKPOINT("HERE");

        // create a request
        kafka::fetch_request_data frq_data;
        frq_data.replica_id = kafka::client::consumer_replica_id;
        frq_data.max_wait_ms = 500ms;
        frq_data.min_bytes = 1;
        frq_data.max_bytes = 52428800;
        frq_data.isolation_level = model::isolation_level::read_uncommitted;
        frq_data.session_id = kafka::invalid_fetch_session_id;
        frq_data.session_epoch = kafka::initial_fetch_session_epoch;
        frq_data.topics.push_back(std::move(ft));

        return kafka::fetch_request{std::move(frq_data)};
    };

    auto fetch_req = make_fetch_req();

    BOOST_TEST_CHECKPOINT("HERE");

    // we need to share a connection among any requests here since the
    // session cache is associated with a connection
    auto conn = make_connection_context();

    BOOST_TEST_CHECKPOINT("HERE");

    kafka::request_header header{
      .key = kafka::fetch_handler::api::key,
      .version = kafka::fetch_handler::max_supported};

    // use this initial request to populate the fetch session
    // in the session cache
    kafka::fetch_session_id sess_id;
    {
        auto rctx = make_request_context(make_fetch_req(), header, conn);
        // set up a fetch session
        auto ctx = rctx.fetch_sessions().maybe_get_session(fetch_req);
        BOOST_REQUIRE_EQUAL(ctx.has_error(), false);
        // first fetch has to be full fetch
        BOOST_REQUIRE_EQUAL(ctx.is_full_fetch(), true);
        BOOST_REQUIRE_EQUAL(ctx.is_sessionless(), false);

        BOOST_REQUIRE_EQUAL(
          ctx.session()->partitions().size(), session_partition_count);

        sess_id = ctx.session()->id();
        BOOST_REQUIRE(sess_id > 0);
    }

    BOOST_TEST_CHECKPOINT("HERE");

    fetch_req.data.session_id = sess_id;
    fetch_req.data.session_epoch = 1;
    fetch_req.data.topics.clear();

    auto rctx = make_request_context(std::move(fetch_req), header);
    BOOST_REQUIRE_EQUAL(rctx.fetch_sessions().size(), 1);

    // add all partitions to fetch metadata
    auto& mdc = rctx.get_fetch_metadata_cache();
    for (int i = 0; i < total_partition_count; i++) {
        mdc.insert_or_assign(
          {t, i}, model::offset(0), model::offset(100), model::offset(100));
    }

    vassert(mdc.size() == total_partition_count, "mdc.size(): {}", mdc.size());

    auto octx = kafka::op_context(
      std::move(rctx), ss::default_smp_service_group());

    BOOST_REQUIRE(!octx.session_ctx.is_sessionless());
    BOOST_REQUIRE_EQUAL(octx.session_ctx.session()->id(), sess_id);

    BOOST_TEST_CHECKPOINT("HERE");

    constexpr size_t iters = 10000; // 0000000U;

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < iters; i++) {
        auto plan = kafka::testing::make_simple_fetch_plan(octx);
        perf_tests::do_not_optimize(plan);
    }
    perf_tests::stop_measuring_time();

    vassert(
      mdc.size() == total_partition_count,
      "mdc.size(): {}",
      mdc.size()); // check that nothing was evicted

    // double micros_per_iter = timer._total_duration / 1ns / 1000.
    //                          / timer._total_timings;
    // fmt::print(
    //   "FPT {} iters, {} micros/iter micros/part {}\n",
    //   timer._total_timings,
    //   micros_per_iter,
    //   micros_per_iter / session_partition_count);

    // auto plan = kafka::make_simple_fetch_plan(octx);
    // auto& pfps = plan.fetches_per_shard;
    // fmt::print("FPT plan count: {}\n", pfps.size());
    // if (pfps.size()) {
    //     fmt::print("FPT plan[0] parts: {}\n", pfps[0].requests.size());
    // }

    // for (auto& sf : plan.fetches_per_shard) {
    //     fmt::print("FPT plan: {}\n", sf);
    // }
    return (size_t)(session_partition_count * iters);
}
