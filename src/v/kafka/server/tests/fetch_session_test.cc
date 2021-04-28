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
#include "kafka/protocol/fetch.h"
#include "kafka/server/fetch_session.h"
#include "kafka/server/fetch_session_cache.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <tuple>

using namespace std::chrono_literals; // NOLINT
struct fixture {
    static kafka::fetch_session_partition make_fetch_partition(
      model::topic topic, model::partition_id p_id, model::offset offset) {
        return kafka::fetch_session_partition{
          .topic = std::move(topic),
          .partition = p_id,
          .max_bytes = 1_MiB,
          .fetch_offset = offset,
          .high_watermark = offset};
    }

    static kafka::fetch_request::topic
    make_fetch_request_topic(model::topic tp, int partitions_count) {
        kafka::fetch_request::topic fetch_topic{
          .name = std::move(tp),
          .partitions = {},
        };

        for (int i = 0; i < partitions_count; ++i) {
            fetch_topic.partitions.push_back(kafka::fetch_request::partition{
              .id = model::partition_id(i),
              .fetch_offset = model::offset(i * 10),
              .partition_max_bytes = 100_KiB,
            });
        }
        return fetch_topic;
    }
};

FIXTURE_TEST(test_next_epoch, fixture) {
    BOOST_REQUIRE_EQUAL(
      kafka::next_epoch(kafka::initial_fetch_session_epoch),
      kafka::fetch_session_epoch(1));

    BOOST_REQUIRE_EQUAL(
      kafka::next_epoch(kafka::fetch_session_epoch(25)),
      kafka::fetch_session_epoch(26));

    BOOST_REQUIRE_EQUAL(
      kafka::next_epoch(kafka::final_fetch_session_epoch),
      kafka::final_fetch_session_epoch);
}

FIXTURE_TEST(test_fetch_session_basic_operations, fixture) {
    kafka::fetch_session session(kafka::fetch_session_id(123));
    std::vector<std::tuple<model::topic, model::partition_id, model::offset>>
      expected;
    expected.reserve(20);

    for (int i = 0; i < 20; ++i) {
        expected.emplace_back(
          model::topic(random_generators::gen_alphanum_string(5)),
          model::partition_id(
            random_generators::get_int(i * 10, ((i + 1) * 10) - 1)),
          model::offset(random_generators::get_int(10000)));

        auto& t = expected.back();
        session.partitions().emplace(
          std::apply(fixture::make_fetch_partition, t));
    }

    BOOST_TEST_MESSAGE("test insertion order iteration");
    size_t i = 0;
    auto rng = boost::make_iterator_range(
      session.partitions().cbegin_insertion_order(),
      session.partitions().cend_insertion_order());

    for (auto fp : rng) {
        BOOST_REQUIRE_EQUAL(fp.topic, std::get<0>(expected[i]));
        BOOST_REQUIRE_EQUAL(fp.partition, std::get<1>(expected[i]));
        BOOST_REQUIRE_EQUAL(fp.fetch_offset, std::get<2>(expected[i]));
        ++i;
    }

    BOOST_TEST_MESSAGE("test lookup");
    for (auto& t : expected) {
        auto key = model::topic_partition_view(std::get<0>(t), std::get<1>(t));
        BOOST_REQUIRE(session.partitions().contains(key));
        BOOST_REQUIRE(
          session.partitions().find(key) != session.partitions().end());
    }

    auto not_existing = model::topic_partition(
      model::topic("123456"), model::partition_id(9999));

    BOOST_REQUIRE(!session.partitions().contains(not_existing));
    BOOST_REQUIRE(
      session.partitions().find(not_existing) == session.partitions().end());

    BOOST_TEST_MESSAGE("test erase");

    auto key = model::topic_partition_view(
      std::get<0>(expected[0]), std::get<1>(expected[0]));
    auto mem_usage_before = session.mem_usage();
    session.partitions().erase(key);
    BOOST_REQUIRE(!session.partitions().contains(key));
    BOOST_REQUIRE(session.partitions().find(key) == session.partitions().end());

    BOOST_REQUIRE_LT(session.mem_usage(), mem_usage_before);
}

FIXTURE_TEST(test_session_operations, fixture) {
    kafka::fetch_session_cache cache(120s);
    kafka::fetch_request req;
    req.session_epoch = kafka::initial_fetch_session_epoch;
    req.session_id = kafka::invalid_fetch_session_id;
    req.topics = {make_fetch_request_topic(model::topic("test"), 3)};
    {
        BOOST_TEST_MESSAGE("create new session");
        auto ctx = cache.maybe_get_session(req);

        BOOST_REQUIRE_EQUAL(ctx.error(), kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(ctx.has_error(), false);
        // first fetch has to be full fetch
        BOOST_REQUIRE_EQUAL(ctx.is_full_fetch(), true);
        BOOST_REQUIRE_EQUAL(ctx.is_sessionless(), false);
        BOOST_REQUIRE_NE(ctx.session().get(), nullptr);
        auto rng = boost::make_iterator_range(
          ctx.session()->partitions().cbegin_insertion_order(),
          ctx.session()->partitions().cend_insertion_order());
        auto i = 0;
        BOOST_REQUIRE_EQUAL(ctx.session()->partitions().size(), 3);
        for (const auto& fp : rng) {
            BOOST_REQUIRE_EQUAL(fp.topic, req.topics[0].name);
            BOOST_REQUIRE_EQUAL(fp.partition, req.topics[0].partitions[i].id);
            BOOST_REQUIRE_EQUAL(
              fp.fetch_offset, req.topics[0].partitions[i].fetch_offset);
            BOOST_REQUIRE_EQUAL(
              fp.max_bytes, req.topics[0].partitions[i].partition_max_bytes);
            i++;
        }

        req.session_id = ctx.session()->id();
        req.session_epoch = ctx.session()->epoch();
    }

    BOOST_TEST_MESSAGE("test updating session");
    {
        req.topics[0].partitions.erase(
          std::next(req.topics[0].partitions.begin()));
        // add 2 partitons from new topic, forget one from the first topic
        req.topics.push_back(
          make_fetch_request_topic(model::topic("test-new"), 2));
        req.forgotten_topics.push_back(kafka::fetch_request::forgotten_topic{
          .name = model::topic("test"), .partitions = {1}});

        auto ctx = cache.maybe_get_session(req);

        BOOST_REQUIRE_EQUAL(ctx.error(), kafka::error_code::none);
        BOOST_REQUIRE_EQUAL(ctx.has_error(), false);
        // this is an incremental fetch
        BOOST_REQUIRE_EQUAL(ctx.is_full_fetch(), false);
        BOOST_REQUIRE_EQUAL(ctx.is_sessionless(), false);
        BOOST_REQUIRE_NE(ctx.session().get(), nullptr);

        BOOST_REQUIRE_EQUAL(ctx.session()->partitions().size(), 4);
        auto rng = boost::make_iterator_range(
          ctx.session()->partitions().cbegin_insertion_order(),
          ctx.session()->partitions().cend_insertion_order());

        auto i = 0;
        // check that insertion order is preserved
        for (const auto& fp : rng) {
            auto t_idx = i < 2 ? 0 : 1;
            auto p_idx = i < 2 ? i : i - 2;

            BOOST_REQUIRE_EQUAL(fp.topic, req.topics[t_idx].name);
            BOOST_REQUIRE_EQUAL(
              fp.partition, req.topics[t_idx].partitions[p_idx].id);
            BOOST_REQUIRE_EQUAL(
              fp.fetch_offset,
              req.topics[t_idx].partitions[p_idx].fetch_offset);
            BOOST_REQUIRE_EQUAL(
              fp.max_bytes,
              req.topics[t_idx].partitions[p_idx].partition_max_bytes);
            i++;
        }
    }
    BOOST_TEST_MESSAGE("removing session");
    {
        req.session_epoch = kafka::final_fetch_session_epoch;
        req.topics = {};

        auto ctx = cache.maybe_get_session(req);

        BOOST_REQUIRE_EQUAL(ctx.is_sessionless(), true);
        BOOST_REQUIRE_EQUAL(ctx.has_error(), false);
        BOOST_REQUIRE(ctx.session().get() == nullptr);
        BOOST_REQUIRE(cache.size() == 0);
    }
}
