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
#include "kafka/fetch_session.h"
#include "kafka/requests/fetch_request.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <tuple>

struct fixture {
    static kafka::fetch_partition make_fetch_partition(
      model::topic topic, model::partition_id p_id, model::offset offset) {
        return kafka::fetch_partition{
          .topic = std::move(topic),
          .partition = p_id,
          .max_bytes = 1_MiB,
          .fetch_offset = offset,
          .high_watermark = offset};
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
