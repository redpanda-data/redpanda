/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/leaders_preference.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(parse_leaders_preference) {
    {
        auto lp = config::leaders_preference::parse("none");
        BOOST_CHECK(lp.type == config::leaders_preference::type_t::none);
        BOOST_CHECK_EQUAL(lp.racks.size(), 0);
    }

    {
        auto lp = config::leaders_preference::parse("racks: A");
        BOOST_CHECK(lp.type == config::leaders_preference::type_t::racks);
        BOOST_CHECK_EQUAL(lp.racks.size(), 1);
        BOOST_CHECK_EQUAL(lp.racks[0], model::rack_id{"A"});
    }

    {
        auto lp = config::leaders_preference::parse("racks:rack1, rack2 ");
        BOOST_CHECK(lp.type == config::leaders_preference::type_t::racks);
        BOOST_CHECK_EQUAL(lp.racks.size(), 2);
        BOOST_CHECK_EQUAL(lp.racks[0], model::rack_id{"rack1"});
        BOOST_CHECK_EQUAL(lp.racks[1], model::rack_id{"rack2"});
    }

    {
        auto orig = config::leaders_preference{};
        auto parsed = config::leaders_preference::parse(fmt::to_string(orig));
        BOOST_CHECK(orig == parsed);
    }

    {
        auto orig = config::leaders_preference{};
        orig.type = config::leaders_preference::type_t::racks;
        orig.racks = {model::rack_id{"A"}, model::rack_id{"rack"}};
        auto parsed = config::leaders_preference::parse(fmt::to_string(orig));
        BOOST_CHECK(orig == parsed);
        BOOST_CHECK(parsed.type == config::leaders_preference::type_t::racks);
        BOOST_CHECK_EQUAL(parsed.racks.size(), 2);
    }

    BOOST_CHECK_THROW(
      config::leaders_preference::parse("quux"), std::runtime_error);
    BOOST_CHECK_THROW(
      config::leaders_preference::parse("racks: "), std::runtime_error);
    BOOST_CHECK_THROW(
      config::leaders_preference::parse("racks: a,,b"), std::runtime_error);
    BOOST_CHECK_THROW(
      config::leaders_preference::parse("racks:a,"), std::runtime_error);
}
