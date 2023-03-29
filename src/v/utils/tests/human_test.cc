// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/human.h"
#include "utils/human_chrono.h"

#include <boost/test/unit_test.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

BOOST_AUTO_TEST_CASE(human_bytes) {
    BOOST_CHECK_EQUAL(fmt::format("{}", human::bytes(-1)), "-1.000bytes");
    BOOST_CHECK_EQUAL(fmt::format("{}", human::bytes(0)), "0.000bytes");
    BOOST_CHECK_EQUAL(fmt::format("{}", human::bytes(1)), "1.000bytes");
    BOOST_CHECK_EQUAL(fmt::format("{}", human::bytes(1024)), "1024.000bytes");
    BOOST_CHECK_EQUAL(fmt::format("{}", human::bytes(1025)), "1.001KiB");
    BOOST_CHECK_EQUAL(
      fmt::format("{}", human::bytes(1UL << 20U)), "1024.000KiB");
    BOOST_CHECK_EQUAL(
      fmt::format("{}", human::bytes(1UL << 30U)), "1024.000MiB");
    BOOST_CHECK_EQUAL(
      fmt::format("{}", human::bytes(1UL << 40U)), "1024.000GiB");
    BOOST_CHECK_EQUAL(
      fmt::format("{}", human::bytes(1UL << 50U)), "1024.000TiB");
    BOOST_CHECK_EQUAL(
      fmt::format("{}", human::bytes(1UL << 60U)), "1024.000PiB");
}

BOOST_AUTO_TEST_CASE(human_seconds_decimal) {
    namespace ch = std::chrono;
    using namespace std::literals::chrono_literals;

    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(70809ms)), "70.809s");
    BOOST_CHECK_EQUAL(fmt::to_string(human::seconds_decimal(5060ms)), "5.060s");
    BOOST_CHECK_EQUAL(fmt::to_string(human::seconds_decimal(304ms)), "0.304s");
    BOOST_CHECK_EQUAL(fmt::to_string(human::seconds_decimal(20ms)), "0.020s");
    BOOST_CHECK_EQUAL(fmt::to_string(human::seconds_decimal(1ms)), "0.001s");
    BOOST_CHECK_EQUAL(fmt::to_string(human::seconds_decimal(0ms)), "0.000s");
    BOOST_CHECK_EQUAL(fmt::to_string(human::seconds_decimal(-1ms)), "-0.001s");
    BOOST_CHECK_EQUAL(fmt::to_string(human::seconds_decimal(-23ms)), "-0.023s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-450ms)), "-0.450s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-6700ms)), "-6.700s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-89000ms)), "-89.000s");

    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(1200300400500ns)),
      "1200.300400500s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(1200300400ns)), "1.200300400s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(1200300ns)), "0.001200300s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(1200ns)), "0.000001200s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(1ns)), "0.000000001s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(0ns)), "0.000000000s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-1ns)), "-0.000000001s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-1200ns)), "-0.000001200s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-1200300ns)), "-0.001200300s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-1200300400ns)), "-1.200300400s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(-1200300400500ns)),
      "-1200.300400500s");

    BOOST_CHECK_EQUAL(
      fmt::to_string(human::seconds_decimal(ch::duration<int, std::deci>(166))),
      "16.6s");
    BOOST_CHECK_EQUAL(
      fmt::to_string(
        human::seconds_decimal(ch::duration<short, std::centi>(-7780))),
      "-77.80s");
}
