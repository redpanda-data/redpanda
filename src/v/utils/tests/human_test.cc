// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/human.h"

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
