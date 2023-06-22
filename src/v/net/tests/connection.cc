/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "net/connection.h"

#include <seastar/core/future.hh>

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>

#include <exception>
#include <system_error>

BOOST_AUTO_TEST_CASE(test_is_disconnect_error) {
    auto is_a_de = std::system_error(
      std::make_error_code(std::errc::broken_pipe));
    auto not_a_de = std::system_error(
      std::make_error_code(std::errc::is_a_directory));

    BOOST_CHECK(net::is_disconnect_exception(std::make_exception_ptr(is_a_de))
                  .has_value());

    BOOST_CHECK(!net::is_disconnect_exception(std::make_exception_ptr(not_a_de))
                   .has_value());

    BOOST_CHECK(
      net::is_disconnect_exception(
        std::make_exception_ptr(ss::nested_exception(
          std::make_exception_ptr(is_a_de), std::make_exception_ptr(not_a_de))))
        .has_value());

    BOOST_CHECK(
      net::is_disconnect_exception(
        std::make_exception_ptr(ss::nested_exception(
          std::make_exception_ptr(not_a_de), std::make_exception_ptr(is_a_de))))
        .has_value());

    BOOST_CHECK(!net::is_disconnect_exception(
                   std::make_exception_ptr(ss::nested_exception(
                     std::make_exception_ptr(not_a_de),
                     std::make_exception_ptr(not_a_de))))
                   .has_value());
}
