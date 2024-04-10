/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/util.h"

#include <seastar/core/sharded.hh>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_nested_exception) {
    {
        auto inner = std::make_exception_ptr(ss::abort_requested_exception{});
        auto outer = std::make_exception_ptr(ss::gate_closed_exception{});
        ss::nested_exception ex{inner, outer};
        BOOST_REQUIRE(
          cloud_storage_clients::util::has_abort_or_gate_close_exception(ex));
    }

    {
        auto inner = std::make_exception_ptr(std::bad_alloc{});
        auto outer = std::make_exception_ptr(ss::gate_closed_exception{});
        ss::nested_exception ex{inner, outer};
        BOOST_REQUIRE(
          cloud_storage_clients::util::has_abort_or_gate_close_exception(ex));
    }

    {
        auto inner = std::make_exception_ptr(std::invalid_argument{""});
        auto outer = std::make_exception_ptr(
          ss::no_sharded_instance_exception{});
        ss::nested_exception ex{inner, outer};
        BOOST_REQUIRE(
          !cloud_storage_clients::util::has_abort_or_gate_close_exception(ex));
    }
}
