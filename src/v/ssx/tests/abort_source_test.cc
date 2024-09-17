// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "ssx/abort_source.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>

#include <boost/test/unit_test.hpp>

static constexpr auto derived_str = "derived";
static constexpr auto expected_msg = std::string_view{derived_str};

struct derived_error final : std::runtime_error {
    derived_error()
      : std::runtime_error(derived_str) {}
};

struct fixture {
    using sub_arg_t = const std::optional<std::exception_ptr>&;

    auto make_incrementor() {
        ++expected_count;
        return [this](sub_arg_t) noexcept { ++count; };
    }

    auto make_async_incrementor() {
        ++expected_count;
        return [this](sub_arg_t) noexcept {
            ++count;
            return ss::now();
        };
    }

    auto make_naive_incrementor() {
        ++expected_count;
        return [this]() noexcept { ++count; };
    }

    auto make_async_naive_incrementor() {
        ++expected_count;
        return [this]() noexcept {
            ++count;
            return ss::now();
        };
    }

    auto start() { return sas.start(as); }
    auto stop() { return sas.stop(); }

    std::atomic_size_t count{0};
    std::atomic_size_t expected_count{0};

    ss::abort_source as;
    ssx::sharded_abort_source sas;
};

SEASTAR_THREAD_TEST_CASE(ssx_sharded_abort_source_test_abort_parent) {
    BOOST_REQUIRE(ss::smp::count > 1);

    fixture f;
    f.start().get();

    auto fas_sub_1 = ss::smp::submit_to(ss::shard_id{1}, [&f]() {
                         return f.sas.subscribe(f.make_incrementor());
                     }).get();

    f.as.request_abort_ex(derived_error{});

    // Ensure parent exception is propated
    BOOST_REQUIRE(f.sas.abort_requested());
    BOOST_REQUIRE_EXCEPTION(
      f.sas.check(), derived_error, [](const derived_error& e) {
          return e.what() == expected_msg;
      });

    f.stop().get();

    BOOST_REQUIRE_EQUAL(f.count, f.expected_count);
}

SEASTAR_THREAD_TEST_CASE(ssx_sharded_abort_source_test_no_abort_parent) {
    BOOST_REQUIRE(ss::smp::count > 1);

    fixture f;
    f.start().get();

    auto fas_sub_1 = ss::smp::submit_to(ss::shard_id{1}, [&f]() {
                         return f.sas.subscribe(f.make_incrementor());
                     }).get();

    BOOST_REQUIRE(!f.sas.abort_requested());
    BOOST_REQUIRE_NO_THROW(f.sas.check());

    // Ensure stop aborts the subscriptions
    f.stop().get();

    BOOST_REQUIRE_EQUAL(f.count, f.expected_count);
}

SEASTAR_THREAD_TEST_CASE(ssx_sharded_abort_source_subscribe_test) {
    fixture f;
    f.start().get();

    // Ensure that sync and async functions are called;
    auto s0 = f.sas.subscribe(f.make_incrementor());
    auto s1 = f.sas.subscribe(f.make_async_incrementor());
    auto s2 = f.sas.subscribe(f.make_naive_incrementor());
    auto s3 = f.sas.subscribe(f.make_async_naive_incrementor());

    f.as.request_abort_ex(derived_error());
    f.stop().get();
    BOOST_REQUIRE_EQUAL(f.count, f.expected_count);
}
