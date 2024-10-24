// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "ssx/semaphore.h"
#include "test_utils/fixture.h"

#include <seastar/core/sleep.hh>

#include <exception>
using namespace std::chrono_literals; // NOLINT

struct test_str_ctx final : public rpc::streaming_context {
    virtual ~test_str_ctx() noexcept = default;
    virtual ss::future<ssx::semaphore_units> reserve_memory(size_t) final {
        return get_units(s, 1);
    }
    virtual const rpc::header& get_header() const final { return hdr; }

    virtual void signal_body_parse() final {}
    virtual void body_parse_exception(std::exception_ptr) final {}

    rpc::header hdr;
    ssx::semaphore s{1, "rpc/mock-sc"};
};

struct test_fixture {
    bool triggered = false;

    rpc::internal::response_handler
    create_handler(rpc::clock_type::duration timeout_period) {
        rpc::internal::response_handler r;
        r.with_timeout(
          rpc::timeout_spec::from_now(timeout_period),
          [this]() mutable { triggered = true; });
        return r;
    }
};

FIXTURE_TEST(fail_with_timeout, test_fixture) {
    auto rh = create_handler(100ms);

    // wait for timeout
    ss::sleep(1s).get();

    BOOST_REQUIRE_EQUAL(
      rh.get_future().get().error(), rpc::errc::client_request_timeout);
    BOOST_REQUIRE_EQUAL(triggered, true);
};

FIXTURE_TEST(fail_other_error_with_timeout, test_fixture) {
    auto rh = create_handler(100s);

    rh.set_exception(std::runtime_error("test"));
    BOOST_REQUIRE_THROW(auto res = rh.get_future().get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(triggered, false);
};

FIXTURE_TEST(success_case_with_timout, test_fixture) {
    auto rh = create_handler(100s);
    rh.set_value(std::make_unique<test_str_ctx>());

    auto v = rh.get_future().get();

    BOOST_REQUIRE_EQUAL(triggered, false);
};
