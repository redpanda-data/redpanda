/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/retry_policy.h"

#include <gtest/gtest.h>

namespace r = iceberg::rest_client;
using enum boost::beast::http::status;

namespace {
template<typename Ex>
r::failure throw_and_catch(Ex ex) {
    try {
        throw ex;
    } catch (...) {
        return r::default_retry_policy{}.should_retry(std::current_exception());
    }
}
} // namespace

TEST(default_retry_policy, status_ok) {
    r::default_retry_policy p;
    auto result = p.should_retry(
      http::downloaded_response{.status = ok, .body = iobuf::from("success")});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().body, iobuf::from("success"));
}

TEST(default_retry_policy, status_retriable) {
    r::default_retry_policy p;
    for (const auto status : std::to_array(
           {internal_server_error,
            bad_gateway,
            service_unavailable,
            gateway_timeout,
            too_many_requests,
            request_timeout})) {
        auto result = p.should_retry(http::downloaded_response{
          .status = status, .body = iobuf::from("retry")});
        ASSERT_FALSE(result.has_value());
        ASSERT_TRUE(result.error().can_be_retried);
    }
}

TEST(default_retry_policy, status_not_retriable) {
    r::default_retry_policy p;
    for (const auto status : std::to_array(
           {bad_request, unauthorized, method_not_allowed, not_acceptable})) {
        auto result = p.should_retry(http::downloaded_response{
          .status = status, .body = iobuf::from("retry")});
        ASSERT_FALSE(result.has_value());
        ASSERT_FALSE(result.error().can_be_retried);
    }
}

TEST(default_retry_policy, boost_system_errors) {
    auto retriable = throw_and_catch(
      boost::system::system_error{boost::beast::http::error::end_of_stream});
    ASSERT_TRUE(retriable.can_be_retried);
    auto unretriable = throw_and_catch(
      boost::system::system_error{boost::beast::http::error::bad_alloc});
    ASSERT_FALSE(unretriable.can_be_retried);
}

TEST(default_retry_policy, system_errors) {
    auto retriable = throw_and_catch(
      std::system_error{ETIMEDOUT, std::generic_category()});
    ASSERT_TRUE(retriable.can_be_retried);
    auto unretriable = throw_and_catch(
      std::system_error{ETIMEDOUT, ss::tls::error_category()});
    ASSERT_FALSE(unretriable.can_be_retried);
}

TEST(default_retry_policy, rethrown_exceptions) {
    EXPECT_THROW(
      throw_and_catch(ss::gate_closed_exception{}), ss::gate_closed_exception);
    EXPECT_THROW(
      throw_and_catch(ss::abort_requested_exception{}),
      ss::abort_requested_exception);
}

TEST(default_retry_policy, nested_exception) {
    EXPECT_THROW(
      throw_and_catch(ss::nested_exception{
        std::make_exception_ptr(ss::gate_closed_exception{}),
        std::make_exception_ptr(std::runtime_error{"out"})}),
      ss::nested_exception);
    EXPECT_THROW(
      throw_and_catch(ss::nested_exception{
        std::make_exception_ptr(std::invalid_argument{""}),
        std::make_exception_ptr(ss::abort_requested_exception{})}),
      ss::nested_exception);

    auto result = throw_and_catch(ss::nested_exception{
      std::make_exception_ptr(std::invalid_argument{"i"}),
      std::make_exception_ptr(std::invalid_argument{"o"})});
    ASSERT_FALSE(result.can_be_retried);
    ASSERT_TRUE(std::holds_alternative<ss::sstring>(result.err));
    ASSERT_EQ(
      "seastar::nested_exception [outer: std::invalid_argument (o), "
      "inner: std::invalid_argument (i)]",
      std::get<ss::sstring>(result.err));
}
