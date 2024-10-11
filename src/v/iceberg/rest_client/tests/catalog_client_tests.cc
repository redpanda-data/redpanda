/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf_parser.h"
#include "iceberg/rest_client/catalog_client.h"

#include <seastar/core/sleep.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace r = iceberg::rest_client;
namespace t = ::testing;
namespace bh = boost::beast::http;

using namespace std::chrono_literals;

namespace {
constexpr auto endpoint = "http://localhost:8181";
const r::credentials credentials{.client_id = "id", .client_secret = "secret"};

template<typename Variant, typename Outer>
void assert_type_and_value(Outer input, Variant expected) {
    EXPECT_TRUE(std::holds_alternative<Variant>(input));
    EXPECT_EQ(std::get<Variant>(input), expected);
}

} // namespace

class mock_client : public http::abstract_client {
public:
    MOCK_METHOD(
      ss::future<http::downloaded_response>,
      request_and_collect_response,
      (bh::request_header<>&&,
       std::optional<iobuf>,
       ss::lowres_clock::duration),
      (override));
    MOCK_METHOD(ss::future<>, shutdown_and_stop, (), (override));
};

// A client source for testing. Allows setting expectation on a client before
// returning to the catalog client.
struct client_source : public r::client_source {
    client_source(
      std::function<void(std::reference_wrapper<mock_client>)> expectation)
      : _expectation{std::move(expectation)} {}

    std::unique_ptr<http::abstract_client> acquire() final {
        auto ptr = std::make_unique<mock_client>();
        _expectation(*(ptr.get()));

        // Client passed to caller should always be shut down, and exactly once
        EXPECT_CALL(*(ptr.get()), shutdown_and_stop())
          .Times(1)
          .WillOnce(t::Return(ss::make_ready_future()));
        return ptr;
    }

private:
    std::function<void(std::reference_wrapper<mock_client>)> _expectation;
};

namespace iceberg::rest_client {

class catalog_client_tester {
public:
    explicit catalog_client_tester(catalog_client& c)
      : _c{c} {}

    ss::sstring root_path() const { return _c.root_path(); }

    ss::future<expected<ss::sstring>> get_current_token() const {
        ss::abort_source as;
        retry_chain_node rtc{as, 5s, 200ms};
        co_return co_await _c.ensure_token(rtc);
    }

private:
    catalog_client& _c;
};

} // namespace iceberg::rest_client

TEST(path_components, path_with_base_and_prefix) {
    r::path_components pc{
      r::base_path{"b"}, r::prefix_path{"pre"}, r::api_version{"v1"}};
    ASSERT_EQ(pc.root_path(), "b/v1/pre/");
    ASSERT_EQ(pc.token_api_path(), "b/v1/oauth/tokens");
}

TEST(path_components, path_with_no_optional_parts) {
    r::path_components pc{};
    ASSERT_EQ(pc.root_path(), "v1/");
    ASSERT_EQ(pc.token_api_path(), "v1/oauth/tokens");
}

TEST(path_components, path_with_prefix) {
    r::path_components pc{std::nullopt, r::prefix_path{"pre"}};
    ASSERT_EQ(pc.root_path(), "v1/pre/");
    ASSERT_EQ(pc.token_api_path(), "v1/oauth/tokens");
}

TEST(client, root_url_computed) {
    client_source cs{[](mock_client&) {}};
    r::catalog_client cc{
      cs,
      endpoint,
      credentials,
      r::base_path{"api/catalog/"},
      r::prefix_path{"x"},
      r::api_version{"v2"}};
    r::catalog_client_tester t{cc};
    ASSERT_EQ(t.root_path(), "api/catalog/v2/x/");
}

ss::future<http::downloaded_response> validate_token_request(
  bh::request_header<>&& r,
  std::optional<iobuf> payload,
  [[maybe_unused]] ss::lowres_clock::duration timeout) {
    EXPECT_EQ(r.at(bh::field::host), "localhost:8181");
    EXPECT_EQ(
      r.at(bh::field::content_type), "application/x-www-form-urlencoded");
    EXPECT_TRUE(payload.has_value());
    iobuf_parser p{std::move(payload.value())};
    auto received = p.read_string(p.bytes_left());
    std::ranges::sort(received);

    ss::sstring expected{
      "grant_type=client_credentials&scope=PRINCIPAL_ROLE%3aALL&client_secret="
      "secret&client_id=id"};
    std::ranges::sort(expected);

    EXPECT_EQ(received, expected);

    co_return http::downloaded_response{
      .status = bh::status::ok,
      .body = iobuf::from(R"J({"access_token": "token", "expires_in": 1})J")};
}

TEST(token_tests, acquire_token) {
    client_source cs{[](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_))
          .WillOnce(validate_token_request);
    }};
    r::catalog_client cc{cs, endpoint, credentials};
    r::catalog_client_tester t{cc};
    auto token = t.get_current_token().get();
    ASSERT_TRUE(token.has_value());
    ASSERT_EQ(token, "token");
}

TEST(token_tests, supplied_token_used) {
    client_source cs{[](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_)).Times(0);
    }};
    const r::oauth_token supplied_token{
      .token = "t", .expires_at = ss::lowres_clock::now() + 1h};
    r::catalog_client cc{
      cs,
      endpoint,
      credentials,
      std::nullopt,
      std::nullopt,
      r::api_version{"v1"},
      supplied_token};

    r::catalog_client_tester t{cc};
    auto token = t.get_current_token().get();
    ASSERT_TRUE(token.has_value());
    ASSERT_EQ(token, supplied_token.token);
}

TEST(token_tests, supplied_token_expired) {
    client_source cs{[](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_))
          .WillOnce(validate_token_request);
    }};
    const r::oauth_token expired_token{
      .token = "t", .expires_at = ss::lowres_clock::now()};
    r::catalog_client cc{
      cs,
      endpoint,
      credentials,
      std::nullopt,
      std::nullopt,
      r::api_version{"v1"},
      expired_token};

    ss::sleep(1s).get();
    r::catalog_client_tester t{cc};
    auto token = t.get_current_token().get();
    ASSERT_TRUE(token.has_value());
    ASSERT_EQ(token, "token");
}

TEST(token_tests, handle_bad_json) {
    client_source cs{[](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_))
          .WillOnce(t::Return(ss::make_ready_future<http::downloaded_response>(
            http::downloaded_response{
              .status = bh::status::ok, .body = iobuf::from(R"J({)J")})));
    }};
    r::catalog_client cc{cs, endpoint, credentials};
    r::catalog_client_tester t{cc};
    auto token = t.get_current_token().get();
    ASSERT_FALSE(token.has_value());
    ASSERT_THAT(
      token.error(),
      t::VariantWith<r::json_parse_error>(
        t::Field(&r::json_parse_error::context, "parse_json")));
}

TEST(token_tests, handle_non_retriable_http_status) {
    client_source cs{[](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_))
          .WillOnce(t::Return(ss::make_ready_future<http::downloaded_response>(
            http::downloaded_response{
              .status = bh::status::bad_request, .body = iobuf()})));
    }};

    r::catalog_client cc{cs, endpoint, credentials};
    r::catalog_client_tester t{cc};

    auto token = t.get_current_token().get();
    ASSERT_FALSE(token.has_value());
    ASSERT_THAT(
      token.error(),
      t::VariantWith<r::http_call_error>(
        t::VariantWith<bh::status>(bh::status::bad_request)));
}

TEST(token_tests, handle_retriable_http_status) {
    client_source cs{[](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_))
          .WillOnce(t::Return(ss::make_ready_future<http::downloaded_response>(
            http::downloaded_response{
              .status = bh::status::gateway_timeout, .body = iobuf()})))
          .WillOnce(t::Return(ss::make_ready_future<http::downloaded_response>(
            http::downloaded_response{
              .status = bh::status::ok,
              .body = iobuf::from(
                R"J({"access_token": "token", "expires_in": 1})J")})));
    }};
    r::catalog_client cc{cs, endpoint, credentials};
    r::catalog_client_tester t{cc};

    auto token = t.get_current_token().get();
    ASSERT_TRUE(token.has_value());
    ASSERT_EQ(token, "token");
}

TEST(token_tests, handle_retries_exhausted) {
    auto ret = [](
                 [[maybe_unused]] bh::request_header<>&& r,
                 [[maybe_unused]] std::optional<iobuf> payload,
                 [[maybe_unused]] ss::lowres_clock::duration timeout) {
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{.status = bh::status::gateway_timeout});
    };

    client_source cs{[&ret](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_))
          .WillRepeatedly(ret);
    }};

    r::catalog_client cc{cs, endpoint, credentials};
    r::catalog_client_tester t{cc};

    auto token = t.get_current_token().get();
    ASSERT_FALSE(token.has_value());
    ASSERT_THAT(
      token.error(),
      t::VariantWith<r::retries_exhausted>(t::Field(
        &r::retries_exhausted::errors,
        t::Each(t::VariantWith<bh::status>(bh::status::gateway_timeout)))));
}
