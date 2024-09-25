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

#include "bytes/iobuf_parser.h"
#include "iceberg/rest_client/catalog_client.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace r = iceberg::rest_client;
namespace t = ::testing;
namespace bh = boost::beast::http;

using namespace std::chrono_literals;

namespace {
constexpr auto endpoint = "http://localhost:8181";
const r::credentials credentials{.client_id = "id", .client_secret = "secret"};
} // namespace

class mock_client : public http::abstract_client {
public:
    MOCK_METHOD(
      ss::future<http::collected_response>,
      request_and_collect_response,
      (bh::request_header<>&&,
       std::optional<iobuf>,
       ss::lowres_clock::duration),
      (override));
};

TEST(client, default_state) {
    mock_client m;
    r::catalog_client cc{m, endpoint, credentials, "api/catalog/", "x", "v2"};
    ASSERT_EQ(cc.root_url(), "api/catalog/v2/x/");
    ASSERT_TRUE(cc.token().empty());
}

ss::future<http::collected_response> validate_token_request(
  bh::request_header<>&& r,
  std::optional<iobuf> payload = std::nullopt,
  ss::lowres_clock::duration timeout = 0s) {
    EXPECT_EQ(r.at(bh::field::host), "localhost:8181");
    EXPECT_EQ(
      r.at(bh::field::content_type), "application/x-www-form-urlencoded");
    EXPECT_TRUE(payload.has_value());
    iobuf_parser p{std::move(payload.value())};
    auto received = p.read_string(p.bytes_left());
    std::ranges::sort(received);

    ss::sstring expected{
      "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL&client_secret="
      "secret&client_id=id"};
    std::ranges::sort(expected);

    EXPECT_EQ(received, expected);

    co_return http::collected_response{
      .status = bh::status::ok,
      .body = iobuf::from(R"J({"access_token": "token", "expires_in": 1})J")};
}

TEST(token_tests, acquire_token) {
    mock_client m;
    EXPECT_CALL(m, request_and_collect_response(t::_, t::_, t::_))
      .WillOnce(t::Invoke(validate_token_request));
    r::catalog_client cc{m, endpoint, credentials};
    ss::abort_source as;
    retry_chain_node rtc{as, ss::lowres_clock::now() + 5s, 1s};
    cc.acquire_token(rtc).get();
    ASSERT_EQ(cc.token(), "token");
}
