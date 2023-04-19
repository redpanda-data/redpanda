/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/aws_refresh_impl.h"
#include "cloud_roles/aws_sts_refresh_impl.h"
#include "cloud_roles/gcp_refresh_impl.h"
#include "cloud_roles/tests/test_definitions.h"
#include "http/tests/http_imposter.h"
#include "test_utils/fixture.h"

#include <seastar/core/file.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/test/unit_test.hpp>

inline ss::logger test_log("test"); // NOLINT

namespace ba = boost::algorithm;

static const cloud_roles::aws_region_name region{""};

class fixture : public http_imposter_fixture {
public:
    fixture()
      : http_imposter_fixture(4445) {}
};

FIXTURE_TEST(test_simple_token_request, fixture) {
    when()
      .request(cloud_role_tests::gcp_url)
      .then_reply_with(cloud_role_tests::gcp_oauth_token);
    listen();
    ss::abort_source as;

    auto cl = cloud_roles::gcp_refresh_impl{address(), region, as};
    auto resp = cl.fetch_credentials().get0();
    BOOST_REQUIRE(std::holds_alternative<iobuf>(resp));
    BOOST_REQUIRE_EQUAL(
      iobuf_to_bytes(std::get<iobuf>(resp)), cloud_role_tests::gcp_oauth_token);
    BOOST_REQUIRE(has_call(cloud_role_tests::gcp_url));
}

FIXTURE_TEST(test_bad_response_handling, fixture) {
    listen();
    ss::abort_source as;

    auto cl = cloud_roles::gcp_refresh_impl{address(), region, as};
    auto resp = cl.fetch_credentials().get0();
    BOOST_REQUIRE(std::holds_alternative<cloud_roles::api_request_error>(resp));
    auto error = std::get<cloud_roles::api_request_error>(resp);
    BOOST_REQUIRE_EQUAL(
      error.error_kind, cloud_roles::api_request_error_kind::failed_abort);
    BOOST_REQUIRE_EQUAL(error.reason, "http request failed:Not Found");
}

FIXTURE_TEST(test_gateway_down, fixture) {
    when()
      .request(cloud_role_tests::gcp_url)
      .then_reply_with(ss::http::reply::status_type::gateway_timeout);
    listen();
    ss::abort_source as;

    auto cl = cloud_roles::gcp_refresh_impl{address(), region, as};
    auto resp = cl.fetch_credentials().get0();
    BOOST_REQUIRE(std::holds_alternative<cloud_roles::api_request_error>(resp));
    auto error = std::get<cloud_roles::api_request_error>(resp);
    BOOST_REQUIRE_EQUAL(
      error.error_kind, cloud_roles::api_request_error_kind::failed_retryable);
    BOOST_REQUIRE_EQUAL(error.reason, "http request failed:Gateway Timeout");
}

FIXTURE_TEST(test_aws_role_fetch_on_startup, fixture) {
    when()
      .request(cloud_role_tests::aws_role_query_url)
      .then_reply_with(cloud_role_tests::aws_role);
    when()
      .request(cloud_role_tests::aws_creds_url)
      .then_reply_with(cloud_role_tests::aws_creds);
    listen();
    ss::abort_source as;

    auto cl = cloud_roles::aws_refresh_impl{address(), region, as};
    auto resp = cl.fetch_credentials().get0();
    // assert that calls are made in order:
    // 1. to find the role
    // 2. to get token
    BOOST_REQUIRE(has_calls_in_order(
      cloud_role_tests::aws_role_query_url, cloud_role_tests::aws_creds_url));

    BOOST_REQUIRE(std::holds_alternative<iobuf>(resp));
    BOOST_REQUIRE_EQUAL(
      iobuf_to_bytes(std::get<iobuf>(resp)), cloud_role_tests::aws_creds);
}

FIXTURE_TEST(test_sts_credentials_fetch, fixture) {
    when()
      .request("/")
      .with_method(ss::httpd::POST)
      .then_reply_with(cloud_role_tests::sts_creds);
    listen();
    ss::abort_source as;

    // the STS client reads these values from pod environment
    setenv("AWS_ROLE_ARN", cloud_role_tests::aws_role, 1);
    setenv("AWS_WEB_IDENTITY_TOKEN_FILE", cloud_role_tests::token_file, 1);

    auto token_f = ss::open_file_dma(
                     cloud_role_tests::token_file,
                     ss::open_flags::create | ss::open_flags::rw)
                     .get0();

    ss::sstring token{"token"};
    auto wrote = token_f.dma_write(0, token.data(), token.size()).get0();
    BOOST_REQUIRE_EQUAL(wrote, token.size());

    auto cl = cloud_roles::aws_sts_refresh_impl{address(), region, as};
    auto resp = cl.fetch_credentials().get0();

    token_f.close().get0();
    ss::remove_file(cloud_role_tests::token_file).get0();
    BOOST_REQUIRE(std::holds_alternative<iobuf>(resp));
    BOOST_REQUIRE_EQUAL(std::get<iobuf>(resp), cloud_role_tests::sts_creds);

    auto posted = get_requests()[0].content;
    BOOST_REQUIRE(ba::contains(posted, "Action=AssumeRoleWithWebIdentity"));
    BOOST_REQUIRE(ba::contains(posted, "RoleArn=tomato"));
    BOOST_REQUIRE(ba::contains(posted, "WebIdentityToken=token"));
}
