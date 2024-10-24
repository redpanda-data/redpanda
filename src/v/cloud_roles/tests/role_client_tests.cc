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
#include "cloud_roles/azure_aks_refresh_impl.h"
#include "cloud_roles/gcp_refresh_impl.h"
#include "http/tests/http_imposter.h"
#include "test_definitions.h"
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
    auto resp = cl.fetch_credentials().get();
    BOOST_REQUIRE(std::holds_alternative<iobuf>(resp));
    BOOST_REQUIRE_EQUAL(
      iobuf_to_bytes(std::get<iobuf>(resp)),
      bytes::from_string(cloud_role_tests::gcp_oauth_token));
    BOOST_REQUIRE(has_call(cloud_role_tests::gcp_url));
}

FIXTURE_TEST(test_bad_response_handling, fixture) {
    listen();
    ss::abort_source as;

    auto cl = cloud_roles::gcp_refresh_impl{address(), region, as};
    auto resp = cl.fetch_credentials().get();
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
    auto resp = cl.fetch_credentials().get();
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
    auto resp = cl.fetch_credentials().get();
    // assert that calls are made in order:
    // 1. to find the role
    // 2. to get token
    BOOST_REQUIRE(has_calls_in_order(
      cloud_role_tests::aws_role_query_url, cloud_role_tests::aws_creds_url));

    BOOST_REQUIRE(std::holds_alternative<iobuf>(resp));
    BOOST_REQUIRE_EQUAL(
      iobuf_to_bytes(std::get<iobuf>(resp)),
      bytes::from_string(cloud_role_tests::aws_creds));
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
                     .get();

    ss::sstring token{"token"};
    auto wrote = token_f.dma_write(0, token.data(), token.size()).get();
    BOOST_REQUIRE_EQUAL(wrote, token.size());

    auto cl = cloud_roles::aws_sts_refresh_impl{address(), region, as};
    auto resp = cl.fetch_credentials().get();

    token_f.close().get();
    ss::remove_file(cloud_role_tests::token_file).get();
    BOOST_REQUIRE(std::holds_alternative<iobuf>(resp));
    BOOST_REQUIRE_EQUAL(std::get<iobuf>(resp), cloud_role_tests::sts_creds);

    auto posted = get_requests()[0].content;
    BOOST_REQUIRE(ba::contains(posted, "Action=AssumeRoleWithWebIdentity"));
    BOOST_REQUIRE(ba::contains(posted, "RoleArn=tomato"));
    BOOST_REQUIRE(ba::contains(posted, "WebIdentityToken=token"));
}

class cloud_roles::aks_test_helper {
public:
    static auto get_address(const azure_aks_refresh_impl& aks) {
        return aks.address();
    }
};

SEASTAR_THREAD_TEST_CASE(aks_authority_host_read_test) {
    // test that we can correctly read various forms of AZURE_AUTHORITY_HOST

    // boilerplate
    using helper_t = cloud_roles::aks_test_helper;
    setenv("AZURE_CLIENT_ID", "dummy_client_id", 0);
    setenv("AZURE_TENANT_ID", "dummy_tenant_id", 0);
    setenv("AZURE_FEDERATED_TOKEN_FILE", "dummy_fed_token_file", 0);

    auto dummy_as = ss::abort_source{};
    BOOST_TEST_CONTEXT("env variable simple hostname") {
        setenv("AZURE_AUTHORITY_HOST", "simple.com", 1);
        auto aks = cloud_roles::azure_aks_refresh_impl{
          net::unresolved_address{},
          cloud_roles::aws_region_name{},
          dummy_as,
          cloud_roles::retry_params{}};
        BOOST_CHECK_EQUAL(
          helper_t::get_address(aks),
          net::unresolved_address("simple.com", uint16_t(443)));
    }

    BOOST_TEST_CONTEXT("env variable url http") {
        setenv("AZURE_AUTHORITY_HOST", "http://simple.com/", 1);
        auto aks = cloud_roles::azure_aks_refresh_impl{
          net::unresolved_address{},
          cloud_roles::aws_region_name{},
          dummy_as,
          cloud_roles::retry_params{}};
        BOOST_CHECK_EQUAL(
          helper_t::get_address(aks),
          net::unresolved_address("simple.com", uint16_t(80)));
    }

    BOOST_TEST_CONTEXT("env variable url https") {
        setenv("AZURE_AUTHORITY_HOST", "https://simple.com/", 1);
        auto aks = cloud_roles::azure_aks_refresh_impl{
          net::unresolved_address{},
          cloud_roles::aws_region_name{},
          dummy_as,
          cloud_roles::retry_params{}};
        BOOST_CHECK_EQUAL(
          helper_t::get_address(aks),
          net::unresolved_address("simple.com", uint16_t(443)));
    }

    BOOST_TEST_CONTEXT("env variable url https and port") {
        setenv("AZURE_AUTHORITY_HOST", "https://simple.com:9999/", 1);
        auto aks = cloud_roles::azure_aks_refresh_impl{
          net::unresolved_address{},
          cloud_roles::aws_region_name{},
          dummy_as,
          cloud_roles::retry_params{}};
        BOOST_CHECK_EQUAL(
          helper_t::get_address(aks),
          net::unresolved_address("simple.com", uint16_t(9999)));
    }

    BOOST_TEST_CONTEXT("external override") {
        setenv("AZURE_AUTHORITY_HOST", "https://simple.com:9999/", 1);
        auto external_override = net::unresolved_address{
          "this is not actually a valid host", 1234};
        auto aks = cloud_roles::azure_aks_refresh_impl{
          external_override,
          cloud_roles::aws_region_name{},
          dummy_as,
          cloud_roles::retry_params{}};
        BOOST_CHECK_EQUAL(helper_t::get_address(aks), external_override);
    }
}
