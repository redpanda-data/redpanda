/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/logger.h"
#include "cloud_roles/refresh_credentials.h"
#include "config/configuration.h"
#include "http/tests/http_imposter.h"
#include "test_definitions.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"
#include "test_utils/tee_log.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/defer.hh>

#include <fmt/chrono.h>

inline ss::logger test_log("test"); // NOLINT

class fixture : public http_imposter_fixture {
public:
    fixture()
      : http_imposter_fixture(4444) {}
};

/// Helps test the credential fetch operation by triggering abort after a single
/// credential is fetched.
struct one_shot_fetch {
    explicit one_shot_fetch(
      std::optional<cloud_roles::credentials>& credentials)
      : credentials(credentials) {}
    std::optional<cloud_roles::credentials>& credentials;

    ss::future<> operator()(cloud_roles::credentials c) {
        credentials.emplace(std::move(c));
        return ss::now();
    }
};

/// Helper to assert refresh calls, aborts after two fetches.
struct two_fetches {
    using counter = ss::shared_ptr<uint8_t>;

    explicit two_fetches(std::optional<cloud_roles::credentials>& credentials)
      : credentials(credentials)
      , count{ss::make_shared<uint8_t>(0)} {}

    std::optional<cloud_roles::credentials>& credentials;
    counter count;

    counter get_counter() { return count; }

    ss::future<> operator()(cloud_roles::credentials c) {
        (*count)++;
        credentials.emplace(std::move(c));
        return ss::now();
    }
};

using namespace std::chrono_literals;

FIXTURE_TEST(test_get_oauth_token, fixture) {
    when()
      .request(cloud_role_tests::gcp_url)
      .then_reply_with(cloud_role_tests::gcp_oauth_token);
    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::gcp_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(10s, [&c] {
        return c.has_value();
    }).get();

    BOOST_REQUIRE_EQUAL(
      "a-token",
      std::get<cloud_roles::gcp_credentials>(c.value()).oauth_token());
}

FIXTURE_TEST(test_token_refresh_on_expiry, fixture) {
    // Token expires in 5 seconds
    auto short_token = R"json(
{"access_token":"a-token","expires_in":5,"token_type":"Bearer"}
)json";
    when().request(cloud_role_tests::gcp_url).then_reply_with(short_token);
    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    two_fetches s(c);
    auto count = s.get_counter();

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::gcp_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(30s, [count] {
        return *count >= 2;
    }).get();

    BOOST_REQUIRE_EQUAL(
      "a-token",
      std::get<cloud_roles::gcp_credentials>(c.value()).oauth_token());

    BOOST_REQUIRE(
      has_calls_in_order(cloud_role_tests::gcp_url, cloud_role_tests::gcp_url));
}

FIXTURE_TEST(test_aws_credentials, fixture) {
    when()
      .request(cloud_role_tests::aws_role_query_url)
      .then_reply_with(cloud_role_tests::aws_role);

    when()
      .request(cloud_role_tests::aws_creds_url)
      .then_reply_with(cloud_role_tests::aws_creds);

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(10s, [&c] {
        return c.has_value();
    }).get();

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("my-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("my-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("my-token", aws_creds.session_token.value());

    BOOST_REQUIRE(has_calls_in_order(
      imdsv2_token_url,
      cloud_role_tests::aws_role_query_url,
      cloud_role_tests::aws_creds_url));
    BOOST_REQUIRE_EQUAL(
      get_latest_request(cloud_role_tests::aws_role_query_url)
        .value()
        .get()
        .header("X-aws-ec2-metadata-token"),
      "IMDSv2-TOKEN");
    BOOST_REQUIRE_EQUAL(
      get_latest_request(cloud_role_tests::aws_creds_url)
        .value()
        .get()
        .header("X-aws-ec2-metadata-token"),
      "IMDSv2-TOKEN");
}

FIXTURE_TEST(test_short_lived_aws_credentials, fixture) {
    when()
      .request(cloud_role_tests::aws_role_query_url)
      .then_reply_with(cloud_role_tests::aws_role);

    using namespace std::chrono_literals;

    auto response = R"json(
{{
  "Code" : "Success",
  "LastUpdated" : "2012-04-26T16:39:16Z",
  "Type" : "AWS-HMAC",
  "AccessKeyId" : "my-key",
  "SecretAccessKey" : "my-secret",
  "Token" : "my-token",
  "Expiration" : "{}"
}}
)json";

    when()
      .request(cloud_role_tests::aws_creds_url)
      .then_reply_with(fmt::format(
        fmt::runtime(response),
        fmt::format(
          "{:%Y-%m-%dT%H:%M:%SZ}",
          fmt::gmtime(std::chrono::system_clock::now() + 5s))));

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    two_fetches s(c);
    auto count = s.get_counter();

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(30s, [count] {
        return *count >= 2;
    }).get();

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("my-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("my-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("my-token", aws_creds.session_token.value());

    const auto& requests = get_requests();
    BOOST_REQUIRE_EQUAL(requests[0].url, imdsv2_token_url);
    BOOST_REQUIRE_EQUAL(requests[1].url, cloud_role_tests::aws_role_query_url);
    BOOST_REQUIRE_EQUAL(requests[2].url, cloud_role_tests::aws_creds_url);
    BOOST_REQUIRE_EQUAL(requests[3].url, imdsv2_token_url);
    BOOST_REQUIRE_EQUAL(requests[4].url, cloud_role_tests::aws_creds_url);
}

FIXTURE_TEST(test_sts_credentials, fixture) {
    setenv("AWS_ROLE_ARN", cloud_role_tests::aws_role, 1);
    setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "test_sts_creds_f", 1);

    config::shard_local_cfg().cloud_storage_credentials_host.set_value(
      std::optional<ss::sstring>{"localhost"});
    auto reset_cfg = ss::defer(
      [] { config::shard_local_cfg().cloud_storage_credentials_host.reset(); });

    when()
      .request("/")
      .with_method(ss::httpd::POST)
      .then_reply_with(cloud_role_tests::sts_creds);
    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto token_f = ss::open_file_dma(
                     "test_sts_creds_f",
                     ss::open_flags::create | ss::open_flags::rw)
                     .get();
    ss::sstring token{"token"};
    token_f.dma_write(0, token.data(), token.size()).get();

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::sts,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(10s, [&c] {
        return c.has_value();
    }).get();

    token_f.close().get();
    ss::remove_file("test_sts_creds_f").get();

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("sts-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("sts-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("sts-token", aws_creds.session_token.value());
    BOOST_REQUIRE(has_call("/"));
}

FIXTURE_TEST(test_short_lived_sts_credentials, fixture) {
    setenv("AWS_ROLE_ARN", cloud_role_tests::aws_role, 1);
    setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "test_short_sts_f", 1);

    constexpr const char* sts_creds = R"xml(
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>sts-key</AccessKeyId>
      <SecretAccessKey>sts-secret</SecretAccessKey>
      <SessionToken>sts-token</SessionToken>
      <Expiration>{}</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>
)xml";

    using namespace std::chrono_literals;
    when()
      .request("/")
      .with_method(ss::httpd::POST)
      .then_reply_with(fmt::format(
        fmt::runtime(sts_creds),
        fmt::format(
          "{:%Y-%m-%dT%H:%M:%SZ}",
          fmt::gmtime(std::chrono::system_clock::now() + 5s))));

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    two_fetches s(c);
    auto count = s.get_counter();

    auto token_f = ss::open_file_dma(
                     "test_short_sts_f",
                     ss::open_flags::create | ss::open_flags::rw)
                     .get();
    ss::sstring token{"token"};
    token_f.dma_write(0, token.data(), token.size()).get();

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::sts,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(30s, [count] {
        return *count >= 2;
    }).get();

    token_f.close().get();
    ss::remove_file("test_short_sts_f").get();
    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("sts-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("sts-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("sts-token", aws_creds.session_token.value());

    BOOST_REQUIRE(has_calls_in_order("/", "/"));
}

FIXTURE_TEST(test_client_closed_on_error, fixture) {
    tee_wrapper wrapper(cloud_roles::clrl_log);

    fail_request_if(
      [](const auto&) { return true; },
      http_test_utils::response{
        .body = "not found",
        .status = ss::http::reply::status_type::not_found});

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(10s, [this] {
        return has_call(cloud_role_tests::aws_role_query_url);
    }).get();

    // Assert that the error response body is logged
    BOOST_REQUIRE(wrapper.string().find("not found") != std::string::npos);
}

FIXTURE_TEST(test_handle_temporary_timeout, fixture) {
    // This test asserts that if the remote endpoint is not reachable, the
    // refresh operation will attempt to retry. In order not to expose the retry
    // counter or make similar changes to the class just for testing, this test
    // scans the log for the message emitted when a ss::timed_out_error is seen.
    tee_wrapper wrapper(cloud_roles::clrl_log);
    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    config::shard_local_cfg()
      .cloud_storage_roles_operation_timeout_ms.set_value(
        std::chrono::milliseconds{100ms});
    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(5s, [&wrapper] {
        return wrapper.string().find(
                 "api request failed (retrying after cool-off "
                 "period): timedout")
               != std::string::npos;
    }).get();
}

FIXTURE_TEST(test_handle_bad_response, fixture) {
    tee_wrapper wrapper(cloud_roles::clrl_log);

    fail_request_if(
      [](const auto&) { return true; },
      http_test_utils::response{
        .body = "{broken response",
        .status = ss::http::reply::status_type::ok});

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number()});

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(10s, [&wrapper] {
        return wrapper.string().find("retrying after cool-off")
               != wrapper.string().npos;
    }).get();

    BOOST_REQUIRE(
      wrapper.string().find(
        "failed during IAM credentials refresh: Can't parse the request")
      != wrapper.string().npos);
}

FIXTURE_TEST(test_intermittent_error, fixture) {
    // This test makes one failing request to API endpoint followed by one
    // successful request. The refresh credentials object should retry after the
    // first failure.

    tee_wrapper wrapper(cloud_roles::clrl_log);

    when()
      .request(cloud_role_tests::aws_role_query_url)
      .then_reply_with(cloud_role_tests::aws_role);

    when()
      .request(cloud_role_tests::aws_creds_url)
      .then_reply_with(cloud_role_tests::aws_creds);

    // Fail the first request and pass all subsequent requests.
    auto idx = 0;
    fail_request_if(
      [&idx](const auto&) { return idx++ == 0; },
      http_test_utils::response{
        .body = "failed!",
        .status = ss::http::reply::status_type::internal_server_error});

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    two_fetches s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number()});

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    tests::cooperative_spin_wait_with_timeout(30s, [&c]() {
        return c.has_value()
               && std::holds_alternative<cloud_roles::aws_credentials>(
                 c.value());
    }).get();

    auto log = wrapper.string();

    BOOST_REQUIRE(
      log.find("failed during IAM credentials refresh: failed!") != log.npos);
    BOOST_REQUIRE(log.find("retrying after cool-off") != log.npos);

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL(aws_creds.access_key_id, "my-key");
}

FIXTURE_TEST(test_imdsv2_fallback, fixture) {
    tee_wrapper wrapper(cloud_roles::clrl_log);

    fail_request_if(
      [](const auto& req) {
          return req._url == imdsv2_token_url && req._method == "PUT";
      },
      http_test_utils::response{
        .body = "not found",
        .status = ss::http::reply::status_type::not_found});

    when()
      .request(cloud_role_tests::aws_role_query_url)
      .then_reply_with(cloud_role_tests::aws_role);

    when()
      .request(cloud_role_tests::aws_creds_url)
      .then_reply_with(cloud_role_tests::aws_creds);

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    RPTEST_REQUIRE_EVENTUALLY(10s, [&c] { return c.has_value(); });

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("my-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("my-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("my-token", aws_creds.session_token.value());

    const auto& requests = get_requests();
    BOOST_REQUIRE_EQUAL(requests[0].url, imdsv2_token_url);
    BOOST_REQUIRE_EQUAL(requests[1].url, cloud_role_tests::aws_role_query_url);
    BOOST_REQUIRE_EQUAL(requests[2].url, cloud_role_tests::aws_creds_url);

    const auto& role_q = get_latest_request(
      cloud_role_tests::aws_role_query_url);
    BOOST_REQUIRE(role_q.has_value());
    BOOST_REQUIRE(
      !role_q->get().header("X-aws-ec2-metadata-token").has_value());

    auto log = wrapper.string();

    BOOST_REQUIRE(
      log.find("Failed to get IMDSv2 token, engaging fallback mode. ")
      != log.npos);
}

FIXTURE_TEST(test_imdsv2_fallback_not_triggered_unless_error_matches, fixture) {
    tee_wrapper wrapper(cloud_roles::clrl_log);

    fail_request_if(
      [](const auto& req) {
          return req._url == imdsv2_token_url && req._method == "PUT";
      },
      http_test_utils::response{
        .body = "X",
        .status = ss::http::reply::status_type::internal_server_error});

    listen();

    ss::abort_source as;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      as,
      s,
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    RPTEST_REQUIRE_EVENTUALLY(
      10s, [this] { return has_call(imdsv2_token_url); });

    BOOST_REQUIRE(!c.has_value());

    const auto& requests = get_requests();
    BOOST_REQUIRE_EQUAL(requests[0].url, imdsv2_token_url);

    auto log = wrapper.string();

    BOOST_REQUIRE(
      log.find("Failed to get IMDSv2 token, engaging fallback mode. ")
      == log.npos);
}

FIXTURE_TEST(test_abs_aks_credentials, fixture) {
    /// check a successful credential refresh: provide a short lived token,
    /// check that the endpoint is called in a timely fashion multiple times

    constexpr static auto token_file_name
      = "test_abs_ask_credentials_token_file";
    constexpr static auto token_value = std::string_view{"a_token"};
    constexpr static auto tenant_id = "a_tenant_id";
    constexpr static auto client_id = "a_client_id";
    // setup env variables
    setenv("AZURE_CLIENT_ID", client_id, 1);
    setenv("AZURE_TENANT_ID", tenant_id, 1);
    setenv("AZURE_FEDERATED_TOKEN_FILE", token_file_name, 1);
    setenv("AZURE_AUTHORITY_HOST", "localhost", 1);

    // create the token file
    auto _ = [] {
        auto token_f = ss::open_file_dma(
                         token_file_name,
                         ss::open_flags::create | ss::open_flags::rw)
                         .get();
        auto _ = ss::defer([&token_f] { token_f.close().get(); });

        auto token_stream = ss::make_file_output_stream(token_f).get();
        token_stream.write(token_value.data(), token_value.size()).get();
        token_stream.flush().get();
        return ss::defer([] { ss::remove_file(token_file_name).get(); });
    }();

    // prepare response for a short lived token
    auto endpoint = ssx::sformat("/{}/oauth2/v2.0/token", tenant_id);
    when()
      .request(endpoint)
      .with_method(ss::httpd::POST)
      .then_reply_with(ssx::sformat(
        R"json(
{{
  "token_type": "Bearer",
  "expires_in": 5,
  "access_token": "{}"
}}
      )json",
        token_value));
    listen();

    // save resulting credentials here
    auto maybe_credentials = std::optional<cloud_roles::credentials>{};
    auto cred_requests = 0;

    // start refresh process
    auto as = ss::abort_source{};
    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::azure_aks_oidc_federation,
      as,
      [&](cloud_roles::credentials in) {
          maybe_credentials = in;
          ++cred_requests;
          return ss::now();
      },
      cloud_roles::aws_region_name{""},
      address());
    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    auto run_check_once = [&] {
        // a check waits for the endpoint to be called, for credentials to be
        // produced and that calls/credentials conforms to expectations
        tests::cooperative_spin_wait_with_timeout(15s, [&] {
            return has_call(endpoint) && maybe_credentials.has_value();
        }).get();
        auto last_req = get_latest_request(endpoint).value();

        BOOST_CHECK(last_req.get()
                      .header("Content-Type")
                      .value()
                      .contains("application/x-www-form-urlencoded"));
        BOOST_CHECK(last_req.get().content.contains(
          ssx::sformat("client_id={}", client_id)));

        BOOST_CHECK(maybe_credentials.has_value());
        auto azure_aks_creds = std::get<cloud_roles::abs_oauth_credentials>(
          maybe_credentials.value());
        BOOST_CHECK_EQUAL(azure_aks_creds.oauth_token(), token_value);
        maybe_credentials.reset();
        reset_http_call_state();
    };

    // check that the endpoint is called multiple times
    run_check_once();
    run_check_once();
    BOOST_CHECK(cred_requests >= 2);
}

FIXTURE_TEST(test_abs_aks_credentials_missing_fields, fixture) {
    // check that a malformed json does not break the system
    constexpr static auto token_file_name
      = "test_abs_ask_credentials_token_file";
    constexpr static auto token_value = std::string_view{"a_token"};
    constexpr static auto tenant_id = "a_tenant_id";
    constexpr static auto client_id = "a_client_id";
    // setup env variables
    setenv("AZURE_CLIENT_ID", client_id, 1);
    setenv("AZURE_TENANT_ID", tenant_id, 1);
    setenv("AZURE_FEDERATED_TOKEN_FILE", token_file_name, 1);
    setenv("AZURE_AUTHORITY_HOST", "localhost", 1);

    // create the token file
    auto _ = [] {
        auto token_f = ss::open_file_dma(
                         token_file_name,
                         ss::open_flags::create | ss::open_flags::rw)
                         .get();
        auto _ = ss::defer([&token_f] { token_f.close().get(); });

        auto token_stream = ss::make_file_output_stream(token_f).get();
        token_stream.write(token_value.data(), token_value.size()).get();
        token_stream.flush().get();
        return ss::defer([] { ss::remove_file(token_file_name).get(); });
    }();

    // prepare broken response
    auto endpoint = ssx::sformat("/{}/oauth2/v2.0/token", tenant_id);
    when()
      .request(endpoint)
      .with_method(ss::httpd::POST)
      .then_reply_with(ssx::sformat(
        R"json(
{{
  "token_type": "Bearer",
  "access_token": "{}"
}}
      )json",
        token_value));
    listen();

    // save credentials here (expected to remain empty)
    auto maybe_credentials = std::optional<cloud_roles::credentials>{};

    auto as = ss::abort_source{};
    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::azure_aks_oidc_federation,
      as,
      [&](cloud_roles::credentials in) {
          maybe_credentials = in;
          return ss::now();
      },
      cloud_roles::aws_region_name{""},
      address());

    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });

    // wait for a call to the endpoint, check that the call is correct and that
    // no result is produced
    tests::cooperative_spin_wait_with_timeout(15s, [&] {
        return has_call(endpoint);
    }).get();
    auto last_req = get_latest_request(endpoint).value();

    BOOST_CHECK(last_req.get()
                  .header("Content-Type")
                  .value()
                  .contains("application/x-www-form-urlencoded"));
    BOOST_CHECK(
      last_req.get().content.contains(ssx::sformat("client_id={}", client_id)));

    BOOST_CHECK(!maybe_credentials.has_value());
}

FIXTURE_TEST(test_abs_vm_credentials, fixture) {
    /// check a successful credential refresh: provide a short lived token,
    /// check that the endpoint is called in a timely fashion multiple times

    constexpr static auto client_id = std::string_view{"a_client_id"};
    constexpr static auto token_value = std::string_view{"a_token"};
    constexpr static auto endpoint = "/metadata/identity/oauth2/token";
    // setup config
    auto s_cfg = scoped_config{};
    s_cfg.get("cloud_storage_azure_managed_identity_id")
      .set_value(std::optional<ss::sstring>{client_id});

    // prepare response for a short lived token
    when()
      .request(endpoint)
      .with_method(ss::httpd::GET)
      .then_reply_with(ssx::sformat(
        R"json(
{{
  "token_type": "Bearer",
  "expires_in": "5",
  "access_token": "{}",
  "refresh_token": "",
  "expires_on": "X",
  "not_before": "X",
  "resource": "https://storage.azure.com/"
}}
      )json",
        token_value));
    listen();

    // save resulting credentials here
    auto maybe_credentials = std::optional<cloud_roles::credentials>{};
    auto cred_requests = 0;

    // start refresh process
    auto as = ss::abort_source{};
    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::azure_vm_instance_metadata,
      as,
      [&](cloud_roles::credentials in) {
          maybe_credentials = in;
          ++cred_requests;
          return ss::now();
      },
      cloud_roles::aws_region_name{""},
      address());
    refresh.start();
    auto deferred = ss::defer([&refresh] { refresh.stop().get(); });
    auto _ = ss::defer([&] {
        fmt::print(
          "has_call({}): {}, get requests:\n",
          endpoint,
          has_call(endpoint, true));
        for (auto& u : get_requests()) {
            fmt::print("{} {} {}\n", u.method, u.url, u.content);
        }
    });
    auto run_check_once = [&] {
        // a check waits for the endpoint to be called, for credentials to be
        // produced and that calls/credentials conforms to expectations
        tests::cooperative_spin_wait_with_timeout(15s, [&] {
            return has_call(endpoint, true) && maybe_credentials.has_value();
        }).get();
        auto last_req = get_latest_request(endpoint, true).value();

        BOOST_CHECK(last_req.get().header("Metadata").value().contains("true"));
        BOOST_CHECK(
          last_req.get().url.contains(ssx::sformat("client_id={}", client_id)));

        BOOST_CHECK(maybe_credentials.has_value());
        auto azure_vm_creds = std::get<cloud_roles::abs_oauth_credentials>(
          maybe_credentials.value());
        BOOST_CHECK_EQUAL(azure_vm_creds.oauth_token(), token_value);
        maybe_credentials.reset();
        reset_http_call_state();
    };

    // check that the endpoint is called multiple times
    run_check_once();
    run_check_once();
    BOOST_CHECK(cred_requests >= 2);
}
