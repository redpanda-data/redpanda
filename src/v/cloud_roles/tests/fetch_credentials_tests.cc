/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cloud_roles/refresh_credentials.h"
#include "cloud_roles/tests/test_definitions.h"
#include "test_utils/fixture.h"
#include "test_utils/http_imposter.h"

#include <seastar/core/file.hh>

#include <fmt/chrono.h>

inline ss::logger test_log("test"); // NOLINT

/// Helps test the credential fetch operation by triggering abort after a single
/// credential is fetched.
struct one_shot_fetch {
    one_shot_fetch(
      std::optional<cloud_roles::credentials>& credentials,
      ss::abort_source& as)
      : credentials(credentials)
      , as(as) {}
    std::optional<cloud_roles::credentials>& credentials;
    ss::abort_source& as;

    ss::future<> operator()(cloud_roles::credentials c) {
        credentials.emplace(std::move(c));
        as.request_abort();
        return ss::now();
    }
};

/// Helper to assert refresh calls, aborts after two fetches.
struct two_fetches {
    two_fetches(
      std::optional<cloud_roles::credentials>& credentials,
      ss::abort_source& as)
      : credentials(credentials)
      , as(as) {}

    uint8_t count{};
    std::optional<cloud_roles::credentials>& credentials;
    ss::abort_source& as;

    ss::future<> operator()(cloud_roles::credentials c) {
        ++count;
        credentials.emplace(std::move(c));
        if (count == 2) {
            as.request_abort();
        }
        return ss::now();
    }
};

using namespace std::chrono_literals;

FIXTURE_TEST(test_get_oauth_token, http_imposter_fixture) {
    when()
      .request(cloud_role_tests::gcp_url)
      .then_reply_with(cloud_role_tests::gcp_oauth_token);
    listen();

    ss::abort_source as;
    ss::gate gate;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c, as);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::gcp_instance_metadata,
      gate,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number});

    refresh.start();
    gate.close().get();

    BOOST_REQUIRE_EQUAL(
      "a-token",
      std::get<cloud_roles::gcp_credentials>(c.value()).oauth_token());
}

FIXTURE_TEST(test_token_refresh_on_expiry, http_imposter_fixture) {
    // Token expires in 5 seconds
    auto short_token = R"json(
{"access_token":"a-token","expires_in":5,"token_type":"Bearer"}
)json";
    when().request(cloud_role_tests::gcp_url).then_reply_with(short_token);
    listen();

    ss::abort_source as;
    ss::gate gate;
    std::optional<cloud_roles::credentials> c;

    two_fetches s(c, as);
    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::gcp_instance_metadata,
      gate,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number});

    refresh.start();
    gate.close().get();

    BOOST_REQUIRE_EQUAL(
      "a-token",
      std::get<cloud_roles::gcp_credentials>(c.value()).oauth_token());

    BOOST_REQUIRE(
      has_calls_in_order(cloud_role_tests::gcp_url, cloud_role_tests::gcp_url));
}

FIXTURE_TEST(test_aws_credentials, http_imposter_fixture) {
    when()
      .request(cloud_role_tests::aws_role_query_url)
      .then_reply_with(cloud_role_tests::aws_role);

    when()
      .request(cloud_role_tests::aws_creds_url)
      .then_reply_with(cloud_role_tests::aws_creds);

    listen();

    ss::abort_source as;
    ss::gate gate;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c, as);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      gate,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number});

    refresh.start();
    gate.close().get();

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("my-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("my-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("my-token", aws_creds.session_token.value());

    BOOST_REQUIRE(has_calls_in_order(
      cloud_role_tests::aws_role_query_url, cloud_role_tests::aws_creds_url));
}

FIXTURE_TEST(test_short_lived_aws_credentials, http_imposter_fixture) {
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
    ss::gate gate;
    std::optional<cloud_roles::credentials> c;

    two_fetches s(c, as);

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::aws_instance_metadata,
      gate,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number});

    refresh.start();
    gate.close().get();

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("my-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("my-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("my-token", aws_creds.session_token.value());

    BOOST_REQUIRE(has_calls_in_order(
      cloud_role_tests::aws_role_query_url,
      cloud_role_tests::aws_creds_url,
      cloud_role_tests::aws_creds_url));
}

FIXTURE_TEST(test_sts_credentials, http_imposter_fixture) {
    setenv("AWS_ROLE_ARN", cloud_role_tests::aws_role, 1);
    setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "test_sts_creds_f", 1);

    when()
      .request("/")
      .with_method(ss::httpd::POST)
      .then_reply_with(cloud_role_tests::sts_creds);
    listen();

    ss::abort_source as;
    ss::gate gate;
    std::optional<cloud_roles::credentials> c;

    one_shot_fetch s(c, as);

    auto token_f = ss::open_file_dma(
                     "test_sts_creds_f",
                     ss::open_flags::create | ss::open_flags::rw)
                     .get0();
    ss::sstring token{"token"};
    token_f.dma_write(0, token.data(), token.size()).get0();

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::sts,
      gate,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number});

    refresh.start();
    gate.close().get();

    token_f.close().get0();
    ss::remove_file("test_sts_creds_f").get0();

    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("sts-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("sts-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("sts-token", aws_creds.session_token.value());
    BOOST_REQUIRE(has_call("/"));
}

FIXTURE_TEST(test_short_lived_sts_credentials, http_imposter_fixture) {
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
    ss::gate gate;
    std::optional<cloud_roles::credentials> c;

    two_fetches s(c, as);

    auto token_f = ss::open_file_dma(
                     "test_short_sts_f",
                     ss::open_flags::create | ss::open_flags::rw)
                     .get0();
    ss::sstring token{"token"};
    token_f.dma_write(0, token.data(), token.size()).get0();

    auto refresh = cloud_roles::make_refresh_credentials(
      model::cloud_credentials_source::sts,
      gate,
      as,
      s,
      cloud_roles::aws_region_name{""},
      net::unresolved_address{httpd_host_name.data(), httpd_port_number});

    refresh.start();
    gate.close().get();

    token_f.close().get0();
    ss::remove_file("test_short_sts_f").get0();
    auto aws_creds = std::get<cloud_roles::aws_credentials>(c.value());
    BOOST_REQUIRE_EQUAL("sts-key", aws_creds.access_key_id());
    BOOST_REQUIRE_EQUAL("sts-secret", aws_creds.secret_access_key());
    BOOST_REQUIRE_EQUAL("sts-token", aws_creds.session_token.value());

    BOOST_REQUIRE(has_calls_in_order("/", "/"));
}
