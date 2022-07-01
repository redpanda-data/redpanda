/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/apply_credentials.h"

#include <boost/test/unit_test.hpp>

namespace bh = boost::beast::http;

BOOST_AUTO_TEST_CASE(test_gcp_headers) {
    auto applier = cloud_roles::make_credentials_applier(
      cloud_roles::gcp_credentials{
        .oauth_token = cloud_roles::oauth_token_str{"a-token"}});

    {
        bh::request_header<> h{};
        applier.add_auth(h);
        BOOST_REQUIRE_EQUAL(h.at(bh::field::authorization), "Bearer a-token");
    }

    applier.reset_creds(cloud_roles::gcp_credentials{
      .oauth_token = cloud_roles::oauth_token_str{"second-token"}});

    {
        bh::request_header<> h{};
        applier.add_auth(h);
        BOOST_REQUIRE_EQUAL(
          h.at(bh::field::authorization), "Bearer second-token");
    }
}

BOOST_AUTO_TEST_CASE(test_aws_headers) {
    auto applier = cloud_roles::make_credentials_applier(
      cloud_roles::aws_credentials{
        .access_key_id = s3::public_key_str{"pub"},
        .secret_access_key = s3::private_key_str{"priv"},
        .session_token = cloud_roles::s3_session_token{"tok"},
      });

    {
        bh::request_header<> h{};
        h.method(bh::verb::put);
        applier.add_auth(h);
        fmt::print("{}", h);
        BOOST_REQUIRE_EQUAL(h.at("x-amz-security-token"), "tok");
        // put request contains unsigned payload
        BOOST_REQUIRE_EQUAL(h.at("x-amz-content-sha256"), "UNSIGNED-PAYLOAD");
    }

    applier.reset_creds(cloud_roles::aws_credentials{
      .access_key_id = s3::public_key_str{"pub2"},
      .secret_access_key = s3::private_key_str{"priv2"},
      .session_token = cloud_roles::s3_session_token{"tok2"},
    });

    {
        bh::request_header<> h{};
        h.method(bh::verb::get);
        applier.add_auth(h);
        fmt::print("{}", h);
        BOOST_REQUIRE_EQUAL(h.at("x-amz-security-token"), "tok2");
        // get request contains empty signature
        BOOST_REQUIRE_EQUAL(
          h.at("x-amz-content-sha256"),
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }
}