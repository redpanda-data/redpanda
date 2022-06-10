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
#include "cloud_roles/refresh_credentials.h"
#include "config/node_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include <boost/test/unit_test.hpp>

inline ss::logger test_log("test"); // NOLINT

BOOST_AUTO_TEST_CASE(test_refresh_client_built_according_to_source) {
    ss::gate gate;
    ss::abort_source as;
    s3::aws_region_name region{"atlantis"};
    {
        auto rc = cloud_roles::make_refresh_credentials(
          model::cloud_credentials_source::gcp_instance_metadata,
          gate,
          as,
          [](auto) { return ss::now(); },
          region);
        BOOST_REQUIRE_EQUAL(
          "gcp_refresh_impl{host:169.254.169.254, port:80}",
          ssx::sformat("{}", rc));
    }

    {
        auto rc = cloud_roles::make_refresh_credentials(
          model::cloud_credentials_source::aws_instance_metadata,
          gate,
          as,
          [](auto) { return ss::now(); },
          region);
        BOOST_REQUIRE_EQUAL(
          "aws_refresh_impl{host:169.254.169.254, port:80}",
          ssx::sformat("{}", rc));
    }

    {
        setenv("AWS_ROLE_ARN", "role", 1);
        setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "file_path", 1);

        auto rc = cloud_roles::make_refresh_credentials(
          model::cloud_credentials_source::sts,
          gate,
          as,
          [](auto) { return ss::now(); },
          region);
        BOOST_REQUIRE_EQUAL(
          "aws_sts_refresh_impl{host:sts.amazonaws.com, port:443}",
          ssx::sformat("{}", rc));
    }

    BOOST_REQUIRE_THROW(
      cloud_roles::make_refresh_credentials(
        model::cloud_credentials_source::config_file,
        gate,
        as,
        [](auto) { return ss::now(); },
        region),
      std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(
  test_credential_applier_built_according_to_credential_kind) {
    {
        cloud_roles::gcp_credentials gc{};
        auto applier = cloud_roles::make_credentials_applier(std::move(gc));
        BOOST_REQUIRE_EQUAL(
          "apply_gcp_credentials", ssx::sformat("{}", applier));
    }

    {
        cloud_roles::aws_credentials ac{};
        auto applier = cloud_roles::make_credentials_applier(std::move(ac));
        BOOST_REQUIRE_EQUAL(
          "apply_aws_credentials", ssx::sformat("{}", applier));
    }
}
