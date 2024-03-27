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

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/testing/thread_test_case.hh>

inline ss::logger test_log("test"); // NOLINT

SEASTAR_THREAD_TEST_CASE(test_refresh_client_built_according_to_source) {
    ss::abort_source as;
    cloud_roles::aws_region_name region{"atlantis"};
    {
        auto rc = cloud_roles::make_refresh_credentials(
          model::cloud_credentials_source::gcp_instance_metadata,
          as,
          [](auto) { return ss::now(); },
          region);
        BOOST_REQUIRE_EQUAL(
          "gcp_refresh_impl{address:{host: 169.254.169.254, port: 80}}",
          ssx::sformat("{}", rc));
    }

    {
        auto rc = cloud_roles::make_refresh_credentials(
          model::cloud_credentials_source::aws_instance_metadata,
          as,
          [](auto) { return ss::now(); },
          region);
        BOOST_REQUIRE_EQUAL(
          "aws_refresh_impl{address:{host: 169.254.169.254, port: 80}}",
          ssx::sformat("{}", rc));
    }

    {
        setenv("AWS_ROLE_ARN", "role", 1);
        setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "file_path", 1);

        auto rc = cloud_roles::make_refresh_credentials(
          model::cloud_credentials_source::sts,
          as,
          [](auto) { return ss::now(); },
          region);
        BOOST_REQUIRE_EQUAL(
          "aws_sts_refresh_impl{address:{host: sts.amazonaws.com, port: 443}}",
          ssx::sformat("{}", rc));
    }

    {
        setenv("AZURE_CLIENT_ID", "client_id", 1);
        setenv("AZURE_TENANT_ID", "tenant_id", 1);
        setenv("AZURE_FEDERATED_TOKEN_FILE", "file_path", 1);
        setenv("AZURE_AUTHORITY_HOST", "host.test.contoso.com", 1);

        auto rc = cloud_roles::make_refresh_credentials(
          model::cloud_credentials_source::azure_aks_oidc_federation,
          as,
          [](auto) { return ss::now(); },
          region);
        BOOST_REQUIRE_EQUAL(
          "azure_aks_refresh_impl{address:{host: host.test.contoso.com, port: "
          "443}}",
          ssx::sformat("{}", rc));
    }

    BOOST_REQUIRE_THROW(
      cloud_roles::make_refresh_credentials(
        model::cloud_credentials_source::config_file,
        as,
        [](auto) { return ss::now(); },
        region),
      std::invalid_argument);
}

SEASTAR_THREAD_TEST_CASE(
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

    {
        cloud_roles::abs_oauth_credentials akc{};
        auto applier = cloud_roles::make_credentials_applier(std::move(akc));
        BOOST_REQUIRE_EQUAL(
          "apply_abs_oauth_credentials", ssx::sformat("{}", applier));
    }
}
