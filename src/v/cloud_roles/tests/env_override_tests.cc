/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/refresh_credentials.h"

#include <seastar/core/gate.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

SEASTAR_THREAD_TEST_CASE(test_override_address) {
    ss::abort_source as;
    cloud_roles::aws_region_name region{"atlantis"};

    setenv("RP_SI_CREDS_API_ADDRESS", "localhost:1234", 1);

    auto undo = ss::defer([] { unsetenv("RP_SI_CREDS_API_ADDRESS"); });

    for (const auto& [kind, name] : {
           std::pair{
             model::cloud_credentials_source::aws_instance_metadata,
             "aws_refresh_impl"},
           std::pair{
             model::cloud_credentials_source::gcp_instance_metadata,
             "gcp_refresh_impl"},
         }) {
        auto rc = cloud_roles::make_refresh_credentials(
          kind, as, [](auto) { return ss::now(); }, region);
        BOOST_REQUIRE_EQUAL(
          std::string(name) + "{address:{host: localhost, port: 1234}}",
          ssx::sformat("{}", rc));
    }
}

SEASTAR_THREAD_TEST_CASE(test_override_address_fails_on_bad_address) {
    ss::abort_source as;
    cloud_roles::aws_region_name region{"atlantis"};

    {
        setenv("RP_SI_CREDS_API_ADDRESS", "localhost:", 1);
        auto undo = ss::defer([] { unsetenv("RP_SI_CREDS_API_ADDRESS"); });
        BOOST_REQUIRE_EXCEPTION(
          cloud_roles::make_refresh_credentials(
            model::cloud_credentials_source::aws_instance_metadata,
            as,
            [](auto) { return ss::now(); },
            region),
          std::invalid_argument,
          [](const auto& ex) {
              ss::sstring what{ex.what()};
              return what.find("found: localhost:") != what.npos;
          });
    }

    {
        setenv("RP_SI_CREDS_API_ADDRESS", ":1234", 1);
        auto undo = ss::defer([] { unsetenv("RP_SI_CREDS_API_ADDRESS"); });
        BOOST_REQUIRE_EXCEPTION(
          cloud_roles::make_refresh_credentials(
            model::cloud_credentials_source::aws_instance_metadata,
            as,
            [](auto) { return ss::now(); },
            region),
          std::invalid_argument,
          [](const auto& ex) {
              ss::sstring what{ex.what()};
              return what.find("found: :1234") != what.npos;
          });
    }

    {
        setenv("RP_SI_CREDS_API_ADDRESS", "localhost", 1);
        auto undo = ss::defer([] { unsetenv("RP_SI_CREDS_API_ADDRESS"); });
        BOOST_REQUIRE_EXCEPTION(
          cloud_roles::make_refresh_credentials(
            model::cloud_credentials_source::aws_instance_metadata,
            as,
            [](auto) { return ss::now(); },
            region),
          std::invalid_argument,
          [](const auto& ex) {
              ss::sstring what{ex.what()};
              return what.find("found: localhost") != what.npos;
          });
    }

    {
        setenv("RP_SI_CREDS_API_ADDRESS", "localhost:1234xxx", 1);
        auto undo = ss::defer([] { unsetenv("RP_SI_CREDS_API_ADDRESS"); });
        BOOST_REQUIRE_EXCEPTION(
          cloud_roles::make_refresh_credentials(
            model::cloud_credentials_source::aws_instance_metadata,
            as,
            [](auto) { return ss::now(); },
            region),
          std::invalid_argument,
          [](const auto& ex) {
              ss::sstring what{ex.what()};
              return what.find("failed to convert 1234xxx") != what.npos;
          });
    }

    {
        setenv("RP_SI_CREDS_API_ADDRESS", "localhost:axxx", 1);
        auto undo = ss::defer([] { unsetenv("RP_SI_CREDS_API_ADDRESS"); });
        BOOST_REQUIRE_EXCEPTION(
          cloud_roles::make_refresh_credentials(
            model::cloud_credentials_source::aws_instance_metadata,
            as,
            [](auto) { return ss::now(); },
            region),
          std::invalid_argument,
          [](const auto& ex) {
              ss::sstring what{ex.what()};
              return what.find("failed to convert axxx") != what.npos;
          });
    }
}
