/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/gssapi_principal_mapper.h"

#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>

namespace security {

namespace bdata = boost::unit_test::data;

struct gssapi_test_record {
    gssapi_test_record(
      std::string_view gssapi_principal_name,
      std::string_view expected_primary,
      std::string_view expected_host_name,
      std::string_view expected_realm)
      : gssapi_principal_name(gssapi_principal_name)
      , expected_primary(expected_primary)
      , expected_host_name(expected_host_name)
      , expected_realm(expected_realm) {}
    std::string_view gssapi_principal_name;
    std::string_view expected_primary;
    std::string_view expected_host_name;
    std::string_view expected_realm;

    friend std::ostream&
    operator<<(std::ostream& os, const gssapi_test_record& r) {
        fmt::print(
          os, "gssapi_principal_name: '{}', ", r.gssapi_principal_name);
        fmt::print(os, "expected_primary: '{}', ", r.expected_primary);
        fmt::print(os, "expected_host_name: '{}', ", r.expected_host_name);
        fmt::print(os, "expected_realm: '{}'", r.expected_realm);

        return os;
    }
};

static std::array<gssapi_test_record, 3> gssapi_name_test_data{
  gssapi_test_record{
    "App.service-name/example.com@REALM.com",
    "App.service-name",
    "example.com",
    "REALM.com"},
  {"App.service-name@REALM.com", "App.service-name", "", "REALM.com"},
  {"user/host@REALM.com", "user", "host", "REALM.com"}};

BOOST_DATA_TEST_CASE(test_gssapi_name, bdata::make(gssapi_name_test_data), c) {
    BOOST_REQUIRE_NO_THROW(
      auto name = gssapi_name::parse(c.gssapi_principal_name);
      BOOST_REQUIRE_EQUAL(c.expected_primary, name.primary());
      BOOST_REQUIRE_EQUAL(c.expected_host_name, name.host_name());
      BOOST_REQUIRE_EQUAL(c.expected_realm, name.realm());
      BOOST_REQUIRE_EQUAL(c.gssapi_principal_name, fmt::format("{}", name)););
}
} // namespace security
