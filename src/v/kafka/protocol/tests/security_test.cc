// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#define BOOST_TEST_MODULE example
#include "kafka/protocol/schemata/sasl_authenticate_request.h"
#include "kafka/protocol/schemata/sasl_authenticate_response.h"
#include "kafka/server/handlers/details/security.h"
#include "random/generators.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "utils/base64.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

namespace kafka {

BOOST_AUTO_TEST_CASE(to_acl_principal) {
    BOOST_REQUIRE_THROW(
      details::to_acl_principal(""), details::acl_conversion_error);
    BOOST_REQUIRE_THROW(
      details::to_acl_principal("XXX"), details::acl_conversion_error);
    BOOST_REQUIRE_THROW(
      details::to_acl_principal("User:"), details::acl_conversion_error);

    BOOST_REQUIRE_EQUAL(
      details::to_acl_principal("User:*"), security::acl_wildcard_user);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_principal("User:*").type(),
      security::principal_type::user);
    BOOST_REQUIRE_EQUAL(details::to_acl_principal("User:*").name(), "*");

    BOOST_REQUIRE_EQUAL(
      details::to_acl_principal("User:Fred").type(),
      security::principal_type::user);
    BOOST_REQUIRE_EQUAL(details::to_acl_principal("User:Fred").name(), "Fred");
}

BOOST_AUTO_TEST_CASE(to_acl_host) {
    BOOST_REQUIRE_THROW(
      details::to_acl_host("a.b.c.d"), details::acl_conversion_error);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_host("*"), security::acl_host::wildcard_host());
    BOOST_REQUIRE(details::to_acl_host("1.2.3.4").address());
}

BOOST_AUTO_TEST_CASE(to_resource_type) {
    BOOST_REQUIRE_THROW(
      details::to_resource_type(0), details::acl_conversion_error);
    BOOST_REQUIRE_THROW(
      details::to_resource_type(1), details::acl_conversion_error);
    BOOST_REQUIRE_EQUAL(
      details::to_resource_type(2), security::resource_type::topic);
    BOOST_REQUIRE_EQUAL(
      details::to_resource_type(3), security::resource_type::group);
    BOOST_REQUIRE_EQUAL(
      details::to_resource_type(4), security::resource_type::cluster);
    BOOST_REQUIRE_EQUAL(
      details::to_resource_type(5), security::resource_type::transactional_id);
    BOOST_REQUIRE_THROW(
      details::to_resource_type(6), details::acl_conversion_error);
}

BOOST_AUTO_TEST_CASE(to_pattern_type) {
    BOOST_REQUIRE_THROW(
      details::to_pattern_type(0), details::acl_conversion_error);
    BOOST_REQUIRE_THROW(
      details::to_pattern_type(1), details::acl_conversion_error);
    BOOST_REQUIRE_THROW(
      details::to_pattern_type(2), details::acl_conversion_error);
    BOOST_REQUIRE_EQUAL(
      details::to_pattern_type(3), security::pattern_type::literal);
    BOOST_REQUIRE_EQUAL(
      details::to_pattern_type(4), security::pattern_type::prefixed);
    BOOST_REQUIRE_THROW(
      details::to_pattern_type(5), details::acl_conversion_error);
}

BOOST_AUTO_TEST_CASE(to_acl_operation) {
    BOOST_REQUIRE_THROW(
      details::to_acl_operation(0), details::acl_conversion_error);
    BOOST_REQUIRE_THROW(
      details::to_acl_operation(1), details::acl_conversion_error);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(2), security::acl_operation::all);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(3), security::acl_operation::read);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(4), security::acl_operation::write);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(5), security::acl_operation::create);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(6), security::acl_operation::remove);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(7), security::acl_operation::alter);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(8), security::acl_operation::describe);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(9), security::acl_operation::cluster_action);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(10), security::acl_operation::describe_configs);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(11), security::acl_operation::alter_configs);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_operation(12), security::acl_operation::idempotent_write);
    BOOST_REQUIRE_THROW(
      details::to_acl_operation(13), details::acl_conversion_error);
}

BOOST_AUTO_TEST_CASE(to_acl_permission) {
    BOOST_REQUIRE_THROW(
      details::to_acl_permission(0), details::acl_conversion_error);
    BOOST_REQUIRE_THROW(
      details::to_acl_permission(1), details::acl_conversion_error);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_permission(2), security::acl_permission::deny);
    BOOST_REQUIRE_EQUAL(
      details::to_acl_permission(3), security::acl_permission::allow);
    BOOST_REQUIRE_THROW(
      details::to_acl_permission(4), details::acl_conversion_error);
}

BOOST_AUTO_TEST_CASE(redact_sensitive_messages) {
    BOOST_REQUIRE_EQUAL(
      "{auth_bytes=****}", fmt::to_string(sasl_authenticate_request_data{}));

    BOOST_REQUIRE_EQUAL(
      "{error_code={ error_code: none [0] } error_message={nullopt} "
      "auth_bytes=**** "
      "session_lifetime_ms=0}",
      fmt::to_string(sasl_authenticate_response_data{}));
}

} // namespace kafka
