// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/property.h"
#include "json/document.h"
#include "security/jwt.h"
#include "security/oidc_principal_mapping_applicator.h"

#include <seastar/core/sstring.hh>

#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>

namespace bdata = boost::unit_test::data;

struct principal_mapping_test_data {
    std::string_view token_payload;
    config::binding<ss::sstring> mapping;
    result<security::acl_principal> principal;

    friend std::ostream&
    operator<<(std::ostream& os, const principal_mapping_test_data& d) {
        fmt::print(
          os,
          "payload: {}, mapping: {}, principal: {}",
          d.token_payload,
          d.mapping(),
          d.principal.has_error()
            ? std::string_view{d.principal.assume_error().message()}
            : std::string_view(d.principal.assume_value().name()));
        return os;
    }
};
const auto principal_mapping_data = std::to_array<principal_mapping_test_data>(
  {// sub (default)
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject",
     "aud": ["redpanda", "wrong"], "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.sub"),
    security::acl_principal{security::principal_type::user, "subject"}},
   // empty sub
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "", "aud":
     ["redpanda", "wrong"], "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.sub"),
    security::oidc::errc::jwt_invalid_principal},
   // no sub
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "aud":
     ["redpanda", "wrong"], "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.sub"),
    security::oidc::errc::jwt_invalid_principal},
   // email
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject",
     "email": "user@example.com", "aud": ["redpanda", "wrong"], "exp":
     1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.email"),
    security::acl_principal{
      security::principal_type::user, "user@example.com"}},
   // empty email
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject",
     "email": "", "aud": ["redpanda", "wrong"], "exp": 1695887942, "iat":
     1695887942})",
    config::mock_binding<ss::sstring>("$.email"),
    security::oidc::errc::jwt_invalid_principal},
   // no email
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject",
     "aud": ["redpanda", "wrong"], "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.email"),
    security::oidc::errc::jwt_invalid_principal},
   // nested principal
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject",
     "user_info": {"name": "user", "email": "user@example.com"},  "aud":
     "redpanda", "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.user_info.email"),
    security::acl_principal{
      security::principal_type::user, "user@example.com"}},
   // invalid nested principal
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject",
     "user_info": "not object",  "aud": "redpanda", "exp": 1695887942, "iat":
     1695887942})",
    config::mock_binding<ss::sstring>("$.user_info.email"),
    security::oidc::errc::jwt_invalid_principal},
   // extract user from email
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "email": "user@example.com", "aud": "redpanda", "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.email/([^@]+)@.*/$1/"),
    security::acl_principal{security::principal_type::user, "user"}},
   // extract uppercase user from email
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "email": "user@example.com", "aud": "redpanda", "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.email/([^@]+)@.*/$1/U"),
    security::acl_principal{security::principal_type::user, "USER"}},
   // extract lowercase user from email
   {R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "email": "USER@example.com", "aud": "redpanda", "exp": 1695887942, "iat": 1695887942})",
    config::mock_binding<ss::sstring>("$.email/([^@]+)@.*/$1/L"),
    security::acl_principal{security::principal_type::user, "user"}}});
BOOST_DATA_TEST_CASE(
  test_principal_mapper, bdata::make(principal_mapping_data), d) {
    auto mapping = security::oidc::parse_principal_mapping_rule(d.mapping());
    BOOST_REQUIRE(mapping.has_value());
    auto mapper = std::move(mapping).assume_value();

    json::Document header;
    header.Parse(R"({"alg": "RS256", "typ": "JWT", "kid": "42"})");
    json::Document payload;
    payload.Parse(d.token_payload.data(), d.token_payload.length());
    auto jwt = security::oidc::jwt::make(std::move(header), std::move(payload));
    BOOST_REQUIRE(!jwt.has_error());

    auto principal = principal_mapping_rule_apply(mapper, jwt.assume_value());
    if (
      d.principal.has_error()
      && d.principal.assume_error() != security::oidc::errc::success) {
        BOOST_REQUIRE(principal.has_error());
        BOOST_REQUIRE_EQUAL(
          principal.assume_error(), d.principal.assume_error());
        return;
    }

    BOOST_REQUIRE(principal.has_value());
    BOOST_REQUIRE_EQUAL(principal.assume_value(), d.principal.assume_value());
}

namespace {
result<security::oidc::jwt> make_test_jwt(std::string_view payload_json) {
    json::Document header;
    header.Parse(R"({"alg": "RS256", "typ": "JWT", "kid": "42"})");

    json::Document payload;
    payload.Parse(payload_json.data(), payload_json.length());

    return security::oidc::jwt::make(std::move(header), std::move(payload));
}
} // namespace

BOOST_AUTO_TEST_CASE(test_get_group_claim_string_array) {
    // Test: Group claim as a string array
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "groups": ["admin", "developers", "users"]
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(result.has_value());
    auto groups = std::move(result).value();
    BOOST_REQUIRE_EQUAL(groups.size(), 3);
    BOOST_CHECK_EQUAL(groups[0], "admin");
    BOOST_CHECK_EQUAL(groups[1], "developers");
    BOOST_CHECK_EQUAL(groups[2], "users");
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_single_string) {
    // Test: Group claim as a single string (no comma)
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "groups": "admin"
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(result.has_value());
    auto groups = std::move(result).value();
    BOOST_REQUIRE_EQUAL(groups.size(), 1);
    BOOST_CHECK_EQUAL(groups[0], "admin");
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_comma_separated_string) {
    // Test: Group claim as comma-separated string
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "groups": "admin,developers,users"
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(result.has_value());
    auto groups = std::move(result).value();
    BOOST_REQUIRE_EQUAL(groups.size(), 3);
    BOOST_CHECK_EQUAL(groups[0], "admin");
    BOOST_CHECK_EQUAL(groups[1], "developers");
    BOOST_CHECK_EQUAL(groups[2], "users");
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_missing) {
    // Test: Missing group claim
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123"
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(!result.has_value());
    BOOST_CHECK_EQUAL(
      result.error(), security::oidc::errc::group_claim_not_found);
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_empty_array) {
    // Test: Empty group array
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "groups": []
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(result.has_value());
    auto groups = std::move(result).value();
    BOOST_CHECK_EQUAL(groups.size(), 0);
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_empty_string) {
    // Test: Empty string (results in one empty element after split)
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "groups": ""
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(result.has_value());
    auto groups = std::move(result).value();
    // boost::split on empty string produces one empty element
    BOOST_REQUIRE_EQUAL(groups.size(), 1);
    BOOST_CHECK_EQUAL(groups[0], "");
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_nested_path) {
    // Test: Nested group claim path
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "resource_access": {
            "redpanda": {
                "roles": ["admin", "user"]
            }
        }
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/resource_access/redpanda/roles");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(result.has_value());
    auto groups = std::move(result).value();
    BOOST_REQUIRE_EQUAL(groups.size(), 2);
    BOOST_CHECK_EQUAL(groups[0], "admin");
    BOOST_CHECK_EQUAL(groups[1], "user");
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_invalid_type) {
    // Test: Group claim with invalid type (number)
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "groups": 123
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(!result.has_value());
    BOOST_CHECK_EQUAL(
      result.error(), security::oidc::errc::group_claim_not_found);
}

BOOST_AUTO_TEST_CASE(test_get_group_claim_comma_with_spaces) {
    // Test: Comma-separated string with spaces around commas
    auto jwt = make_test_jwt(R"({
        "iss": "http://example.com",
        "sub": "user123",
        "groups": "admin, developers, users"
    })");
    BOOST_REQUIRE(jwt.has_value());

    json::Pointer group_pointer("/groups");
    auto result = security::oidc::detail::get_group_claim(
      group_pointer, jwt.assume_value());

    BOOST_REQUIRE(result.has_value());
    auto groups = std::move(result).value();
    BOOST_REQUIRE_EQUAL(groups.size(), 3);
    // Note: boost::split includes the spaces
    // TODO: Update this so that leading/trailing spaces are trimmed.
    BOOST_CHECK_EQUAL(groups[0], "admin");
    BOOST_CHECK_EQUAL(groups[1], " developers");
    BOOST_CHECK_EQUAL(groups[2], " users");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_none_simple) {
    // Test: nested_group_behavior::none with simple group name
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "admin", security::oidc::nested_group_behavior::none);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "admin");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_none_nested) {
    // Test: nested_group_behavior::none with nested group name
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "parent/child/admin", security::oidc::nested_group_behavior::none);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "parent/child/admin");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_none_empty) {
    // Test: nested_group_behavior::none with empty string
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "", security::oidc::nested_group_behavior::none);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_none_trailing_slash) {
    // Test: nested_group_behavior::none with trailing slash
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "parent/child/", security::oidc::nested_group_behavior::none);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "parent/child/");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_none_leading_slash) {
    // Test: nested_group_behavior::none with leading slash
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "/admin", security::oidc::nested_group_behavior::none);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "/admin");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_suffix_simple) {
    // Test: nested_group_behavior::suffix with simple group name (no slashes)
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "admin", security::oidc::nested_group_behavior::suffix);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "admin");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_suffix_single_level) {
    // Test: nested_group_behavior::suffix with single-level nested group
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "parent/child", security::oidc::nested_group_behavior::suffix);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "child");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_suffix_multi_level) {
    // Test: nested_group_behavior::suffix with multi-level nested group
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "a/b/c/d", security::oidc::nested_group_behavior::suffix);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "d");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_suffix_trailing_slash) {
    // Test: nested_group_behavior::suffix with trailing slash
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "parent/child/", security::oidc::nested_group_behavior::suffix);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_suffix_leading_slash) {
    // Test: nested_group_behavior::suffix with leading slash
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "/admin", security::oidc::nested_group_behavior::suffix);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "admin");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_suffix_only_slash) {
    // Test: nested_group_behavior::suffix with only slash
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "/", security::oidc::nested_group_behavior::suffix);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "");
}

BOOST_AUTO_TEST_CASE(test_apply_nested_group_policy_suffix_empty) {
    // Test: nested_group_behavior::suffix with empty string
    auto principal = security::oidc::detail::apply_nested_group_policy(
      "", security::oidc::nested_group_behavior::suffix);

    BOOST_CHECK_EQUAL(principal.type(), security::principal_type::group);
    BOOST_CHECK_EQUAL(principal.name(), "");
}
