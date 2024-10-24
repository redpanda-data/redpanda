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
