// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "security/jwt.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/data/monomorphic/fwd.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>

namespace oidc = security::oidc;
namespace bdata = boost::unit_test::data;

struct parse_test_data {
    std::string_view data;
    oidc::errc err;
    friend std::ostream&
    operator<<(std::ostream& os, parse_test_data const& r) {
        fmt::print(os, "data: {}, errc: {}", r.data, r.err);
        return os;
    }
};
const auto metadata_data = std::to_array<parse_test_data>(
  {// Auth0
   {R"({"issuer":"https://dev-ltxchcls4igzho78.us.auth0.com/","authorization_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/authorize","token_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oauth/token","device_authorization_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oauth/device/code","userinfo_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/userinfo","mfa_challenge_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/mfa/challenge","jwks_uri":"https://dev-ltxchcls4igzho78.us.auth0.com/.well-known/jwks.json","registration_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oidc/register","revocation_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oauth/revoke","scopes_supported":["openid","profile","offline_access","name","given_name","family_name","nickname","email","email_verified","picture","created_at","identities","phone","address"],"response_types_supported":["code","token","id_token","code token","code id_token","token id_token","code token id_token"],"code_challenge_methods_supported":["S256","plain"],"response_modes_supported":["query","fragment","form_post"],"subject_types_supported":["public"],"id_token_signing_alg_values_supported":["HS256","RS256","PS256"],"token_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post","private_key_jwt"],"claims_supported":["aud","auth_time","created_at","email","email_verified","exp","family_name","given_name","iat","identities","iss","name","nickname","phone_number","picture","sub"],"request_uri_parameter_supported":false,"request_parameter_supported":false,"token_endpoint_auth_signing_alg_values_supported":["RS256","RS384","PS256"]})",
    oidc::errc::success},
   // Auth0 minimal
   {R"({"issuer":"https://dev-ltxchcls4igzho78.us.auth0.com/","jwks_uri":"https://dev-ltxchcls4igzho78.us.auth0.com/.well-known/jwks.json"})",
    oidc::errc::success},
   // No jwks_uri
   {R"({"issuer":"https://dev-ltxchcls4igzho78.us.auth0.com/"})",
    oidc::errc::metadata_invalid},
   // No issuer
   {R"({"jwks_uri":"https://dev-ltxchcls4igzho78.us.auth0.com/.well-known/jwks.json"})",
    oidc::errc::metadata_invalid},
   // Empty issuer
   {R"({"issuer":"","jwks_uri":"https://dev-ltxchcls4igzho78.us.auth0.com/.well-known/jwks.json"})",
    oidc::errc::metadata_invalid},
   // Empty jwks_uri
   {R"({"issuer":"https://dev-ltxchcls4igzho78.us.auth0.com/","jwks_uri":""})",
    oidc::errc::metadata_invalid},
   // Empty object
   {R"({})", oidc::errc::metadata_invalid},
   // Not object
   {R"("not_object")", oidc::errc::metadata_invalid}});
BOOST_DATA_TEST_CASE(test_metadata, bdata::make(metadata_data), d) {
    auto metadata = oidc::metadata::make(std::move(d.data));
    if (d.err == oidc::errc::success) {
        BOOST_REQUIRE(metadata.has_value());
    } else {
        BOOST_REQUIRE_EQUAL(d.err, metadata.error());
    }
}
