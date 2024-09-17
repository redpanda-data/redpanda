// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "security/jwt.h"
#include "security/oidc_authenticator.h"
#include "security/oidc_principal_mapping.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/data/monomorphic/fwd.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <fmt/chrono.h>

namespace oidc = security::oidc;
namespace bdata = boost::unit_test::data;

using namespace std::chrono_literals;

struct parse_test_data {
    std::string_view data;
    oidc::errc err;
    friend std::ostream&
    operator<<(std::ostream& os, const parse_test_data& r) {
        fmt::print(os, "data: {}, errc: {}", r.data, r.err);
        return os;
    }
};
const auto metadata_data
  = std::
    to_array<parse_test_data>(
      {// Auth0
       {R"({"issuer":"https://dev-ltxchcls4igzho78.us.auth0.com/","authorization_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/authorize","token_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oauth/token","device_authorization_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oauth/device/code","userinfo_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/userinfo","mfa_challenge_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/mfa/challenge","jwks_uri":"https://dev-ltxchcls4igzho78.us.auth0.com/.well-known/jwks.json","registration_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oidc/register","revocation_endpoint":"https://dev-ltxchcls4igzho78.us.auth0.com/oauth/revoke","scopes_supported":["openid","profile","offline_access","name","given_name","family_name","nickname","email","email_verified","picture","created_at","identities","phone","address"],"response_types_supported":["code","token","id_token","code token","code id_token","token id_token","code token id_token"],"code_challenge_methods_supported":["S256","plain"],"response_modes_supported":["query","fragment","form_post"],"subject_types_supported":["public"],"id_token_signing_alg_values_supported":["HS256","RS256","PS256"],"token_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post","private_key_jwt"],"claims_supported":["aud","auth_time","created_at","email","email_verified","exp","family_name","given_name","iat","identities","iss","name","nickname","phone_number","picture","sub"],"request_uri_parameter_supported":false,"request_parameter_supported":false,"token_endpoint_auth_signing_alg_values_supported":["RS256","RS384","PS256"]})",
        oidc::errc::success},
       // Auth0 minimal
       {R"({"issuer":"https://dev-ltxchcls4igzho78.us.auth0.com/","jwks_uri":"https://dev-ltxchcls4igzho78.us.auth0.com/.well-known/jwks.json"})",
        oidc::errc::success},
       // Keycloak
       {R"({"issuer":"http://docker-rp-1:8080/realms/demorealm","authorization_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/auth","token_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token","introspection_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token/introspect","userinfo_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/userinfo","end_session_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/logout","frontchannel_logout_session_supported":true,"frontchannel_logout_supported":true,"jwks_uri":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/certs","check_session_iframe":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/login-status-iframe.html","grant_types_supported":["authorization_code","implicit","refresh_token","password","client_credentials","urn:ietf:params:oauth:grant-type:device_code","urn:openid:params:grant-type:ciba"],"acr_values_supported":["0","1"],"response_types_supported":["code","none","id_token","token","id_token token","code id_token","code token","code id_token token"],"subject_types_supported":["public","pairwise"],"id_token_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"id_token_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"id_token_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"userinfo_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512","none"],"userinfo_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"userinfo_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"request_object_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512","none"],"request_object_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"request_object_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"response_modes_supported":["query","fragment","form_post","query.jwt","fragment.jwt","form_post.jwt","jwt"],"registration_endpoint":"http://docker-rp-1:8080/realms/demorealm/clients-registrations/openid-connect","token_endpoint_auth_methods_supported":["private_key_jwt","client_secret_basic","client_secret_post","tls_client_auth","client_secret_jwt"],"token_endpoint_auth_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"introspection_endpoint_auth_methods_supported":["private_key_jwt","client_secret_basic","client_secret_post","tls_client_auth","client_secret_jwt"],"introspection_endpoint_auth_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"authorization_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"authorization_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"authorization_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"claims_supported":["aud","sub","iss","auth_time","name","given_name","family_name","preferred_username","email","acr"],"claim_types_supported":["normal"],"claims_parameter_supported":true,"scopes_supported":["openid","web-origins","microprofile-jwt","roles","email","offline_access","profile","phone","acr","address"],"request_parameter_supported":true,"request_uri_parameter_supported":true,"require_request_uri_registration":true,"code_challenge_methods_supported":["plain","S256"],"tls_client_certificate_bound_access_tokens":true,"revocation_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/revoke","revocation_endpoint_auth_methods_supported":["private_key_jwt","client_secret_basic","client_secret_post","tls_client_auth","client_secret_jwt"],"revocation_endpoint_auth_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"backchannel_logout_supported":true,"backchannel_logout_session_supported":true,"device_authorization_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/auth/device","backchannel_token_delivery_modes_supported":["poll","ping"],"backchannel_authentication_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/ciba/auth","backchannel_authentication_request_signing_alg_values_supported":["PS384","ES384","RS384","ES256","RS256","ES512","PS256","PS512","RS512"],"require_pushed_authorization_requests":false,"pushed_authorization_request_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/par/request","mtls_endpoint_aliases":{"token_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token","revocation_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/revoke","introspection_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token/introspect","device_authorization_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/auth/device","registration_endpoint":"http://docker-rp-1:8080/realms/demorealm/clients-registrations/openid-connect","userinfo_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/userinfo","pushed_authorization_request_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/par/request","backchannel_authentication_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/ciba/auth"}})",
        oidc::errc::success},
       // Azure AD
       {R"({"token_endpoint":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/oauth2/v2.0/token","token_endpoint_auth_methods_supported":["client_secret_post","private_key_jwt","client_secret_basic"],"jwks_uri":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/discovery/v2.0/keys","response_modes_supported":["query","fragment","form_post"],"subject_types_supported":["pairwise"],"id_token_signing_alg_values_supported":["RS256"],"response_types_supported":["code","id_token","codeid_token","id_tokentoken"],"scopes_supported":["openid","profile","email","offline_access"],"issuer":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/v2.0","request_uri_parameter_supported":false,"userinfo_endpoint":"https://graph.microsoft.com/oidc/userinfo","authorization_endpoint":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/oauth2/v2.0/authorize","device_authorization_endpoint":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/oauth2/v2.0/devicecode","http_logout_supported":true,"frontchannel_logout_supported":true,"end_session_endpoint":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/oauth2/v2.0/logout","claims_supported":["sub","iss","cloud_instance_name","cloud_instance_host_name","cloud_graph_host_name","msgraph_host","aud","exp","iat","auth_time","acr","nonce","preferred_username","name","tid","ver","at_hash","c_hash","email"],"kerberos_endpoint":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/kerberos","tenant_region_scope":"NA","cloud_instance_name":"microsoftonline.com","cloud_graph_host_name":"graph.windows.net","msgraph_host":"graph.microsoft.com","rbac_url":"https://pas.windows.net"})",
        oidc::errc::success},
       // Google ID Platform
       {R"({"issuer":"https://accounts.google.com","authorization_endpoint":"https://accounts.google.com/o/oauth2/v2/auth","device_authorization_endpoint":"https://oauth2.googleapis.com/device/code","token_endpoint":"https://oauth2.googleapis.com/token","userinfo_endpoint":"https://openidconnect.googleapis.com/v1/userinfo","revocation_endpoint":"https://oauth2.googleapis.com/revoke","jwks_uri":"https://www.googleapis.com/oauth2/v3/certs","response_types_supported":["code","token","id_token","codetoken","codeid_token","tokenid_token","codetokenid_token","none"],"subject_types_supported":["public"],"id_token_signing_alg_values_supported":["RS256"],"scopes_supported":["openid","email","profile"],"token_endpoint_auth_methods_supported":["client_secret_post","client_secret_basic"],"claims_supported":["aud","email","email_verified","exp","family_name","given_name","iat","iss","locale","name","picture","sub"],"code_challenge_methods_supported":["plain","S256"],"grant_types_supported":["authorization_code","refresh_token","urn:ietf:params:oauth:grant-type:device_code","urn:ietf:params:oauth:grant-type:jwt-bearer"]})", oidc::errc::success},
       // Okta example
       {R"({"issuer":"https://dev-69785679.okta.com/oauth2/default","authorization_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/authorize","token_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/token","userinfo_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/userinfo","registration_endpoint":"https://dev-69785679.okta.com/oauth2/v1/clients","jwks_uri":"https://dev-69785679.okta.com/oauth2/default/v1/keys","response_types_supported":["code","id_token","codeid_token","codetoken","id_tokentoken","codeid_tokentoken"],"response_modes_supported":["query","fragment","form_post","okta_post_message"],"grant_types_supported":["authorization_code","implicit","refresh_token","password","urn:ietf:params:oauth:grant-type:device_code","urn:openid:params:grant-type:ciba"],"subject_types_supported":["public"],"id_token_signing_alg_values_supported":["RS256"],"scopes_supported":["okta.myAccount.appAuthenticator.maintenance.manage","okta.myAccount.appAuthenticator.maintenance.read","okta.myAccount.appAuthenticator.manage","okta.myAccount.appAuthenticator.read","okta.myAccount.email.manage","okta.myAccount.email.read","okta.myAccount.manage","okta.myAccount.phone.manage","okta.myAccount.phone.read","okta.myAccount.profile.manage","okta.myAccount.profile.read","okta.myAccount.read","openid","profile","email","address","phone","offline_access","device_sso"],"token_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post","client_secret_jwt","private_key_jwt","none"],"claims_supported":["iss","ver","sub","aud","iat","exp","jti","auth_time","amr","idp","nonce","name","nickname","preferred_username","given_name","middle_name","family_name","email","email_verified","profile","zoneinfo","locale","address","phone_number","picture","website","gender","birthdate","updated_at","at_hash","c_hash"],"code_challenge_methods_supported":["S256"],"introspection_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/introspect","introspection_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post","client_secret_jwt","private_key_jwt","none"],"revocation_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/revoke","revocation_endpoint_auth_methods_supported":["client_secret_basic","client_secret_post","client_secret_jwt","private_key_jwt","none"],"end_session_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/logout","request_parameter_supported":true,"request_object_signing_alg_values_supported":["HS256","HS384","HS512","RS256","RS384","RS512","ES256","ES384","ES512"],"device_authorization_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/device/authorize","pushed_authorization_request_endpoint":"https://dev-69785679.okta.com/oauth2/default/v1/par","backchannel_token_delivery_modes_supported":["poll"],"backchannel_authentication_request_signing_alg_values_supported":["HS256","HS384","HS512","RS256","RS384","RS512","ES256","ES384","ES512"]})",
        oidc::errc::success},
       // AD-FS example
       {R"({"issuer":"https://EC2AMAZ-5A2FKI6.REDPANDA.COM/adfs","authorization_endpoint":"https://ec2amaz-5a2fki6.redpanda.com/adfs/oauth2/authorize/","token_endpoint":"https://ec2amaz-5a2fki6.redpanda.com/adfs/oauth2/token/","jwks_uri":"https://ec2amaz-5a2fki6.redpanda.com/adfs/discovery/keys","token_endpoint_auth_methods_supported":["client_secret_post","client_secret_basic","private_key_jwt","windows_client_authentication"],"response_types_supported":["code","id_token","codeid_token","id_tokentoken","codetoken","codeid_tokentoken"],"response_modes_supported":["query","fragment","form_post"],"grant_types_supported":["authorization_code","refresh_token","client_credentials","urn:ietf:params:oauth:grant-type:jwt-bearer","implicit","password","srv_challenge","urn:ietf:params:oauth:grant-type:device_code","device_code"],"subject_types_supported":["pairwise"],"scopes_supported":["allatclaims","openid","email","aza","user_impersonation","profile","logon_cert","winhello_cert","vpn_cert"],"id_token_signing_alg_values_supported":["RS256"],"token_endpoint_auth_signing_alg_values_supported":["RS256"],"access_token_issuer":"http://EC2AMAZ-5A2FKI6.REDPANDA.COM/adfs/services/trust","claims_supported":["aud","iss","iat","exp","auth_time","nonce","at_hash","c_hash","sub","upn","unique_name","pwd_url","pwd_exp","mfa_auth_time","sid","nbf"],"microsoft_multi_refresh_token":true,"userinfo_endpoint":"https://ec2amaz-5a2fki6.redpanda.com/adfs/userinfo","capabilities":["kdf_ver2"],"end_session_endpoint":"https://ec2amaz-5a2fki6.redpanda.com/adfs/oauth2/logout","as_access_token_token_binding_supported":true,"as_refresh_token_token_binding_supported":true,"resource_access_token_token_binding_supported":true,"op_id_token_token_binding_supported":true,"rp_id_token_token_binding_supported":true,"frontchannel_logout_supported":true,"frontchannel_logout_session_supported":true,"device_authorization_endpoint":"https://ec2amaz-5a2fki6.redpanda.com/adfs/oauth2/devicecode"})",
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

const auto jwks_data = std::to_array<parse_test_data>({
  // Auth0
  {R"({"keys":[{"kty":"RSA","use":"sig","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","e":"AQAB","kid":"tMQzailSAdaW4nojXxES9","x5t":"_LDYs6FFeEvJ-ng_a5-cxjUYaRw","x5c":["MIIDHTCCAgWgAwIBAgIJGbkt6m2aGpdZMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDRaFw0zNzA1MTUxMDM4NDRaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM7PGZNYQ/YYfFxn18ttSucs63mX+0fb2E0EzVRinQwx3we2EWwtRUcIuqRnGtxJUB7R+ZeEvdzFGkLZDfCWCPuWMhs8QTlcosHXP6ifozVmM1XWV+z7gqPod4pASZA+TfX/W/s79HP7h4bZbsx0JIWgsVSZ+beF0ORwJtan8SYKLVV260wxgHWRIe6eUSNpFPEQBhOvES1FbABl3Xn7O90BlaTP7hjjZanzEJNOXsMktoqqNeDHzMFJ+tIERJNcO64jBib3XRAE3vF3zk2cBK7H/GXBMVzFTnAZiJ+veMtQmcei4pq1iM1sp77guofDhQgu+iP04jx6ojGi16mjWm0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUIVqxco8b4MbVi3oKXvpvTQcAgxAwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQAz2efVzpAKHu7l7iPlBSRNRbXDnrxLR54ZtEX025l0yPdfM9fhPECYe9s2ze2TDupu2Is6XFk7ur5lUnON3qEKUU1kLRygz8oaj9lqsXdrfg9aTwXpxywWUelUKjLUL7FFZrXITSlG8SvwioTocpWPDaaDw0xefXnZRay5jjc9pl4E9uvd6K50SyQr7mY1ZEmNSYSftaoJGorFROaZs8Q0dc998JleYG1kFN0788eycCn4aRa0IKD/RfMXYj0j61T66vKnCALUfzFVtd/BUNwWdu0kRQceeuca4A+GWWxvYbDa4wJ/hEzWXT71BHUM6OhW4ls91wNTO9jdId/WJ3Rx"],"alg":"RS256"},{"kty":"RSA","use":"sig","n":"sP1lZhMdFVBrS06tFjwtuY0oRxDcZ8vPzUyUA5-vULpihTFDM-Jkeskvi3lAsZVkIv8iJVGSqdoBQyr3c27DWfDsUnH1HY1vGI6oB2m61uemCir104P07J6sZwO46hRnjp5vub2vJMjN_o4BOD2XiYXsTLg2gXsuh32HHKOr7ljbEZm4ygLeDVknGsSRIROxRWE8VPWjTQYRktAzwW8SMXI1wWxvg8wI6sKI4ydBMhQHO8ZomcIzdo66H31a45j2Jxn5JxKy-fMJbMg3qfTVh_9FMIOAjdVqtPN1g0TkoI8Y1H_iqkGq7tvqURnHBsbVkxwnaisJFJ1r67P7QnK3pw","e":"AQAB","kid":"NKxtg1GbhZJBVcnBjtSqI","x5t":"ENffeDpSw-aWSjJq0pCEhtDnYP0","x5c":["MIIDHTCCAgWgAwIBAgIJYGCzfjL18UZhMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDVaFw0zNzA1MTUxMDM4NDVaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALD9ZWYTHRVQa0tOrRY8LbmNKEcQ3GfLz81MlAOfr1C6YoUxQzPiZHrJL4t5QLGVZCL/IiVRkqnaAUMq93Nuw1nw7FJx9R2NbxiOqAdputbnpgoq9dOD9OyerGcDuOoUZ46eb7m9ryTIzf6OATg9l4mF7Ey4NoF7Lod9hxyjq+5Y2xGZuMoC3g1ZJxrEkSETsUVhPFT1o00GEZLQM8FvEjFyNcFsb4PMCOrCiOMnQTIUBzvGaJnCM3aOuh99WuOY9icZ+ScSsvnzCWzIN6n01Yf/RTCDgI3VarTzdYNE5KCPGNR/4qpBqu7b6lEZxwbG1ZMcJ2orCRSda+uz+0Jyt6cCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU3JMo7j8KyWFC2F184jYmV55OHjcwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQATHVMl6HagdRkYMP+ZZtdKN4ZSnc5HW0ttANDA5fM19OUFKEdRhQdlhsutD8yQtM4/XDIQ29p7q/665IgA3NJvIOQ98+aDub3Gs92yCnSZqCpSvaJGWkczjL5HQQAEDpSW+WAqAuoazkNdlPmeU0fkA/W92BaZaLw7oDiUrz/JT9pXcnN1SBOALfoj3BiGvvTRNFctFqX7nE8PCwj5tIrzYUVRGD8iNPj342G91D3Q+awp+YJNQxZ5MahWbdcoUJXTgIIOGkIOd0vZ1KcKUyADGMZp0U/pSAWbXXaJtzf8VZjBO0ySZGOMy73HYogUrOQGHoKecLuDIEWX75pOOH3d"],"alg":"RS256"}]})",
   oidc::errc::success},
  // Google RSA
  {R"({"keys":[{"kty":"RSA","kid":"838c06c62046c2d948affe137dd5310129f4d5d1","use":"sig","n":"hsYvCPtkUV7SIxwkOkJsJfhwV_CMdXU5i0UmY2QEs-Pa7v0-0y-s4EjEDtsQ8Yow6hc670JhkGBcMzhU4DtrqNGROXebyOse5FX0m0UvWo1qXqNTf28uBKB990mY42Icr8sGjtOw8ajyT9kufbmXi3eZKagKpG0TDGK90oBEfoGzCxoFT87F95liNth_GoyU5S8-G3OqIqLlQCwxkI5s-g2qvg_aooALfh1rhvx2wt4EJVMSrdnxtPQSPAtZBiw5SwCnVglc6OnalVNvAB2JArbqC9GAzzz9pApAk28SYg5a4hPiPyqwRv-4X1CXEK8bO5VesIeRX0oDf7UoM-pVAw","e":"AQAB","alg":"RS256"},{"n":"lWXY0XOj_ikSIDIvGOhfuRhQJAAj6BWsbbZ6P-PXRclzV32-QLB4GZHPPcH37Lou5pQsTQPvTETAfCLnglIRSbP8x1zA5tUakRlm5RiGF4kcWh5k60x8u0Uslx-d6EueKuY-KLHUVDuMULlHkYAScIdYnXz-Cnr6PFZj8RQezzdPVPH53Q8a_Z9b-vpGzsMS5gszITb-72OQNokojXdPVctl5WzSx-JnWbJxPiwHx_dSWgmTnyiYrZLqrqfampGdroaamtIXy0W8CAe0uCqcD1LunpfX-Q-RD1IycxnEaXSuUKhNhCcxtHWrozEyeD23Zja2WlcvHdYuTzyrvrvS9Q","kid":"7c0b6913fe13820a333399ace426e70535a9a0bf","use":"sig","e":"AQAB","alg":"RS256","kty":"RSA"}]})",
   oidc::errc::success},
  // Keycloak
  {R"({"keys":[{"kid":"EWGWeiW8jqUC_T7Fawi5ecRbrDOanB_uqtTgAOgJDPk","kty":"RSA","alg":"RSA-OAEP","use":"enc","n":"0OceMDMwIZCADl3oZnevxMBzB5GTLwFYevc7PFbw394YY4-Sdt4xqudPMSf2WhU22CidYiMVP1xvcZP7W_5eNLS9nNg3gsswgPiL52ZbWeWoOYx1WDVZzZur3YyjOR1iYvwhR572JumAfDE64y2cHpN2meVana9Zv7S4_yc9pFwGdPTJh7O76lamMjigfYKGF8BKSsFYPkxh_RgaRVqOaaF-CtXPBWmwqvJZ1r0h7GewcP5QeyaUUrZtTo7z0H6VovUGdwguL-OxhWVWZNyGDudtc50PPu3BBn47NpZIa4qCBEsMpm0mJ7YUvwAG-Ktd93uSquJkXvFjaVbTYj_iqQ","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4llfDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDQ5x4wMzAhkIAOXehmd6/EwHMHkZMvAVh69zs8VvDf3hhjj5J23jGq508xJ/ZaFTbYKJ1iIxU/XG9xk/tb/l40tL2c2DeCyzCA+IvnZltZ5ag5jHVYNVnNm6vdjKM5HWJi/CFHnvYm6YB8MTrjLZwek3aZ5Vqdr1m/tLj/Jz2kXAZ09MmHs7vqVqYyOKB9goYXwEpKwVg+TGH9GBpFWo5poX4K1c8FabCq8lnWvSHsZ7Bw/lB7JpRStm1OjvPQfpWi9QZ3CC4v47GFZVZk3IYO521znQ8+7cEGfjs2lkhrioIESwymbSYnthS/AAb4q133e5Kq4mRe8WNpVtNiP+KpAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAGXwcq9tUEOYmO9U8yiVqTxHT1mYHmSJYJITHyRH21tYZKqx8vtj83Xnywh1uioBMkD9YBZL3T5j1wNU3HOvucox62uaWTihd/fliGEMPcAJNqQIaoScOM0Ur6Mh03/qTU3fb53iyKvhd/MrOnO71hwXTFDOi5neIAopLqZHNPBwRiIsxHvlDZ0pN/maUYsTpDVBBYPZZpv3XklKyAmIrAL1655Xz+l6OU/586IY3b6N0EMMLQ0DWZedLK7ueneNV1m6bnc1uzBtj23VcafJN3AkgpQXrAKeREBWn8idyxCWjJZUkJxGUVUAYfBQ90J+WQl/YPtCOwwRcifdNl+9TyM="],"x5t":"2pddNGN6l8gxd7dKncQNOU67zn0","x5t#S256":"08bCcr5IDx1OPuLJcT3t5rSeJbY7Y1JHPF-aBCQfdio"},{"kid":"kf46eDlB9jtz2PpktQ6NQSfRwoU_tT3NqxlI8C4MGvQ","kty":"RSA","alg":"RS256","use":"sig","n":"3yqePLS9Q5Nf-q-ujodgMbblIwwPsEUCcXSxBS0eqrrSTNuPaHr-v_y_yyfKfxqhEmK_MBVvlXXmmwWZk6rWlR2hSdQW6Ih7UWLVaNOT35slcCfjQWQa3O0ZHxzemhgj0HHQ9x8t_AcK1wJd_hMjl3qwG12V1l-9-vSVIOrnl47YXC57K_j8MuNBPr1YdD4Rm9GGFxPQPRpc3nxTO2tFkkeGubfN6eJ226OvaA1uP6SlZEeA4KSjd-Bl0AAKotKcF9orHOuiceAarKs6uGZ84GocGaUtEEOli4e5bJrnNOXZDzCo34VdhFXFmJ2vU3g1XkY8e9wgA0fs9b3OhjqXIw","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4lk7DANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDfKp48tL1Dk1/6r66Oh2AxtuUjDA+wRQJxdLEFLR6qutJM249oev6//L/LJ8p/GqESYr8wFW+VdeabBZmTqtaVHaFJ1BboiHtRYtVo05PfmyVwJ+NBZBrc7RkfHN6aGCPQcdD3Hy38BwrXAl3+EyOXerAbXZXWX7369JUg6ueXjthcLnsr+Pwy40E+vVh0PhGb0YYXE9A9GlzefFM7a0WSR4a5t83p4nbbo69oDW4/pKVkR4DgpKN34GXQAAqi0pwX2isc66Jx4Bqsqzq4ZnzgahwZpS0QQ6WLh7lsmuc05dkPMKjfhV2EVcWYna9TeDVeRjx73CADR+z1vc6GOpcjAgMBAAEwDQYJKoZIhvcNAQELBQADggEBALGuaok4wAivQTlQON+CqhCYeG7ulkfwWoEdBlwLGmBmletdteIX3rC9ESQx9dHTMgk3+qFfH8BIfMVpiH202P5VfGygEdza3zj9C0ot/9HANjpff60XpPdqHmdwq/wpgaZR7ZfJqLjs1BwMZuGC/aQiGh70JBm7tvgXN+qrOrRnhsZiBT4Uec3mCIgEJlsrHqqBiJBohVK5c+EGh+NtvZAW5YNZgXV9SNXDwUS1z4AERoWnV4DtDuGWe0Q0IrFD/B8jFriOAOnu37RAhVt1Gvk/MM5UaHWsFAzyv8t99e0deFafiYmCqaERo1IoHAqCbIgU4nBid8IN5elj5+B4oq4="],"x5t":"8pMnWd5FLQJEK-62faplcAnJg3s","x5t#S256":"RiXQ3fgtOOATBmwUxB6sCnDP9aTthPtAdHo4ZXj0uPY"}]})",
   oidc::errc::success},
  // Azure AD example
  {R"({"keys":[{"kty":"RSA","use":"sig","kid":"9GmnyFPkhc3hOuR22mvSvgnLo7Y","x5t":"9GmnyFPkhc3hOuR22mvSvgnLo7Y","n":"z_w-5U4eZwenXYnEgt2rCN-753YQ7RN8ykiNprNiLl4ilpwAGLWF1cssoRflsSiBVZcCSwUzUwsifG7sbRq9Vc8RFs72Gg0AUwPsJFUqNttMg3Ot-wTqsZtE5GNSBUSqnI-iWoZfjw-uLsS0u4MfzP8Fpkd-rzRlifuIAYK8Ffi1bldkszeBzQbBZbXFwiw5uTf8vEAkH_IAdB732tQAsNXpWWYDV74nKAiwLlDS5FWVs2S2T-MPNAg28MLxYfRhW2bUpd693inxI8WTSLRncouzMImJF4XeMG2ZRZ0z_KJra_uzzMCLbILtpnLA95ysxWw-4ygm3MxN2iBM2IaJeQ","e":"AQAB","x5c":["MIIC/jCCAeagAwIBAgIJAOCJOVRxNKcNMA0GCSqGSIb3DQEBCwUAMC0xKzApBgNVBAMTImFjY291bnRzLmFjY2Vzc2NvbnRyb2wud2luZG93cy5uZXQwHhcNMjMwODI4MjAwMjQwWhcNMjgwODI4MjAwMjQwWjAtMSswKQYDVQQDEyJhY2NvdW50cy5hY2Nlc3Njb250cm9sLndpbmRvd3MubmV0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAz/w+5U4eZwenXYnEgt2rCN+753YQ7RN8ykiNprNiLl4ilpwAGLWF1cssoRflsSiBVZcCSwUzUwsifG7sbRq9Vc8RFs72Gg0AUwPsJFUqNttMg3Ot+wTqsZtE5GNSBUSqnI+iWoZfjw+uLsS0u4MfzP8Fpkd+rzRlifuIAYK8Ffi1bldkszeBzQbBZbXFwiw5uTf8vEAkH/IAdB732tQAsNXpWWYDV74nKAiwLlDS5FWVs2S2T+MPNAg28MLxYfRhW2bUpd693inxI8WTSLRncouzMImJF4XeMG2ZRZ0z/KJra/uzzMCLbILtpnLA95ysxWw+4ygm3MxN2iBM2IaJeQIDAQABoyEwHzAdBgNVHQ4EFgQU/wzRzxsifMCz54SZ3HuF4P4jtzowDQYJKoZIhvcNAQELBQADggEBACaWlbJTObDai8+wmskHedKYb3FCfTwvH/sCRsygHIeDIi23CpoWeKt5FwXsSeqDMd0Hb6IMtYDG5rfGvhkNfunt3sutK0VpZZMNdSBmIXaUx4mBRRUsG4hpeWRrHRgTnxweDDVw4Mv+oYCmpY7eZ4SenISkSd/4qrXzFaI9NeZCY7Jg9vg1bev+NaUtD3C4As6GQ+mN8Rm2NG9vzgTDlKf4Wb5Exy7u9dMW1TChiy28ieVkETKdqwXcbhqM8GOLBUFicdmgP2y9aDGjb89BuaeoHJCGpWWCi3UZth14clVzC6p7ZD6fFx5tKMOL/hQvs3ugGtvFDWCsvcT8bB84RO8="],"issuer":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/v2.0"},{"kty":"RSA","use":"sig","kid":"lHLIu4moKqzPcokwlfCRPHyjl5g","x5t":"lHLIu4moKqzPcokwlfCRPHyjl5g","n":"xlc-u9LJvOdbwAsgsYZpaJrgmrGHaEkoa_3_7Jvu4-Hb8LNtszrQy5Ik4CXgQ_uiLPt4-ePprX3klFAx91ahfd5LwX6mEQPT8WuHMDunx8MaNQrYNVvnOI1L5NxFBFV_6ghi_0d-cOslErcTMML2lbMCSjQ8jwltxz1Oy-Hd9wdY2pz2YC3WR4tHzAGreWGeOB2Vs2NLGv0U3CGSCMqpM9vxbWLZQPuCNpKF93RkxHj5bLng9U_rM6YScacEnTFlKIOOrk4pcVVdoSNNIK2uNUs1hHS1mBXuQjfceghzj3QQYHfp1Z5qWXPRIw3PDyn_1Sowe5UljLurkpj_8m3KnQ","e":"AQAB","x5c":["MIIC6TCCAdGgAwIBAgIIT3fcexMa3ggwDQYJKoZIhvcNAQELBQAwIzEhMB8GA1UEAxMYbG9naW4ubWljcm9zb2Z0b25saW5lLnVzMB4XDTIzMDcxNDAwNDU0NFoXDTI4MDcxNDAwNDU0NFowIzEhMB8GA1UEAxMYbG9naW4ubWljcm9zb2Z0b25saW5lLnVzMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxlc+u9LJvOdbwAsgsYZpaJrgmrGHaEkoa/3/7Jvu4+Hb8LNtszrQy5Ik4CXgQ/uiLPt4+ePprX3klFAx91ahfd5LwX6mEQPT8WuHMDunx8MaNQrYNVvnOI1L5NxFBFV/6ghi/0d+cOslErcTMML2lbMCSjQ8jwltxz1Oy+Hd9wdY2pz2YC3WR4tHzAGreWGeOB2Vs2NLGv0U3CGSCMqpM9vxbWLZQPuCNpKF93RkxHj5bLng9U/rM6YScacEnTFlKIOOrk4pcVVdoSNNIK2uNUs1hHS1mBXuQjfceghzj3QQYHfp1Z5qWXPRIw3PDyn/1Sowe5UljLurkpj/8m3KnQIDAQABoyEwHzAdBgNVHQ4EFgQUCSJrrznFYz1BLqd17S8HFjGrAOAwDQYJKoZIhvcNAQELBQADggEBAAQHNudtmYpeh9x5+rGDVy6OYpTnQ2D5+rmgOHM5yRvgEnFBNuZ6bnr3Ap9nb6EM08juYKPaVyhkV+5axMl+dT8KOuCgrfcKvXqzdQ3BgVFkyU9XfajHzq3JALYpNkixCs/BvqRhXx2ecYxFHB2D671cOwhYIaMZdGtbmOOk8puYSgJ9DBqqn3pLksHmxLP656l/U3hPATTCdfDaNcTagIPx+Q2d9RBn8zOIa/p4CLsu3E0aJfDw3ljPD8inLJ2mpKq06TBfd5Rr/auwipb4J8Y/PHhef8b2kOf42fikIKAP538k9lLsXSowyPWn7KZDTEsku7xpyqvKvEiFkmaV+RY="],"issuer":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/v2.0"}]})",
   oidc::errc::success},
  // Google ID Platform example
  {R"({"keys":[{"e":"AQAB","alg":"RS256","kid":"a06af0b68a2119d692cac4abf415ff3788136f65","n":"yrIpMnHYrVPwlbC-IY8aU2Q6QKnLf_p1FQXNiTO9mWFdeYXP4cNF6QKWgy4jbVSrOs-4qLZbKwRvZhfTuuKW6fwj5lVZcNsq5dd6GXR65I8kwomMH-Zv_pDt9zLiiJCp5_GU6Klb8zMY_jEE1fZp88HIk2ci4GrmtPTbw8LHAkn0P54sQQqmCtzqAWp8qkZ-GGNITxMIdQMY225kX7Dx91ruCb26jPCvF5uOrHT-I6rFU9fZbIgn4T9PthruubbUCutKIR-JK8B7djf61f8ETuKomaHVbCcxA-Q7xD0DEJzeRMqiPrlb9nJszZjmp_VsChoQQg-wl0jFP-1Rygsx9w","use":"sig","kty":"RSA"},{"kid":"f5f4bf46e52b31d9b6249f7309ad0338400680cd","use":"sig","n":"q5hcowR4IuPiSvHbwj9Rv9j2XRnrgbAAFYBqoLBwUV5GVIiNPKnQBYa8ZEIK2naj9gqpo3DU9lx7d7RzeVlzCS5eUA2LV94--KbT0YgIJnApj5-hyDIaevI1Sf2YQr_cntgVLvxqfW1n9ZvbQSitz5Tgh0cplZvuiWMFPu4_mh6B3ShEKIl-qi-h0cZJlRcIf0ZwkfcDOTE8bqEzWUvlCpCH9FK6Mo9YLjw5LroBcHdUbOg3Keu0uW5SCEi-2XBQgCF6xF3kliciwwnv2HhCPyTiX0paM_sT2uKspYock-IQglQ2TExoJqbYZe6CInSHiAA68fkSkJQDnuRZE7XTJQ","e":"AQAB","alg":"RS256","kty":"RSA"},{"kty":"RSA","alg":"RS256","use":"sig","kid":"f833e8a7fe3fe4b878948219a1684afa373ca86f","e":"AQAB","n":"uB-3s136B_Vcme1zGQEg-Avs31_voau8BPKtvbYhB0QOHTtrXCF_wxIH5vWjl-5ts8up8Iy2kVnaItsecGohBAy_0kRgq8oi-n_cZ0i5bspAX5VW0peh_QU3KTlKSBaz3ZD9xMCDWuJFFniHuxLtJ4QtL4v2oDD3pBPNRPyIcZ_LKhH3-Jm-EAvubI5-6lB01zkP5x8f2mp2upqAmyex0jKFka2e0DOBavmGsGvKHKtTnE9oSOTDlhINgQPohoSmir89NRbEqqzeZVb55LWRl_hkiDDOZmcM_oJ8iUbm6vQu3YwCy-ef9wGYEij5GOWLmpYsws5vLVtTE2U-0C_ItQ"}]})",
   oidc::errc::success},
  // Okta example
  {R"({"keys": [{"kty": "RSA", "alg": "RS256", "kid": "f4-dpAc7AqynmGcjjS9LLPCBEW2ezVfIbAqTHPyKe_Y", "use": "sig", "e": "AQAB", "n": "jRkJD3j3D5Zt2ZutvOxaJvgG9iCAi2NjXMAF8lSwFH_2YDc5XFAuFuvMeJp0O-RRPFuOqltp-TmC9v6b7lJaLbvenAf8YtwS5j1hfwekTDpEDjtDrd_57VsbMqCt8_2aEG5qOO8nMB2bNSNKwWL6OsDf8cZZl_v9ifeFD5L12JIzPaCskhL2SZVuHtUTwcuDgjLTGTVrPVmwJri8ZulmKu8t3DOqwwsr1ezqwcVtNucDxB-GeDKAkUCIdEbTEvpcVaQt-Z4i9AA-9tbXuXOQwLvy8ijt-Kaf0V7527LshRejwFCz7Ifz-orWaGyXbGOQDiJl6-hLd8kI8uTfU3zwWw"}]})",
   oidc::errc::success},
  // AD-FS example
  {R"({"keys": [{"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "oIi4qk4BIlAgCct70pqb7gMUfxY", "x5t": "oIi4qk4BIlAgCct70pqb7gMUfxY", "n": "wThO8OU2i4pmyKO7CmiuZJEi6A36vMPXAeng6-9_eU41OF5dgYm4QKelhd4OKKQ8_uQyaOTsyOGERL5gqfGoHd2RA2ic76khzDyI0f0a0JeU4w-wUxaNUI9j643K9XwXmtI5M07MWXFJqGkrOq7kdls9Hnb7n1X4-e2cFpk4AVA9RLWkfeaVDFdYbflyfI1DB9owJq--B3sdP0vjsE7ooLvWc3c7hCuyE98sMKDLacaipOBeJDuD3RdZ9IRH7I4s_6WpNzdtKNtev8JzKC75lXsd8gjDnncYFeLfrtzGe-2mfy7G71Xz2cDUyxaSsYZ3dAbf2vWJFPIi20p9wUU6I_r-hBmfdmHrb4EjEdEZtPJxxiu23jZsONXIN6B4ikMY65gjn2KL_Rf9TKznDfUjJBqdZIMUmn1mFcIc-srAvLIQa3Jq9G2KCzFLWLB-DAHGv4AFGj6XJWk9ybWJdpmmrcPl_s96NhHvkVt016BU3EpGAlDG_Rur57zkismNlPS0j-I-pVqG1ErBsdDqlqjEgKSgubT7NkMqsRGP7l40sqGfXmq6G4ZD_Fr1Y7nTF2CHa3GDF_y59YoxGwkxcvcvIqKH9igo3RIZsuXoIwf6g8Ge7PlWDGtNBSP0sU96Vld1K2Eecn1FqdhPGL0pHMfVj5CxIYZtTbr3KkQB77wMNXk", "e": "AQAB", "x5c": ["MIIE9DCCAtygAwIBAgIQT06T4p1LDYJL8Ha7h1T8TjANBgkqhkiG9w0BAQsFADA2MTQwMgYDVQQDEytBREZTIFNpZ25pbmcgLSBFQzJBTUFaLTVBMkZLSTYuUkVEUEFOREEuQ09NMB4XDTIzMTEwOTIyNTgxOVoXDTI0MTEwODIyNTgxOVowNjE0MDIGA1UEAxMrQURGUyBTaWduaW5nIC0gRUMyQU1BWi01QTJGS0k2LlJFRFBBTkRBLkNPTTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAME4TvDlNouKZsijuwpormSRIugN+rzD1wHp4Ovvf3lONTheXYGJuECnpYXeDiikPP7kMmjk7MjhhES+YKnxqB3dkQNonO+pIcw8iNH9GtCXlOMPsFMWjVCPY+uNyvV8F5rSOTNOzFlxSahpKzqu5HZbPR52+59V+PntnBaZOAFQPUS1pH3mlQxXWG35cnyNQwfaMCavvgd7HT9L47BO6KC71nN3O4QrshPfLDCgy2nGoqTgXiQ7g90XWfSER+yOLP+lqTc3bSjbXr/Ccygu+ZV7HfIIw553GBXi367cxnvtpn8uxu9V89nA1MsWkrGGd3QG39r1iRTyIttKfcFFOiP6/oQZn3Zh62+BIxHRGbTyccYrtt42bDjVyDegeIpDGOuYI59ii/0X/Uys5w31IyQanWSDFJp9ZhXCHPrKwLyyEGtyavRtigsxS1iwfgwBxr+ABRo+lyVpPcm1iXaZpq3D5f7PejYR75FbdNegVNxKRgJQxv0bq+e85IrJjZT0tI/iPqVahtRKwbHQ6paoxICkoLm0+zZDKrERj+5eNLKhn15quhuGQ/xa9WO50xdgh2txgxf8ufWKMRsJMXL3LyKih/YoKN0SGbLl6CMH+oPBnuz5VgxrTQUj9LFPelZXdSthHnJ9RanYTxi9KRzH1Y+QsSGGbU269ypEAe+8DDV5AgMBAAEwDQYJKoZIhvcNAQELBQADggIBAE2yzb07icvwvh0v4XFq+Z6D1dBfogy0Xx7MYmdHRQUFwSIoCPOxe0r5Ec6WurdANl3+ByrRot+XHfxwACSo8xF55E+sRq9pf9oxsRzZ1BrQrx/K2k8Efc+otQAfzc8ZuPOUHMCyGoEkFHIIG+WG28KylfF8tKCGgCEiqHl4fitY4U/Qaib0+ZpIo6A44H8P8t93Hh+aQrituvLiacriI4cRL4hX7qiRoCBVzjN64jGpVvhE1xavs8GCFo17g7h2e9Dj4Etlrj2qGopjr740T1KYfic3l9TS8Npp55FupbfYNWKf7MfHfaFurYRu1hx8nKiCQtbA/bULTorFFroHrKBD+50PqLLFZmJ9XzWxKAG8eNno4lwxDlwWLzeV9u/fPXAL2CB3axK+BZ1N7aJ9dcd4IhfcxOEMTvhIQ02bIwE59h6w+iwwk0O/jlXyScIUdCBNnunnArsTKJtLfuN6+WXkTjpgpPZSwgj+kb9obE2Q3J0EvlNDAARedZsM9ycDENU6oJHbYT/lWGr3yWEYHySDJyRmEEDA8JnN2OwqO7O9QRyEnogY4IX7zh7085EAQoMhXyoo2ihtUbsFBGtwjCFnda8WywbxPxogp2b0q08kb+yZvEFg4/Ip4ExTHBhuu97nbrHZ2iOcZvZvpmyLtGGKtdGXyBqggn6UdJ4x9GSm"]}]})",
   oidc::errc::success},
  // Empty object
  {R"({})", oidc::errc::jwks_invalid},
  // Not object
  {R"("not_object")", oidc::errc::jwks_invalid},
});
BOOST_DATA_TEST_CASE(test_jwks, bdata::make(jwks_data), d) {
    auto jwks = oidc::jwks::make(std::move(d.data));
    if (d.err == oidc::errc::success) {
        BOOST_REQUIRE(jwks.has_value());
    } else {
        BOOST_REQUIRE_EQUAL(d.err, jwks.error());
    }
}

const auto jwk_data = std::to_array<parse_test_data>(
  {// RSA without n
   {R"({"kty":"RSA","use":"sig","e":"AQAB","kid":"tMQzailSAdaW4nojXxES9","alg":"RS256"})",
    oidc::errc::jwk_invalid},
   // RSA without e
   {R"({"kty":"RSA","use":"sig","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","kid":"tMQzailSAdaW4nojXxES9","alg":"RS256"})",
    oidc::errc::jwk_invalid},
   // RSA without use
   {R"({"kty":"RSA","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","e":"AQAB","kid":"tMQzailSAdaW4nojXxES9","alg":"RS256"})",
    oidc::errc::success},
   // RSA with incorrect use
   {R"({"kty":"RSA","use":"enc","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","kid":"tMQzailSAdaW4nojXxES9","alg":"RS256"})",
    oidc::errc::jwk_invalid},
   // RSA without alg
   {R"({"kty":"RSA","use":"sig","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","kid":"tMQzailSAdaW4nojXxES9"})",
    oidc::errc::jwk_invalid}});
BOOST_DATA_TEST_CASE(test_jwk_RS256, bdata::make(jwk_data), d) {
    json::Document doc;
    BOOST_REQUIRE(!doc.Parse(d.data.data(), d.data.length()).HasParseError());

    auto verifiers = oidc::detail::make_rs256_verifier(doc);
    if (d.err == oidc::errc::success) {
        BOOST_REQUIRE(verifiers.has_value());
    } else {
        BOOST_REQUIRE_EQUAL(d.err, verifiers.error());
    }
}

const auto jws_data = std::to_array<parse_test_data>(
  {// Example from https://jwt.io/
   {"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"
    ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
    oidc::errc::success},
   // Auth0 example
   {"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRNUXphaWxTQWRhVzRub2pYeEVTOS"
    "J9."
    "eyJpc3MiOiJodHRwczovL2Rldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbS8iLCJzdW"
    "IiOiIzSkplSTR0bU1DNnY4bUNWQ1NEbkFHVmYydnJuSjBCVEBjbGllbnRzIiwiYXVkIjoibG9j"
    "YWxob3N0IiwiaWF0IjoxNjk1ODg3OTQyLCJleHAiOjE2OTU5NzQzNDIsImF6cCI6IjNKSmVJNH"
    "RtTUM2djhtQ1ZDU0RuQUdWZjJ2cm5KMEJUIiwic2NvcGUiOiJlbWFpbDIiLCJndHkiOiJjbGll"
    "bnQtY3JlZGVudGlhbHMifQ.W6rhgOjWcTPmpeLHiGSd668HHnZJvHgP0QSKU0F1vnin_"
    "UMIpei9IONfN28YSHlAabdUs2JAV70AvVeXB9IqUcEi_"
    "Cfubf3JRpmRcvfyApnmfcRktg1Rq0DVGVl6uBPlqX12SAQ4GPYe4BysUuMb8X-"
    "FU99wF6waCAQw8XLu_Texqy8QOQhW9vZtm5HC54-APn9PV6ZAtG989ihePOsauXUHNe2sqF_"
    "iJ1_7-nkqRbgb_Je-8UjahAkC54y5LPTMVFQvTB5lntf-"
    "sUyHl5oPH7P58M8eNUocOOGADUmrfKMYeSLacM_9mPvZR_"
    "uMbVX0iNt18KO6hKkIvAPrb4U8SA",
    oidc::errc::success},
   // JWS Example: https://www.rfc-editor.org/rfc/rfc7515.html#appendix-A.1.1
   {"eyJ0eXAiOiJKV1QiLA0KICJhbGciOiJIUzI1NiJ9."
    "eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS"
    "9pc19yb290Ijp0cnVlfQ.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
    oidc::errc::success},
   // JWE Example: https://www.rfc-editor.org/rfc/rfc7516#appendix-A.3.7
   {"eyJhbGciOiJBMTI4S1ciLCJlbmMiOiJBMTI4Q0JDLUhTMjU2In0."
    "6KB707dM9YTIgHtLvtgWQ8mKwboJW3of9locizkDTHzBC2IlrT1oOQ."
    "AxY8DCtDaGlsbGljb3RoZQ.KDlTtXchhZTGufMYmOYGS4HffxPSUrfmqCHXaI9wOGY.U0m_"
    "YmjN04DJvceFICbCVQ",
    oidc::errc::jws_invalid_parts},
   // Unsecured JWT Example: https://www.rfc-editor.org/rfc/rfc7519#section-6.1
   {"eyJhbGciOiJub25lIn0."
    "eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS"
    "9pc19yb290Ijp0cnVlfQ.",
    oidc::errc::jws_invalid_parts}});
BOOST_DATA_TEST_CASE(test_jws, bdata::make(jws_data), d) {
    auto jws = oidc::jws::make(ss::sstring{d.data});
    if (d.err == oidc::errc::success) {
        BOOST_REQUIRE(jws.has_value());
    } else {
        BOOST_REQUIRE_EQUAL(d.err, jws.error());
    }
}

struct verify_test_data {
    std::string_view jwks;
    oidc::errc jwks_err;
    std::string_view jws;
    oidc::errc jws_err;
    oidc::errc update_err;
    oidc::errc verify_err;
    friend std::ostream&
    operator<<(std::ostream& os, const verify_test_data& r) {
        fmt::print(
          os,
          "jwks: {}, jwks_err: {}, jws: {}, jws_err: {}, update_err: {}, "
          "verify_err: {}",
          r.jwks,
          r.jwks_err,
          r.jws,
          r.jws_err,
          r.update_err,
          r.verify_err);
        return os;
    }
};
const auto oidc_verify_data = std::to_array<verify_test_data>({
  // Auth0 example
  {R"({"keys":[{"kty":"RSA","use":"sig","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","e":"AQAB","kid":"tMQzailSAdaW4nojXxES9","x5t":"_LDYs6FFeEvJ-ng_a5-cxjUYaRw","x5c":["MIIDHTCCAgWgAwIBAgIJGbkt6m2aGpdZMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDRaFw0zNzA1MTUxMDM4NDRaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM7PGZNYQ/YYfFxn18ttSucs63mX+0fb2E0EzVRinQwx3we2EWwtRUcIuqRnGtxJUB7R+ZeEvdzFGkLZDfCWCPuWMhs8QTlcosHXP6ifozVmM1XWV+z7gqPod4pASZA+TfX/W/s79HP7h4bZbsx0JIWgsVSZ+beF0ORwJtan8SYKLVV260wxgHWRIe6eUSNpFPEQBhOvES1FbABl3Xn7O90BlaTP7hjjZanzEJNOXsMktoqqNeDHzMFJ+tIERJNcO64jBib3XRAE3vF3zk2cBK7H/GXBMVzFTnAZiJ+veMtQmcei4pq1iM1sp77guofDhQgu+iP04jx6ojGi16mjWm0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUIVqxco8b4MbVi3oKXvpvTQcAgxAwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQAz2efVzpAKHu7l7iPlBSRNRbXDnrxLR54ZtEX025l0yPdfM9fhPECYe9s2ze2TDupu2Is6XFk7ur5lUnON3qEKUU1kLRygz8oaj9lqsXdrfg9aTwXpxywWUelUKjLUL7FFZrXITSlG8SvwioTocpWPDaaDw0xefXnZRay5jjc9pl4E9uvd6K50SyQr7mY1ZEmNSYSftaoJGorFROaZs8Q0dc998JleYG1kFN0788eycCn4aRa0IKD/RfMXYj0j61T66vKnCALUfzFVtd/BUNwWdu0kRQceeuca4A+GWWxvYbDa4wJ/hEzWXT71BHUM6OhW4ls91wNTO9jdId/WJ3Rx"],"alg":"RS256"},{"kty":"RSA","use":"sig","n":"sP1lZhMdFVBrS06tFjwtuY0oRxDcZ8vPzUyUA5-vULpihTFDM-Jkeskvi3lAsZVkIv8iJVGSqdoBQyr3c27DWfDsUnH1HY1vGI6oB2m61uemCir104P07J6sZwO46hRnjp5vub2vJMjN_o4BOD2XiYXsTLg2gXsuh32HHKOr7ljbEZm4ygLeDVknGsSRIROxRWE8VPWjTQYRktAzwW8SMXI1wWxvg8wI6sKI4ydBMhQHO8ZomcIzdo66H31a45j2Jxn5JxKy-fMJbMg3qfTVh_9FMIOAjdVqtPN1g0TkoI8Y1H_iqkGq7tvqURnHBsbVkxwnaisJFJ1r67P7QnK3pw","e":"AQAB","kid":"NKxtg1GbhZJBVcnBjtSqI","x5t":"ENffeDpSw-aWSjJq0pCEhtDnYP0","x5c":["MIIDHTCCAgWgAwIBAgIJYGCzfjL18UZhMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDVaFw0zNzA1MTUxMDM4NDVaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALD9ZWYTHRVQa0tOrRY8LbmNKEcQ3GfLz81MlAOfr1C6YoUxQzPiZHrJL4t5QLGVZCL/IiVRkqnaAUMq93Nuw1nw7FJx9R2NbxiOqAdputbnpgoq9dOD9OyerGcDuOoUZ46eb7m9ryTIzf6OATg9l4mF7Ey4NoF7Lod9hxyjq+5Y2xGZuMoC3g1ZJxrEkSETsUVhPFT1o00GEZLQM8FvEjFyNcFsb4PMCOrCiOMnQTIUBzvGaJnCM3aOuh99WuOY9icZ+ScSsvnzCWzIN6n01Yf/RTCDgI3VarTzdYNE5KCPGNR/4qpBqu7b6lEZxwbG1ZMcJ2orCRSda+uz+0Jyt6cCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU3JMo7j8KyWFC2F184jYmV55OHjcwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQATHVMl6HagdRkYMP+ZZtdKN4ZSnc5HW0ttANDA5fM19OUFKEdRhQdlhsutD8yQtM4/XDIQ29p7q/665IgA3NJvIOQ98+aDub3Gs92yCnSZqCpSvaJGWkczjL5HQQAEDpSW+WAqAuoazkNdlPmeU0fkA/W92BaZaLw7oDiUrz/JT9pXcnN1SBOALfoj3BiGvvTRNFctFqX7nE8PCwj5tIrzYUVRGD8iNPj342G91D3Q+awp+YJNQxZ5MahWbdcoUJXTgIIOGkIOd0vZ1KcKUyADGMZp0U/pSAWbXXaJtzf8VZjBO0ySZGOMy73HYogUrOQGHoKecLuDIEWX75pOOH3d"],"alg":"RS256"}]})",
   oidc::errc::success,
   R"(eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRNUXphaWxTQWRhVzRub2pYeEVTOSJ9.eyJpc3MiOiJodHRwczovL2Rldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbS8iLCJzdWIiOiIzSkplSTR0bU1DNnY4bUNWQ1NEbkFHVmYydnJuSjBCVEBjbGllbnRzIiwiYXVkIjoibG9jYWxob3N0IiwiaWF0IjoxNjk1ODg3OTQyLCJleHAiOjE2OTU5NzQzNDIsImF6cCI6IjNKSmVJNHRtTUM2djhtQ1ZDU0RuQUdWZjJ2cm5KMEJUIiwic2NvcGUiOiJlbWFpbDIiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMifQ.W6rhgOjWcTPmpeLHiGSd668HHnZJvHgP0QSKU0F1vnin_UMIpei9IONfN28YSHlAabdUs2JAV70AvVeXB9IqUcEi_Cfubf3JRpmRcvfyApnmfcRktg1Rq0DVGVl6uBPlqX12SAQ4GPYe4BysUuMb8X-FU99wF6waCAQw8XLu_Texqy8QOQhW9vZtm5HC54-APn9PV6ZAtG989ihePOsauXUHNe2sqF_iJ1_7-nkqRbgb_Je-8UjahAkC54y5LPTMVFQvTB5lntf-sUyHl5oPH7P58M8eNUocOOGADUmrfKMYeSLacM_9mPvZR_uMbVX0iNt18KO6hKkIvAPrb4U8SA)",
   oidc::errc::success,
   oidc::errc::success},
  // Keycloak example
  {R"({"keys":[{"kid":"EWGWeiW8jqUC_T7Fawi5ecRbrDOanB_uqtTgAOgJDPk","kty":"RSA","alg":"RSA-OAEP","use":"enc","n":"0OceMDMwIZCADl3oZnevxMBzB5GTLwFYevc7PFbw394YY4-Sdt4xqudPMSf2WhU22CidYiMVP1xvcZP7W_5eNLS9nNg3gsswgPiL52ZbWeWoOYx1WDVZzZur3YyjOR1iYvwhR572JumAfDE64y2cHpN2meVana9Zv7S4_yc9pFwGdPTJh7O76lamMjigfYKGF8BKSsFYPkxh_RgaRVqOaaF-CtXPBWmwqvJZ1r0h7GewcP5QeyaUUrZtTo7z0H6VovUGdwguL-OxhWVWZNyGDudtc50PPu3BBn47NpZIa4qCBEsMpm0mJ7YUvwAG-Ktd93uSquJkXvFjaVbTYj_iqQ","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4llfDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDQ5x4wMzAhkIAOXehmd6/EwHMHkZMvAVh69zs8VvDf3hhjj5J23jGq508xJ/ZaFTbYKJ1iIxU/XG9xk/tb/l40tL2c2DeCyzCA+IvnZltZ5ag5jHVYNVnNm6vdjKM5HWJi/CFHnvYm6YB8MTrjLZwek3aZ5Vqdr1m/tLj/Jz2kXAZ09MmHs7vqVqYyOKB9goYXwEpKwVg+TGH9GBpFWo5poX4K1c8FabCq8lnWvSHsZ7Bw/lB7JpRStm1OjvPQfpWi9QZ3CC4v47GFZVZk3IYO521znQ8+7cEGfjs2lkhrioIESwymbSYnthS/AAb4q133e5Kq4mRe8WNpVtNiP+KpAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAGXwcq9tUEOYmO9U8yiVqTxHT1mYHmSJYJITHyRH21tYZKqx8vtj83Xnywh1uioBMkD9YBZL3T5j1wNU3HOvucox62uaWTihd/fliGEMPcAJNqQIaoScOM0Ur6Mh03/qTU3fb53iyKvhd/MrOnO71hwXTFDOi5neIAopLqZHNPBwRiIsxHvlDZ0pN/maUYsTpDVBBYPZZpv3XklKyAmIrAL1655Xz+l6OU/586IY3b6N0EMMLQ0DWZedLK7ueneNV1m6bnc1uzBtj23VcafJN3AkgpQXrAKeREBWn8idyxCWjJZUkJxGUVUAYfBQ90J+WQl/YPtCOwwRcifdNl+9TyM="],"x5t":"2pddNGN6l8gxd7dKncQNOU67zn0","x5t#S256":"08bCcr5IDx1OPuLJcT3t5rSeJbY7Y1JHPF-aBCQfdio"},{"kid":"kf46eDlB9jtz2PpktQ6NQSfRwoU_tT3NqxlI8C4MGvQ","kty":"RSA","alg":"RS256","use":"sig","n":"3yqePLS9Q5Nf-q-ujodgMbblIwwPsEUCcXSxBS0eqrrSTNuPaHr-v_y_yyfKfxqhEmK_MBVvlXXmmwWZk6rWlR2hSdQW6Ih7UWLVaNOT35slcCfjQWQa3O0ZHxzemhgj0HHQ9x8t_AcK1wJd_hMjl3qwG12V1l-9-vSVIOrnl47YXC57K_j8MuNBPr1YdD4Rm9GGFxPQPRpc3nxTO2tFkkeGubfN6eJ226OvaA1uP6SlZEeA4KSjd-Bl0AAKotKcF9orHOuiceAarKs6uGZ84GocGaUtEEOli4e5bJrnNOXZDzCo34VdhFXFmJ2vU3g1XkY8e9wgA0fs9b3OhjqXIw","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4lk7DANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDfKp48tL1Dk1/6r66Oh2AxtuUjDA+wRQJxdLEFLR6qutJM249oev6//L/LJ8p/GqESYr8wFW+VdeabBZmTqtaVHaFJ1BboiHtRYtVo05PfmyVwJ+NBZBrc7RkfHN6aGCPQcdD3Hy38BwrXAl3+EyOXerAbXZXWX7369JUg6ueXjthcLnsr+Pwy40E+vVh0PhGb0YYXE9A9GlzefFM7a0WSR4a5t83p4nbbo69oDW4/pKVkR4DgpKN34GXQAAqi0pwX2isc66Jx4Bqsqzq4ZnzgahwZpS0QQ6WLh7lsmuc05dkPMKjfhV2EVcWYna9TeDVeRjx73CADR+z1vc6GOpcjAgMBAAEwDQYJKoZIhvcNAQELBQADggEBALGuaok4wAivQTlQON+CqhCYeG7ulkfwWoEdBlwLGmBmletdteIX3rC9ESQx9dHTMgk3+qFfH8BIfMVpiH202P5VfGygEdza3zj9C0ot/9HANjpff60XpPdqHmdwq/wpgaZR7ZfJqLjs1BwMZuGC/aQiGh70JBm7tvgXN+qrOrRnhsZiBT4Uec3mCIgEJlsrHqqBiJBohVK5c+EGh+NtvZAW5YNZgXV9SNXDwUS1z4AERoWnV4DtDuGWe0Q0IrFD/B8jFriOAOnu37RAhVt1Gvk/MM5UaHWsFAzyv8t99e0deFafiYmCqaERo1IoHAqCbIgU4nBid8IN5elj5+B4oq4="],"x5t":"8pMnWd5FLQJEK-62faplcAnJg3s","x5t#S256":"RiXQ3fgtOOATBmwUxB6sCnDP9aTthPtAdHo4ZXj0uPY"}]})",
   oidc::errc::success,
   R"(eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJrZjQ2ZURsQjlqdHoyUHBrdFE2TlFTZlJ3b1VfdFQzTnF4bEk4QzRNR3ZRIn0.eyJleHAiOjE2OTc2NDU0NjksImlhdCI6MTY5NzY0NTE3MCwiYXV0aF90aW1lIjowLCJqdGkiOiJkZGYxZWE2MC01NzcwLTQ0MzctYTMzYy02MWM2MjllYmM3YWIiLCJpc3MiOiJodHRwOi8vZG9ja2VyLXJwLTE6ODA4MC9yZWFsbXMvZGVtb3JlYWxtIiwiYXVkIjoibXlhcHAiLCJzdWIiOiJiMDBmNjU0Yy04NDJiLTRlYjEtODhmMi0zMDRkMTcyNDZiYjUiLCJ0eXAiOiJJRCIsImF6cCI6Im15YXBwIiwiYXRfaGFzaCI6ImFHb2tGUGVpa2FDUm85WUZxbHlnbEEiLCJhY3IiOiIxIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRIb3N0IjoiMTcyLjE4LjAuMzYiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtbXlhcHAiLCJjbGllbnRBZGRyZXNzIjoiMTcyLjE4LjAuMzYiLCJlbWFpbCI6Im15YXBwQGN1c3RvbWVyLmNvbSIsImNsaWVudF9pZCI6Im15YXBwIn0.bYoI1XzW6Bw-dOnPAwBoB3tjM4xrt3loeUgwuipOmcm-yTheLSCf4qK3LOlzmuZdyF7TduDiizl4q8XIDe6XydrYL0tw4ebnQjcQo0wJNSZZQ3olZ65iJUoFHqK0YZuI4a4VDqMq0viEo2c2zFo3reVsr2XTtBgtU-iVyWpvmz6x-CceaGaKZpjDOcS8ixFcpSx-_mraQ18vr2yNHDO1DExg3Pc6FdbaiYApfldR9QIe2Oa5VTlVWXaqmjbFv3dA-FODIznEZ4tiN_8fZz5-BhkTTEupQGbCrYTIhR9rW_2xC_b8MhJCBMVFkMYATxLmJH9WPN5Fnzs2kiP7Xvs_eg)",
   oidc::errc::success,
   oidc::errc::success},
  // Azure AD example
  {
    R"({"keys":[{"kty":"RSA","use":"sig","kid":"9GmnyFPkhc3hOuR22mvSvgnLo7Y","x5t":"9GmnyFPkhc3hOuR22mvSvgnLo7Y","n":"z_w-5U4eZwenXYnEgt2rCN-753YQ7RN8ykiNprNiLl4ilpwAGLWF1cssoRflsSiBVZcCSwUzUwsifG7sbRq9Vc8RFs72Gg0AUwPsJFUqNttMg3Ot-wTqsZtE5GNSBUSqnI-iWoZfjw-uLsS0u4MfzP8Fpkd-rzRlifuIAYK8Ffi1bldkszeBzQbBZbXFwiw5uTf8vEAkH_IAdB732tQAsNXpWWYDV74nKAiwLlDS5FWVs2S2T-MPNAg28MLxYfRhW2bUpd693inxI8WTSLRncouzMImJF4XeMG2ZRZ0z_KJra_uzzMCLbILtpnLA95ysxWw-4ygm3MxN2iBM2IaJeQ","e":"AQAB","x5c":["MIIC/jCCAeagAwIBAgIJAOCJOVRxNKcNMA0GCSqGSIb3DQEBCwUAMC0xKzApBgNVBAMTImFjY291bnRzLmFjY2Vzc2NvbnRyb2wud2luZG93cy5uZXQwHhcNMjMwODI4MjAwMjQwWhcNMjgwODI4MjAwMjQwWjAtMSswKQYDVQQDEyJhY2NvdW50cy5hY2Nlc3Njb250cm9sLndpbmRvd3MubmV0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAz/w+5U4eZwenXYnEgt2rCN+753YQ7RN8ykiNprNiLl4ilpwAGLWF1cssoRflsSiBVZcCSwUzUwsifG7sbRq9Vc8RFs72Gg0AUwPsJFUqNttMg3Ot+wTqsZtE5GNSBUSqnI+iWoZfjw+uLsS0u4MfzP8Fpkd+rzRlifuIAYK8Ffi1bldkszeBzQbBZbXFwiw5uTf8vEAkH/IAdB732tQAsNXpWWYDV74nKAiwLlDS5FWVs2S2T+MPNAg28MLxYfRhW2bUpd693inxI8WTSLRncouzMImJF4XeMG2ZRZ0z/KJra/uzzMCLbILtpnLA95ysxWw+4ygm3MxN2iBM2IaJeQIDAQABoyEwHzAdBgNVHQ4EFgQU/wzRzxsifMCz54SZ3HuF4P4jtzowDQYJKoZIhvcNAQELBQADggEBACaWlbJTObDai8+wmskHedKYb3FCfTwvH/sCRsygHIeDIi23CpoWeKt5FwXsSeqDMd0Hb6IMtYDG5rfGvhkNfunt3sutK0VpZZMNdSBmIXaUx4mBRRUsG4hpeWRrHRgTnxweDDVw4Mv+oYCmpY7eZ4SenISkSd/4qrXzFaI9NeZCY7Jg9vg1bev+NaUtD3C4As6GQ+mN8Rm2NG9vzgTDlKf4Wb5Exy7u9dMW1TChiy28ieVkETKdqwXcbhqM8GOLBUFicdmgP2y9aDGjb89BuaeoHJCGpWWCi3UZth14clVzC6p7ZD6fFx5tKMOL/hQvs3ugGtvFDWCsvcT8bB84RO8="],"issuer":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/v2.0"},{"kty":"RSA","use":"sig","kid":"lHLIu4moKqzPcokwlfCRPHyjl5g","x5t":"lHLIu4moKqzPcokwlfCRPHyjl5g","n":"xlc-u9LJvOdbwAsgsYZpaJrgmrGHaEkoa_3_7Jvu4-Hb8LNtszrQy5Ik4CXgQ_uiLPt4-ePprX3klFAx91ahfd5LwX6mEQPT8WuHMDunx8MaNQrYNVvnOI1L5NxFBFV_6ghi_0d-cOslErcTMML2lbMCSjQ8jwltxz1Oy-Hd9wdY2pz2YC3WR4tHzAGreWGeOB2Vs2NLGv0U3CGSCMqpM9vxbWLZQPuCNpKF93RkxHj5bLng9U_rM6YScacEnTFlKIOOrk4pcVVdoSNNIK2uNUs1hHS1mBXuQjfceghzj3QQYHfp1Z5qWXPRIw3PDyn_1Sowe5UljLurkpj_8m3KnQ","e":"AQAB","x5c":["MIIC6TCCAdGgAwIBAgIIT3fcexMa3ggwDQYJKoZIhvcNAQELBQAwIzEhMB8GA1UEAxMYbG9naW4ubWljcm9zb2Z0b25saW5lLnVzMB4XDTIzMDcxNDAwNDU0NFoXDTI4MDcxNDAwNDU0NFowIzEhMB8GA1UEAxMYbG9naW4ubWljcm9zb2Z0b25saW5lLnVzMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxlc+u9LJvOdbwAsgsYZpaJrgmrGHaEkoa/3/7Jvu4+Hb8LNtszrQy5Ik4CXgQ/uiLPt4+ePprX3klFAx91ahfd5LwX6mEQPT8WuHMDunx8MaNQrYNVvnOI1L5NxFBFV/6ghi/0d+cOslErcTMML2lbMCSjQ8jwltxz1Oy+Hd9wdY2pz2YC3WR4tHzAGreWGeOB2Vs2NLGv0U3CGSCMqpM9vxbWLZQPuCNpKF93RkxHj5bLng9U/rM6YScacEnTFlKIOOrk4pcVVdoSNNIK2uNUs1hHS1mBXuQjfceghzj3QQYHfp1Z5qWXPRIw3PDyn/1Sowe5UljLurkpj/8m3KnQIDAQABoyEwHzAdBgNVHQ4EFgQUCSJrrznFYz1BLqd17S8HFjGrAOAwDQYJKoZIhvcNAQELBQADggEBAAQHNudtmYpeh9x5+rGDVy6OYpTnQ2D5+rmgOHM5yRvgEnFBNuZ6bnr3Ap9nb6EM08juYKPaVyhkV+5axMl+dT8KOuCgrfcKvXqzdQ3BgVFkyU9XfajHzq3JALYpNkixCs/BvqRhXx2ecYxFHB2D671cOwhYIaMZdGtbmOOk8puYSgJ9DBqqn3pLksHmxLP656l/U3hPATTCdfDaNcTagIPx+Q2d9RBn8zOIa/p4CLsu3E0aJfDw3ljPD8inLJ2mpKq06TBfd5Rr/auwipb4J8Y/PHhef8b2kOf42fikIKAP538k9lLsXSowyPWn7KZDTEsku7xpyqvKvEiFkmaV+RY="],"issuer":"https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/v2.0"}]})",
    oidc::errc::success,
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSIsImtpZCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSJ9.eyJhdWQiOiJhcGk6Ly82ZGUxZGJiMy0xMGM2LTQ5ZTYtOWY2Ny0xMTlmMzA2ODRhMWIiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC85YTk1ZmQ5ZS0wMDVkLTQ4N2EtOWEwMS1kMDhjMWVhYjI3NTcvIiwiaWF0IjoxNjk5MjEzMTYxLCJuYmYiOjE2OTkyMTMxNjEsImV4cCI6MTY5OTIxNzA2MSwiYWlvIjoiRTJWZ1lKajEvdlc5OTR2bFpXbyt6SnQ0STBGNE9nQT0iLCJhcHBpZCI6IjZkZTFkYmIzLTEwYzYtNDllNi05ZjY3LTExOWYzMDY4NGExYiIsImFwcGlkYWNyIjoiMSIsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzlhOTVmZDllLTAwNWQtNDg3YS05YTAxLWQwOGMxZWFiMjc1Ny8iLCJvaWQiOiJhMzY1ODI3YS02YjMyLTQxMDAtYmY1MS01ZmJkM2MzZDFmNzQiLCJyaCI6IjAuQVgwQW52MlZtbDBBZWtpYUFkQ01IcXNuVjdQYjRXM0dFT1pKbjJjUm56Qm9TaHVhQUFBLiIsInN1YiI6ImEzNjU4MjdhLTZiMzItNDEwMC1iZjUxLTVmYmQzYzNkMWY3NCIsInRpZCI6IjlhOTVmZDllLTAwNWQtNDg3YS05YTAxLWQwOGMxZWFiMjc1NyIsInV0aSI6IndMVG5Dc2tRLVVLc3lDTjBUTFE0QVEiLCJ2ZXIiOiIxLjAifQ.h50DAbes4A4ffY2vNfkB000aRryrqBXF_nOYdVowPTkaBjdbVjNLDuYt6YNRVTZ9aWUsZ4M_LV7r4f9zQd830gXWm9i8ZEKnsbLdzMUWzY4Gxk3jHt4FsixZzw4XxdZO5oF2EpiD50m--t6sjUB9uouy93Llu7eGhk_Qg_HVvzAoclja_6bRPseJoL681iEOh8BZBG_W0d2OkEndZEGKVMa4vw3-lkEuo4wwnODru30_SXm7qwpFXoiqfwfK_ndGdDDbL__LyoWxWoWwwGyICK0SwnP7gIh2wrxvohQRItTDsZf5StO3H0zmeeE1bSkt_v8b8dRanOGd54oZK7k2wQ)",
    oidc::errc::success,
    oidc::errc::success,
  },
  // Google ID Platform example
  {
    R"({"keys":[{"e":"AQAB","alg":"RS256","kid":"a06af0b68a2119d692cac4abf415ff3788136f65","n":"yrIpMnHYrVPwlbC-IY8aU2Q6QKnLf_p1FQXNiTO9mWFdeYXP4cNF6QKWgy4jbVSrOs-4qLZbKwRvZhfTuuKW6fwj5lVZcNsq5dd6GXR65I8kwomMH-Zv_pDt9zLiiJCp5_GU6Klb8zMY_jEE1fZp88HIk2ci4GrmtPTbw8LHAkn0P54sQQqmCtzqAWp8qkZ-GGNITxMIdQMY225kX7Dx91ruCb26jPCvF5uOrHT-I6rFU9fZbIgn4T9PthruubbUCutKIR-JK8B7djf61f8ETuKomaHVbCcxA-Q7xD0DEJzeRMqiPrlb9nJszZjmp_VsChoQQg-wl0jFP-1Rygsx9w","use":"sig","kty":"RSA"},{"kid":"f5f4bf46e52b31d9b6249f7309ad0338400680cd","use":"sig","n":"q5hcowR4IuPiSvHbwj9Rv9j2XRnrgbAAFYBqoLBwUV5GVIiNPKnQBYa8ZEIK2naj9gqpo3DU9lx7d7RzeVlzCS5eUA2LV94--KbT0YgIJnApj5-hyDIaevI1Sf2YQr_cntgVLvxqfW1n9ZvbQSitz5Tgh0cplZvuiWMFPu4_mh6B3ShEKIl-qi-h0cZJlRcIf0ZwkfcDOTE8bqEzWUvlCpCH9FK6Mo9YLjw5LroBcHdUbOg3Keu0uW5SCEi-2XBQgCF6xF3kliciwwnv2HhCPyTiX0paM_sT2uKspYock-IQglQ2TExoJqbYZe6CInSHiAA68fkSkJQDnuRZE7XTJQ","e":"AQAB","alg":"RS256","kty":"RSA"},{"kty":"RSA","alg":"RS256","use":"sig","kid":"f833e8a7fe3fe4b878948219a1684afa373ca86f","e":"AQAB","n":"uB-3s136B_Vcme1zGQEg-Avs31_voau8BPKtvbYhB0QOHTtrXCF_wxIH5vWjl-5ts8up8Iy2kVnaItsecGohBAy_0kRgq8oi-n_cZ0i5bspAX5VW0peh_QU3KTlKSBaz3ZD9xMCDWuJFFniHuxLtJ4QtL4v2oDD3pBPNRPyIcZ_LKhH3-Jm-EAvubI5-6lB01zkP5x8f2mp2upqAmyex0jKFka2e0DOBavmGsGvKHKtTnE9oSOTDlhINgQPohoSmir89NRbEqqzeZVb55LWRl_hkiDDOZmcM_oJ8iUbm6vQu3YwCy-ef9wGYEij5GOWLmpYsws5vLVtTE2U-0C_ItQ"}]})",
    oidc::errc::success,
    R"(eyJhbGciOiJSUzI1NiIsImtpZCI6ImY1ZjRiZjQ2ZTUyYjMxZDliNjI0OWY3MzA5YWQwMzM4NDAwNjgwY2QiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJteWFwcCIsImF6cCI6IjExNzM2NzgxMDA0ODY0MjA3NDkzOSIsImVtYWlsIjoidGVzdC03MTlAZGlyZWN0LXN1YmplY3QtNDA0MDE3LmlhbS5nc2VydmljZWFjY291bnQuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY5OTA2MTA4MiwiaWF0IjoxNjk5MDU3NDgyLCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxMTczNjc4MTAwNDg2NDIwNzQ5MzkifQ.gm60Q6OiKzpnmTqh0BrmTkXzyUm2OEUrL8gfA4LBKdzXHucMhlqBA2LaNACv0LH2oqCxwy5BCdCaZWkHhIVJGCLMy7p__c_OO_1VUXwCtr9j2MtQaPPgOK8z1dT8-dmU3xXuWsEl5FX2k3JFiDlSUdgNrFs8o2uuJG_VtiLjV3u4jyHTS-vwcdYbFpH7ipS-bL-g-_uW8-qKAfJqk4V_fWXJ__tjNlvMRogweujpRpa-_1H9Cdk7avlRB_xbthUVFI2guE-0yMGDZhZrjI1vkA9uj1bpcH-85JiGPRlcGuikBnmB3ZUkJ6QffVfa4PwyYV78ybkPjtbcXjcp3X0AjQ)",
    oidc::errc::success,
    oidc::errc::success,
  },
  // Okta example
  {
    R"({"keys": [{"kty": "RSA", "alg": "RS256", "kid": "f4-dpAc7AqynmGcjjS9LLPCBEW2ezVfIbAqTHPyKe_Y", "use": "sig", "e": "AQAB", "n": "jRkJD3j3D5Zt2ZutvOxaJvgG9iCAi2NjXMAF8lSwFH_2YDc5XFAuFuvMeJp0O-RRPFuOqltp-TmC9v6b7lJaLbvenAf8YtwS5j1hfwekTDpEDjtDrd_57VsbMqCt8_2aEG5qOO8nMB2bNSNKwWL6OsDf8cZZl_v9ifeFD5L12JIzPaCskhL2SZVuHtUTwcuDgjLTGTVrPVmwJri8ZulmKu8t3DOqwwsr1ezqwcVtNucDxB-GeDKAkUCIdEbTEvpcVaQt-Z4i9AA-9tbXuXOQwLvy8ijt-Kaf0V7527LshRejwFCz7Ifz-orWaGyXbGOQDiJl6-hLd8kI8uTfU3zwWw"}]})",
    oidc::errc::success,
    R"(eyJraWQiOiJmNC1kcEFjN0FxeW5tR2NqalM5TExQQ0JFVzJlelZmSWJBcVRIUHlLZV9ZIiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULk1ZcVRWaTVFT2V5U0NjeHZ2QUNENGVfN1pFTl8zanVSTmFfbDFnb3lTQXciLCJpc3MiOiJodHRwczovL2Rldi02OTc4NTY3OS5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2OTkzOTUzNjcsImV4cCI6MTY5OTM5ODk2NywiY2lkIjoiMG9hZDVuZ3B2ZktZMXBqNlI1ZDciLCJzY3AiOlsibXlDdXN0b21TY29wZSJdLCJzdWIiOiIwb2FkNW5ncHZmS1kxcGo2UjVkNyJ9.Z70D28OtRHmgUZrBQ9XbdEONk-UfdPiGlFAuyREjn3GWVsGi4K9wwvLs8vGgcNR3r1IWBOTXPL-MXtptQDBhRwDkZDQWRUqaoBBul5mHFjSXGeSk1_YM4se4_jH9FZCP0vpavRIlsBpNwiOT9I9I9fkJp9qGA7IQ6LrxezKYpBbQUXcy7bw0oom84ej0ai2zP1_Yphtw6fYgZNOJ8xpf-wtBxzyZgW4Vb3ABnlMcS294h_VR39Cx09yfHXLqVgYactujzXxcAjYD2fCyFWHJ-5ZCZZxHWrM_wToCvoe7vytGTrAbOo2_hLhYw78o6cGXLGoHOe-akCpmGd9HzNIMCg)",
    oidc::errc::success,
    oidc::errc::success,
  },
  // AD-FS example
  {
    R"({"keys": [{"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "oIi4qk4BIlAgCct70pqb7gMUfxY", "x5t": "oIi4qk4BIlAgCct70pqb7gMUfxY", "n": "wThO8OU2i4pmyKO7CmiuZJEi6A36vMPXAeng6-9_eU41OF5dgYm4QKelhd4OKKQ8_uQyaOTsyOGERL5gqfGoHd2RA2ic76khzDyI0f0a0JeU4w-wUxaNUI9j643K9XwXmtI5M07MWXFJqGkrOq7kdls9Hnb7n1X4-e2cFpk4AVA9RLWkfeaVDFdYbflyfI1DB9owJq--B3sdP0vjsE7ooLvWc3c7hCuyE98sMKDLacaipOBeJDuD3RdZ9IRH7I4s_6WpNzdtKNtev8JzKC75lXsd8gjDnncYFeLfrtzGe-2mfy7G71Xz2cDUyxaSsYZ3dAbf2vWJFPIi20p9wUU6I_r-hBmfdmHrb4EjEdEZtPJxxiu23jZsONXIN6B4ikMY65gjn2KL_Rf9TKznDfUjJBqdZIMUmn1mFcIc-srAvLIQa3Jq9G2KCzFLWLB-DAHGv4AFGj6XJWk9ybWJdpmmrcPl_s96NhHvkVt016BU3EpGAlDG_Rur57zkismNlPS0j-I-pVqG1ErBsdDqlqjEgKSgubT7NkMqsRGP7l40sqGfXmq6G4ZD_Fr1Y7nTF2CHa3GDF_y59YoxGwkxcvcvIqKH9igo3RIZsuXoIwf6g8Ge7PlWDGtNBSP0sU96Vld1K2Eecn1FqdhPGL0pHMfVj5CxIYZtTbr3KkQB77wMNXk", "e": "AQAB", "x5c": ["MIIE9DCCAtygAwIBAgIQT06T4p1LDYJL8Ha7h1T8TjANBgkqhkiG9w0BAQsFADA2MTQwMgYDVQQDEytBREZTIFNpZ25pbmcgLSBFQzJBTUFaLTVBMkZLSTYuUkVEUEFOREEuQ09NMB4XDTIzMTEwOTIyNTgxOVoXDTI0MTEwODIyNTgxOVowNjE0MDIGA1UEAxMrQURGUyBTaWduaW5nIC0gRUMyQU1BWi01QTJGS0k2LlJFRFBBTkRBLkNPTTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAME4TvDlNouKZsijuwpormSRIugN+rzD1wHp4Ovvf3lONTheXYGJuECnpYXeDiikPP7kMmjk7MjhhES+YKnxqB3dkQNonO+pIcw8iNH9GtCXlOMPsFMWjVCPY+uNyvV8F5rSOTNOzFlxSahpKzqu5HZbPR52+59V+PntnBaZOAFQPUS1pH3mlQxXWG35cnyNQwfaMCavvgd7HT9L47BO6KC71nN3O4QrshPfLDCgy2nGoqTgXiQ7g90XWfSER+yOLP+lqTc3bSjbXr/Ccygu+ZV7HfIIw553GBXi367cxnvtpn8uxu9V89nA1MsWkrGGd3QG39r1iRTyIttKfcFFOiP6/oQZn3Zh62+BIxHRGbTyccYrtt42bDjVyDegeIpDGOuYI59ii/0X/Uys5w31IyQanWSDFJp9ZhXCHPrKwLyyEGtyavRtigsxS1iwfgwBxr+ABRo+lyVpPcm1iXaZpq3D5f7PejYR75FbdNegVNxKRgJQxv0bq+e85IrJjZT0tI/iPqVahtRKwbHQ6paoxICkoLm0+zZDKrERj+5eNLKhn15quhuGQ/xa9WO50xdgh2txgxf8ufWKMRsJMXL3LyKih/YoKN0SGbLl6CMH+oPBnuz5VgxrTQUj9LFPelZXdSthHnJ9RanYTxi9KRzH1Y+QsSGGbU269ypEAe+8DDV5AgMBAAEwDQYJKoZIhvcNAQELBQADggIBAE2yzb07icvwvh0v4XFq+Z6D1dBfogy0Xx7MYmdHRQUFwSIoCPOxe0r5Ec6WurdANl3+ByrRot+XHfxwACSo8xF55E+sRq9pf9oxsRzZ1BrQrx/K2k8Efc+otQAfzc8ZuPOUHMCyGoEkFHIIG+WG28KylfF8tKCGgCEiqHl4fitY4U/Qaib0+ZpIo6A44H8P8t93Hh+aQrituvLiacriI4cRL4hX7qiRoCBVzjN64jGpVvhE1xavs8GCFo17g7h2e9Dj4Etlrj2qGopjr740T1KYfic3l9TS8Npp55FupbfYNWKf7MfHfaFurYRu1hx8nKiCQtbA/bULTorFFroHrKBD+50PqLLFZmJ9XzWxKAG8eNno4lwxDlwWLzeV9u/fPXAL2CB3axK+BZ1N7aJ9dcd4IhfcxOEMTvhIQ02bIwE59h6w+iwwk0O/jlXyScIUdCBNnunnArsTKJtLfuN6+WXkTjpgpPZSwgj+kb9obE2Q3J0EvlNDAARedZsM9ycDENU6oJHbYT/lWGr3yWEYHySDJyRmEEDA8JnN2OwqO7O9QRyEnogY4IX7zh7085EAQoMhXyoo2ihtUbsFBGtwjCFnda8WywbxPxogp2b0q08kb+yZvEFg4/Ip4ExTHBhuu97nbrHZ2iOcZvZvpmyLtGGKtdGXyBqggn6UdJ4x9GSm"]}]})",
    oidc::errc::success,
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im9JaTRxazRCSWxBZ0NjdDcwcHFiN2dNVWZ4WSIsImtpZCI6Im9JaTRxazRCSWxBZ0NjdDcwcHFiN2dNVWZ4WSJ9.eyJhdWQiOiJodHRwczovL2xvY2FsaG9zdDozMDMwIiwiaXNzIjoiaHR0cDovL0VDMkFNQVotNUEyRktJNi5SRURQQU5EQS5DT00vYWRmcy9zZXJ2aWNlcy90cnVzdCIsImlhdCI6MTcwMDE1MjkyMSwibmJmIjoxNzAwMTUyOTIxLCJleHAiOjE3MDAxNTY1MjEsImFwcHR5cGUiOiJDb25maWRlbnRpYWwiLCJhcHBpZCI6Ijc2YjI5NjZiLWIxYjAtNGFlYy1hNWRhLTY2MWNmZmUwOTVlMCIsImF1dGhtZXRob2QiOiJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvYXV0aGVudGljYXRpb25tZXRob2QvcGFzc3dvcmQiLCJhdXRoX3RpbWUiOiIyMDIzLTExLTE2VDE2OjQyOjAxLjIzOVoiLCJ2ZXIiOiIxLjAiLCJzY3AiOiJvcGVuaWQifQ.vUqRYrGhkNOtKEWaTA1cCT24aH8BqbknETp6P3ndlLCPoLi_Ui3B06KyxilVU4o8kEDmWfsrQGLeBUnSF_Q_eODsMSIHWysw3vMUU-BIP2yZKKWvSphUA1X7uqBV1qTu-jJRqgC-9ubXoXMB7iiwhQenxvmtDC_1OvGDGEmb9dQ655Fo3wUfYuDlu1QbhlvbOKC48JAbuHQh1J_ZHWNHCOQdihDg2vsUhm9XO20CzC5I6fSa7FPWkwyl3LrWSwB4TSJfeNwTIftTJ5D_v_WQIXclo2wYNjdWxNB1_DrkNM0D7rHdGFkGxW5c4MlFHZ9Zw7x6FybjBamFOriCVZTohCfDVBQIwx2KHvy5Zlcyt5xmDie7ghOgiWxFbzd9jxssbIc2EhIRM3B3bVh4ulTlpABqw3GCwUR-ameW7V5fWnNr3UD9UqvO9F2eQCeQdQzXKGHCgamUzk0K2zlIZjV12JRYKwwvJN5OuyNHeVzS38E4FXNZzyXmJinBsv7800avvcqD1ivR9RhOnU2BIt-KTia2e3v7D_GHhS4e5RP4F59aNNVZbFCZK5ElRTESsmurIQ449p208acvGr3wLePbPXUx3KuC5VWaFam4m1NCXgJy6yvkZl61xLvUVcmxUQXjAistrBRzQ1lhnmsh8vhcxGluCNRqyU-ks5YdG-xpoGk)",
    oidc::errc::success,
    oidc::errc::success,
  },
});
BOOST_DATA_TEST_CASE(test_oidc_verifier, bdata::make(oidc_verify_data), d) {
    auto jwks = oidc::jwks::make(ss::sstring{d.jwks});
    if (d.jwks_err != oidc::errc::success) {
        BOOST_REQUIRE_EQUAL(d.jwks_err, jwks.error());
        return;
    }
    BOOST_REQUIRE(!jwks.has_error());

    auto jws = oidc::jws::make(ss::sstring{d.jws});
    if (d.jws_err != oidc::errc::success) {
        BOOST_REQUIRE_EQUAL(d.jws_err, jws.error());
        return;
    }
    BOOST_REQUIRE(!jws.has_error());

    oidc::verifier v;
    auto update = v.update_keys(std::move(jwks).assume_value());
    if (d.update_err != oidc::errc::success) {
        BOOST_REQUIRE_EQUAL(d.update_err, update.error());
        return;
    }
    BOOST_REQUIRE(!update.has_error());

    auto verify = v.verify(std::move(jws).assume_value());
    if (d.verify_err != oidc::errc::success) {
        BOOST_REQUIRE_EQUAL(d.verify_err, verify.error());
        return;
    }
    BOOST_REQUIRE(!verify.has_error());
}

struct auth_test_data {
    time_t now;
    std::string_view jwks;
    std::string_view jws;
    ss::sstring issuer;
    ss::sstring audience;
    std::chrono::seconds clock_skew_tolerance;
    oidc::errc auth_err;
    security::acl_principal principal;
    security::oidc::principal_mapping_rule mapping{};
    friend std::ostream& operator<<(std::ostream& os, const auth_test_data& r) {
        fmt::print(
          os,
          "jwks: {}, jws: {}, issuer: {}, audience: {}, clock_skew_tolerance: "
          "{}, auth_err: {}",
          r.jwks,
          r.jws,
          r.issuer,
          r.audience,
          r.clock_skew_tolerance,
          r.auth_err);
        return os;
    }
};
const auto oidc_auth_data = std::to_array<auth_test_data>({
  // Auth0 example
  {
    1695887942,
    R"({"keys":[{"kty":"RSA","use":"sig","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","e":"AQAB","kid":"tMQzailSAdaW4nojXxES9","x5t":"_LDYs6FFeEvJ-ng_a5-cxjUYaRw","x5c":["MIIDHTCCAgWgAwIBAgIJGbkt6m2aGpdZMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDRaFw0zNzA1MTUxMDM4NDRaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM7PGZNYQ/YYfFxn18ttSucs63mX+0fb2E0EzVRinQwx3we2EWwtRUcIuqRnGtxJUB7R+ZeEvdzFGkLZDfCWCPuWMhs8QTlcosHXP6ifozVmM1XWV+z7gqPod4pASZA+TfX/W/s79HP7h4bZbsx0JIWgsVSZ+beF0ORwJtan8SYKLVV260wxgHWRIe6eUSNpFPEQBhOvES1FbABl3Xn7O90BlaTP7hjjZanzEJNOXsMktoqqNeDHzMFJ+tIERJNcO64jBib3XRAE3vF3zk2cBK7H/GXBMVzFTnAZiJ+veMtQmcei4pq1iM1sp77guofDhQgu+iP04jx6ojGi16mjWm0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUIVqxco8b4MbVi3oKXvpvTQcAgxAwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQAz2efVzpAKHu7l7iPlBSRNRbXDnrxLR54ZtEX025l0yPdfM9fhPECYe9s2ze2TDupu2Is6XFk7ur5lUnON3qEKUU1kLRygz8oaj9lqsXdrfg9aTwXpxywWUelUKjLUL7FFZrXITSlG8SvwioTocpWPDaaDw0xefXnZRay5jjc9pl4E9uvd6K50SyQr7mY1ZEmNSYSftaoJGorFROaZs8Q0dc998JleYG1kFN0788eycCn4aRa0IKD/RfMXYj0j61T66vKnCALUfzFVtd/BUNwWdu0kRQceeuca4A+GWWxvYbDa4wJ/hEzWXT71BHUM6OhW4ls91wNTO9jdId/WJ3Rx"],"alg":"RS256"},{"kty":"RSA","use":"sig","n":"sP1lZhMdFVBrS06tFjwtuY0oRxDcZ8vPzUyUA5-vULpihTFDM-Jkeskvi3lAsZVkIv8iJVGSqdoBQyr3c27DWfDsUnH1HY1vGI6oB2m61uemCir104P07J6sZwO46hRnjp5vub2vJMjN_o4BOD2XiYXsTLg2gXsuh32HHKOr7ljbEZm4ygLeDVknGsSRIROxRWE8VPWjTQYRktAzwW8SMXI1wWxvg8wI6sKI4ydBMhQHO8ZomcIzdo66H31a45j2Jxn5JxKy-fMJbMg3qfTVh_9FMIOAjdVqtPN1g0TkoI8Y1H_iqkGq7tvqURnHBsbVkxwnaisJFJ1r67P7QnK3pw","e":"AQAB","kid":"NKxtg1GbhZJBVcnBjtSqI","x5t":"ENffeDpSw-aWSjJq0pCEhtDnYP0","x5c":["MIIDHTCCAgWgAwIBAgIJYGCzfjL18UZhMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDVaFw0zNzA1MTUxMDM4NDVaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALD9ZWYTHRVQa0tOrRY8LbmNKEcQ3GfLz81MlAOfr1C6YoUxQzPiZHrJL4t5QLGVZCL/IiVRkqnaAUMq93Nuw1nw7FJx9R2NbxiOqAdputbnpgoq9dOD9OyerGcDuOoUZ46eb7m9ryTIzf6OATg9l4mF7Ey4NoF7Lod9hxyjq+5Y2xGZuMoC3g1ZJxrEkSETsUVhPFT1o00GEZLQM8FvEjFyNcFsb4PMCOrCiOMnQTIUBzvGaJnCM3aOuh99WuOY9icZ+ScSsvnzCWzIN6n01Yf/RTCDgI3VarTzdYNE5KCPGNR/4qpBqu7b6lEZxwbG1ZMcJ2orCRSda+uz+0Jyt6cCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU3JMo7j8KyWFC2F184jYmV55OHjcwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQATHVMl6HagdRkYMP+ZZtdKN4ZSnc5HW0ttANDA5fM19OUFKEdRhQdlhsutD8yQtM4/XDIQ29p7q/665IgA3NJvIOQ98+aDub3Gs92yCnSZqCpSvaJGWkczjL5HQQAEDpSW+WAqAuoazkNdlPmeU0fkA/W92BaZaLw7oDiUrz/JT9pXcnN1SBOALfoj3BiGvvTRNFctFqX7nE8PCwj5tIrzYUVRGD8iNPj342G91D3Q+awp+YJNQxZ5MahWbdcoUJXTgIIOGkIOd0vZ1KcKUyADGMZp0U/pSAWbXXaJtzf8VZjBO0ySZGOMy73HYogUrOQGHoKecLuDIEWX75pOOH3d"],"alg":"RS256"}]})",
    R"(eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRNUXphaWxTQWRhVzRub2pYeEVTOSJ9.eyJpc3MiOiJodHRwczovL2Rldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbS8iLCJzdWIiOiIzSkplSTR0bU1DNnY4bUNWQ1NEbkFHVmYydnJuSjBCVEBjbGllbnRzIiwiYXVkIjoibG9jYWxob3N0IiwiaWF0IjoxNjk1ODg3OTQyLCJleHAiOjE2OTU5NzQzNDIsImF6cCI6IjNKSmVJNHRtTUM2djhtQ1ZDU0RuQUdWZjJ2cm5KMEJUIiwic2NvcGUiOiJlbWFpbDIiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMifQ.W6rhgOjWcTPmpeLHiGSd668HHnZJvHgP0QSKU0F1vnin_UMIpei9IONfN28YSHlAabdUs2JAV70AvVeXB9IqUcEi_Cfubf3JRpmRcvfyApnmfcRktg1Rq0DVGVl6uBPlqX12SAQ4GPYe4BysUuMb8X-FU99wF6waCAQw8XLu_Texqy8QOQhW9vZtm5HC54-APn9PV6ZAtG989ihePOsauXUHNe2sqF_iJ1_7-nkqRbgb_Je-8UjahAkC54y5LPTMVFQvTB5lntf-sUyHl5oPH7P58M8eNUocOOGADUmrfKMYeSLacM_9mPvZR_uMbVX0iNt18KO6hKkIvAPrb4U8SA)",
    "https://dev-ltxchcls4igzho78.us.auth0.com/",
    "localhost",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user,
      "3JJeI4tmMC6v8mCVCSDnAGVf2vrnJ0BT@clients"},
  },
  // Keycloak example
  {
    1697645170,
    R"({"keys":[{"kid":"EWGWeiW8jqUC_T7Fawi5ecRbrDOanB_uqtTgAOgJDPk","kty":"RSA","alg":"RSA-OAEP","use":"enc","n":"0OceMDMwIZCADl3oZnevxMBzB5GTLwFYevc7PFbw394YY4-Sdt4xqudPMSf2WhU22CidYiMVP1xvcZP7W_5eNLS9nNg3gsswgPiL52ZbWeWoOYx1WDVZzZur3YyjOR1iYvwhR572JumAfDE64y2cHpN2meVana9Zv7S4_yc9pFwGdPTJh7O76lamMjigfYKGF8BKSsFYPkxh_RgaRVqOaaF-CtXPBWmwqvJZ1r0h7GewcP5QeyaUUrZtTo7z0H6VovUGdwguL-OxhWVWZNyGDudtc50PPu3BBn47NpZIa4qCBEsMpm0mJ7YUvwAG-Ktd93uSquJkXvFjaVbTYj_iqQ","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4llfDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDQ5x4wMzAhkIAOXehmd6/EwHMHkZMvAVh69zs8VvDf3hhjj5J23jGq508xJ/ZaFTbYKJ1iIxU/XG9xk/tb/l40tL2c2DeCyzCA+IvnZltZ5ag5jHVYNVnNm6vdjKM5HWJi/CFHnvYm6YB8MTrjLZwek3aZ5Vqdr1m/tLj/Jz2kXAZ09MmHs7vqVqYyOKB9goYXwEpKwVg+TGH9GBpFWo5poX4K1c8FabCq8lnWvSHsZ7Bw/lB7JpRStm1OjvPQfpWi9QZ3CC4v47GFZVZk3IYO521znQ8+7cEGfjs2lkhrioIESwymbSYnthS/AAb4q133e5Kq4mRe8WNpVtNiP+KpAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAGXwcq9tUEOYmO9U8yiVqTxHT1mYHmSJYJITHyRH21tYZKqx8vtj83Xnywh1uioBMkD9YBZL3T5j1wNU3HOvucox62uaWTihd/fliGEMPcAJNqQIaoScOM0Ur6Mh03/qTU3fb53iyKvhd/MrOnO71hwXTFDOi5neIAopLqZHNPBwRiIsxHvlDZ0pN/maUYsTpDVBBYPZZpv3XklKyAmIrAL1655Xz+l6OU/586IY3b6N0EMMLQ0DWZedLK7ueneNV1m6bnc1uzBtj23VcafJN3AkgpQXrAKeREBWn8idyxCWjJZUkJxGUVUAYfBQ90J+WQl/YPtCOwwRcifdNl+9TyM="],"x5t":"2pddNGN6l8gxd7dKncQNOU67zn0","x5t#S256":"08bCcr5IDx1OPuLJcT3t5rSeJbY7Y1JHPF-aBCQfdio"},{"kid":"kf46eDlB9jtz2PpktQ6NQSfRwoU_tT3NqxlI8C4MGvQ","kty":"RSA","alg":"RS256","use":"sig","n":"3yqePLS9Q5Nf-q-ujodgMbblIwwPsEUCcXSxBS0eqrrSTNuPaHr-v_y_yyfKfxqhEmK_MBVvlXXmmwWZk6rWlR2hSdQW6Ih7UWLVaNOT35slcCfjQWQa3O0ZHxzemhgj0HHQ9x8t_AcK1wJd_hMjl3qwG12V1l-9-vSVIOrnl47YXC57K_j8MuNBPr1YdD4Rm9GGFxPQPRpc3nxTO2tFkkeGubfN6eJ226OvaA1uP6SlZEeA4KSjd-Bl0AAKotKcF9orHOuiceAarKs6uGZ84GocGaUtEEOli4e5bJrnNOXZDzCo34VdhFXFmJ2vU3g1XkY8e9wgA0fs9b3OhjqXIw","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4lk7DANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDfKp48tL1Dk1/6r66Oh2AxtuUjDA+wRQJxdLEFLR6qutJM249oev6//L/LJ8p/GqESYr8wFW+VdeabBZmTqtaVHaFJ1BboiHtRYtVo05PfmyVwJ+NBZBrc7RkfHN6aGCPQcdD3Hy38BwrXAl3+EyOXerAbXZXWX7369JUg6ueXjthcLnsr+Pwy40E+vVh0PhGb0YYXE9A9GlzefFM7a0WSR4a5t83p4nbbo69oDW4/pKVkR4DgpKN34GXQAAqi0pwX2isc66Jx4Bqsqzq4ZnzgahwZpS0QQ6WLh7lsmuc05dkPMKjfhV2EVcWYna9TeDVeRjx73CADR+z1vc6GOpcjAgMBAAEwDQYJKoZIhvcNAQELBQADggEBALGuaok4wAivQTlQON+CqhCYeG7ulkfwWoEdBlwLGmBmletdteIX3rC9ESQx9dHTMgk3+qFfH8BIfMVpiH202P5VfGygEdza3zj9C0ot/9HANjpff60XpPdqHmdwq/wpgaZR7ZfJqLjs1BwMZuGC/aQiGh70JBm7tvgXN+qrOrRnhsZiBT4Uec3mCIgEJlsrHqqBiJBohVK5c+EGh+NtvZAW5YNZgXV9SNXDwUS1z4AERoWnV4DtDuGWe0Q0IrFD/B8jFriOAOnu37RAhVt1Gvk/MM5UaHWsFAzyv8t99e0deFafiYmCqaERo1IoHAqCbIgU4nBid8IN5elj5+B4oq4="],"x5t":"8pMnWd5FLQJEK-62faplcAnJg3s","x5t#S256":"RiXQ3fgtOOATBmwUxB6sCnDP9aTthPtAdHo4ZXj0uPY"}]})",
    R"(eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJrZjQ2ZURsQjlqdHoyUHBrdFE2TlFTZlJ3b1VfdFQzTnF4bEk4QzRNR3ZRIn0.eyJleHAiOjE2OTc2NDU0NjksImlhdCI6MTY5NzY0NTE3MCwiYXV0aF90aW1lIjowLCJqdGkiOiJkZGYxZWE2MC01NzcwLTQ0MzctYTMzYy02MWM2MjllYmM3YWIiLCJpc3MiOiJodHRwOi8vZG9ja2VyLXJwLTE6ODA4MC9yZWFsbXMvZGVtb3JlYWxtIiwiYXVkIjoibXlhcHAiLCJzdWIiOiJiMDBmNjU0Yy04NDJiLTRlYjEtODhmMi0zMDRkMTcyNDZiYjUiLCJ0eXAiOiJJRCIsImF6cCI6Im15YXBwIiwiYXRfaGFzaCI6ImFHb2tGUGVpa2FDUm85WUZxbHlnbEEiLCJhY3IiOiIxIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRIb3N0IjoiMTcyLjE4LjAuMzYiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtbXlhcHAiLCJjbGllbnRBZGRyZXNzIjoiMTcyLjE4LjAuMzYiLCJlbWFpbCI6Im15YXBwQGN1c3RvbWVyLmNvbSIsImNsaWVudF9pZCI6Im15YXBwIn0.bYoI1XzW6Bw-dOnPAwBoB3tjM4xrt3loeUgwuipOmcm-yTheLSCf4qK3LOlzmuZdyF7TduDiizl4q8XIDe6XydrYL0tw4ebnQjcQo0wJNSZZQ3olZ65iJUoFHqK0YZuI4a4VDqMq0viEo2c2zFo3reVsr2XTtBgtU-iVyWpvmz6x-CceaGaKZpjDOcS8ixFcpSx-_mraQ18vr2yNHDO1DExg3Pc6FdbaiYApfldR9QIe2Oa5VTlVWXaqmjbFv3dA-FODIznEZ4tiN_8fZz5-BhkTTEupQGbCrYTIhR9rW_2xC_b8MhJCBMVFkMYATxLmJH9WPN5Fnzs2kiP7Xvs_eg)",
    "http://docker-rp-1:8080/realms/demorealm",
    "myapp",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user, "b00f654c-842b-4eb1-88f2-304d17246bb5"},
  },
  // Azure AD example
  {
    1699213461,
    R"({"keys": [{"kty": "RSA", "use": "sig", "kid": "9GmnyFPkhc3hOuR22mvSvgnLo7Y", "x5t": "9GmnyFPkhc3hOuR22mvSvgnLo7Y", "n": "z_w-5U4eZwenXYnEgt2rCN-753YQ7RN8ykiNprNiLl4ilpwAGLWF1cssoRflsSiBVZcCSwUzUwsifG7sbRq9Vc8RFs72Gg0AUwPsJFUqNttMg3Ot-wTqsZtE5GNSBUSqnI-iWoZfjw-uLsS0u4MfzP8Fpkd-rzRlifuIAYK8Ffi1bldkszeBzQbBZbXFwiw5uTf8vEAkH_IAdB732tQAsNXpWWYDV74nKAiwLlDS5FWVs2S2T-MPNAg28MLxYfRhW2bUpd693inxI8WTSLRncouzMImJF4XeMG2ZRZ0z_KJra_uzzMCLbILtpnLA95ysxWw-4ygm3MxN2iBM2IaJeQ", "e": "AQAB", "x5c": ["MIIC/jCCAeagAwIBAgIJAOCJOVRxNKcNMA0GCSqGSIb3DQEBCwUAMC0xKzApBgNVBAMTImFjY291bnRzLmFjY2Vzc2NvbnRyb2wud2luZG93cy5uZXQwHhcNMjMwODI4MjAwMjQwWhcNMjgwODI4MjAwMjQwWjAtMSswKQYDVQQDEyJhY2NvdW50cy5hY2Nlc3Njb250cm9sLndpbmRvd3MubmV0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAz/w+5U4eZwenXYnEgt2rCN+753YQ7RN8ykiNprNiLl4ilpwAGLWF1cssoRflsSiBVZcCSwUzUwsifG7sbRq9Vc8RFs72Gg0AUwPsJFUqNttMg3Ot+wTqsZtE5GNSBUSqnI+iWoZfjw+uLsS0u4MfzP8Fpkd+rzRlifuIAYK8Ffi1bldkszeBzQbBZbXFwiw5uTf8vEAkH/IAdB732tQAsNXpWWYDV74nKAiwLlDS5FWVs2S2T+MPNAg28MLxYfRhW2bUpd693inxI8WTSLRncouzMImJF4XeMG2ZRZ0z/KJra/uzzMCLbILtpnLA95ysxWw+4ygm3MxN2iBM2IaJeQIDAQABoyEwHzAdBgNVHQ4EFgQU/wzRzxsifMCz54SZ3HuF4P4jtzowDQYJKoZIhvcNAQELBQADggEBACaWlbJTObDai8+wmskHedKYb3FCfTwvH/sCRsygHIeDIi23CpoWeKt5FwXsSeqDMd0Hb6IMtYDG5rfGvhkNfunt3sutK0VpZZMNdSBmIXaUx4mBRRUsG4hpeWRrHRgTnxweDDVw4Mv+oYCmpY7eZ4SenISkSd/4qrXzFaI9NeZCY7Jg9vg1bev+NaUtD3C4As6GQ+mN8Rm2NG9vzgTDlKf4Wb5Exy7u9dMW1TChiy28ieVkETKdqwXcbhqM8GOLBUFicdmgP2y9aDGjb89BuaeoHJCGpWWCi3UZth14clVzC6p7ZD6fFx5tKMOL/hQvs3ugGtvFDWCsvcT8bB84RO8="], "issuer": "https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/v2.0"}, {"kty": "RSA", "use": "sig", "kid": "lHLIu4moKqzPcokwlfCRPHyjl5g", "x5t": "lHLIu4moKqzPcokwlfCRPHyjl5g", "n": "xlc-u9LJvOdbwAsgsYZpaJrgmrGHaEkoa_3_7Jvu4-Hb8LNtszrQy5Ik4CXgQ_uiLPt4-ePprX3klFAx91ahfd5LwX6mEQPT8WuHMDunx8MaNQrYNVvnOI1L5NxFBFV_6ghi_0d-cOslErcTMML2lbMCSjQ8jwltxz1Oy-Hd9wdY2pz2YC3WR4tHzAGreWGeOB2Vs2NLGv0U3CGSCMqpM9vxbWLZQPuCNpKF93RkxHj5bLng9U_rM6YScacEnTFlKIOOrk4pcVVdoSNNIK2uNUs1hHS1mBXuQjfceghzj3QQYHfp1Z5qWXPRIw3PDyn_1Sowe5UljLurkpj_8m3KnQ", "e": "AQAB", "x5c": ["MIIC6TCCAdGgAwIBAgIIT3fcexMa3ggwDQYJKoZIhvcNAQELBQAwIzEhMB8GA1UEAxMYbG9naW4ubWljcm9zb2Z0b25saW5lLnVzMB4XDTIzMDcxNDAwNDU0NFoXDTI4MDcxNDAwNDU0NFowIzEhMB8GA1UEAxMYbG9naW4ubWljcm9zb2Z0b25saW5lLnVzMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxlc+u9LJvOdbwAsgsYZpaJrgmrGHaEkoa/3/7Jvu4+Hb8LNtszrQy5Ik4CXgQ/uiLPt4+ePprX3klFAx91ahfd5LwX6mEQPT8WuHMDunx8MaNQrYNVvnOI1L5NxFBFV/6ghi/0d+cOslErcTMML2lbMCSjQ8jwltxz1Oy+Hd9wdY2pz2YC3WR4tHzAGreWGeOB2Vs2NLGv0U3CGSCMqpM9vxbWLZQPuCNpKF93RkxHj5bLng9U/rM6YScacEnTFlKIOOrk4pcVVdoSNNIK2uNUs1hHS1mBXuQjfceghzj3QQYHfp1Z5qWXPRIw3PDyn/1Sowe5UljLurkpj/8m3KnQIDAQABoyEwHzAdBgNVHQ4EFgQUCSJrrznFYz1BLqd17S8HFjGrAOAwDQYJKoZIhvcNAQELBQADggEBAAQHNudtmYpeh9x5+rGDVy6OYpTnQ2D5+rmgOHM5yRvgEnFBNuZ6bnr3Ap9nb6EM08juYKPaVyhkV+5axMl+dT8KOuCgrfcKvXqzdQ3BgVFkyU9XfajHzq3JALYpNkixCs/BvqRhXx2ecYxFHB2D671cOwhYIaMZdGtbmOOk8puYSgJ9DBqqn3pLksHmxLP656l/U3hPATTCdfDaNcTagIPx+Q2d9RBn8zOIa/p4CLsu3E0aJfDw3ljPD8inLJ2mpKq06TBfd5Rr/auwipb4J8Y/PHhef8b2kOf42fikIKAP538k9lLsXSowyPWn7KZDTEsku7xpyqvKvEiFkmaV+RY="], "issuer": "https://login.microsoftonline.com/9a95fd9e-005d-487a-9a01-d08c1eab2757/v2.0"}]})",
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSIsImtpZCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSJ9.eyJhdWQiOiJhcGk6Ly82ZGUxZGJiMy0xMGM2LTQ5ZTYtOWY2Ny0xMTlmMzA2ODRhMWIiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC85YTk1ZmQ5ZS0wMDVkLTQ4N2EtOWEwMS1kMDhjMWVhYjI3NTcvIiwiaWF0IjoxNjk5MjEzMTYxLCJuYmYiOjE2OTkyMTMxNjEsImV4cCI6MTY5OTIxNzA2MSwiYWlvIjoiRTJWZ1lKajEvdlc5OTR2bFpXbyt6SnQ0STBGNE9nQT0iLCJhcHBpZCI6IjZkZTFkYmIzLTEwYzYtNDllNi05ZjY3LTExOWYzMDY4NGExYiIsImFwcGlkYWNyIjoiMSIsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzlhOTVmZDllLTAwNWQtNDg3YS05YTAxLWQwOGMxZWFiMjc1Ny8iLCJvaWQiOiJhMzY1ODI3YS02YjMyLTQxMDAtYmY1MS01ZmJkM2MzZDFmNzQiLCJyaCI6IjAuQVgwQW52MlZtbDBBZWtpYUFkQ01IcXNuVjdQYjRXM0dFT1pKbjJjUm56Qm9TaHVhQUFBLiIsInN1YiI6ImEzNjU4MjdhLTZiMzItNDEwMC1iZjUxLTVmYmQzYzNkMWY3NCIsInRpZCI6IjlhOTVmZDllLTAwNWQtNDg3YS05YTAxLWQwOGMxZWFiMjc1NyIsInV0aSI6IndMVG5Dc2tRLVVLc3lDTjBUTFE0QVEiLCJ2ZXIiOiIxLjAifQ.h50DAbes4A4ffY2vNfkB000aRryrqBXF_nOYdVowPTkaBjdbVjNLDuYt6YNRVTZ9aWUsZ4M_LV7r4f9zQd830gXWm9i8ZEKnsbLdzMUWzY4Gxk3jHt4FsixZzw4XxdZO5oF2EpiD50m--t6sjUB9uouy93Llu7eGhk_Qg_HVvzAoclja_6bRPseJoL681iEOh8BZBG_W0d2OkEndZEGKVMa4vw3-lkEuo4wwnODru30_SXm7qwpFXoiqfwfK_ndGdDDbL__LyoWxWoWwwGyICK0SwnP7gIh2wrxvohQRItTDsZf5StO3H0zmeeE1bSkt_v8b8dRanOGd54oZK7k2wQ)",
    "https://sts.windows.net/9a95fd9e-005d-487a-9a01-d08c1eab2757/",
    "api://6de1dbb3-10c6-49e6-9f67-119f30684a1b",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user, "a365827a-6b32-4100-bf51-5fbd3c3d1f74"},
  },
  // Google ID Platform example
  {
    1699057482,
    R"({"keys":[{"e":"AQAB","alg":"RS256","kid":"a06af0b68a2119d692cac4abf415ff3788136f65","n":"yrIpMnHYrVPwlbC-IY8aU2Q6QKnLf_p1FQXNiTO9mWFdeYXP4cNF6QKWgy4jbVSrOs-4qLZbKwRvZhfTuuKW6fwj5lVZcNsq5dd6GXR65I8kwomMH-Zv_pDt9zLiiJCp5_GU6Klb8zMY_jEE1fZp88HIk2ci4GrmtPTbw8LHAkn0P54sQQqmCtzqAWp8qkZ-GGNITxMIdQMY225kX7Dx91ruCb26jPCvF5uOrHT-I6rFU9fZbIgn4T9PthruubbUCutKIR-JK8B7djf61f8ETuKomaHVbCcxA-Q7xD0DEJzeRMqiPrlb9nJszZjmp_VsChoQQg-wl0jFP-1Rygsx9w","use":"sig","kty":"RSA"},{"kid":"f5f4bf46e52b31d9b6249f7309ad0338400680cd","use":"sig","n":"q5hcowR4IuPiSvHbwj9Rv9j2XRnrgbAAFYBqoLBwUV5GVIiNPKnQBYa8ZEIK2naj9gqpo3DU9lx7d7RzeVlzCS5eUA2LV94--KbT0YgIJnApj5-hyDIaevI1Sf2YQr_cntgVLvxqfW1n9ZvbQSitz5Tgh0cplZvuiWMFPu4_mh6B3ShEKIl-qi-h0cZJlRcIf0ZwkfcDOTE8bqEzWUvlCpCH9FK6Mo9YLjw5LroBcHdUbOg3Keu0uW5SCEi-2XBQgCF6xF3kliciwwnv2HhCPyTiX0paM_sT2uKspYock-IQglQ2TExoJqbYZe6CInSHiAA68fkSkJQDnuRZE7XTJQ","e":"AQAB","alg":"RS256","kty":"RSA"},{"kty":"RSA","alg":"RS256","use":"sig","kid":"f833e8a7fe3fe4b878948219a1684afa373ca86f","e":"AQAB","n":"uB-3s136B_Vcme1zGQEg-Avs31_voau8BPKtvbYhB0QOHTtrXCF_wxIH5vWjl-5ts8up8Iy2kVnaItsecGohBAy_0kRgq8oi-n_cZ0i5bspAX5VW0peh_QU3KTlKSBaz3ZD9xMCDWuJFFniHuxLtJ4QtL4v2oDD3pBPNRPyIcZ_LKhH3-Jm-EAvubI5-6lB01zkP5x8f2mp2upqAmyex0jKFka2e0DOBavmGsGvKHKtTnE9oSOTDlhINgQPohoSmir89NRbEqqzeZVb55LWRl_hkiDDOZmcM_oJ8iUbm6vQu3YwCy-ef9wGYEij5GOWLmpYsws5vLVtTE2U-0C_ItQ"}]})",
    R"(eyJhbGciOiJSUzI1NiIsImtpZCI6ImY1ZjRiZjQ2ZTUyYjMxZDliNjI0OWY3MzA5YWQwMzM4NDAwNjgwY2QiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJteWFwcCIsImF6cCI6IjExNzM2NzgxMDA0ODY0MjA3NDkzOSIsImVtYWlsIjoidGVzdC03MTlAZGlyZWN0LXN1YmplY3QtNDA0MDE3LmlhbS5nc2VydmljZWFjY291bnQuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTY5OTA2MTA4MiwiaWF0IjoxNjk5MDU3NDgyLCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiIxMTczNjc4MTAwNDg2NDIwNzQ5MzkifQ.gm60Q6OiKzpnmTqh0BrmTkXzyUm2OEUrL8gfA4LBKdzXHucMhlqBA2LaNACv0LH2oqCxwy5BCdCaZWkHhIVJGCLMy7p__c_OO_1VUXwCtr9j2MtQaPPgOK8z1dT8-dmU3xXuWsEl5FX2k3JFiDlSUdgNrFs8o2uuJG_VtiLjV3u4jyHTS-vwcdYbFpH7ipS-bL-g-_uW8-qKAfJqk4V_fWXJ__tjNlvMRogweujpRpa-_1H9Cdk7avlRB_xbthUVFI2guE-0yMGDZhZrjI1vkA9uj1bpcH-85JiGPRlcGuikBnmB3ZUkJ6QffVfa4PwyYV78ybkPjtbcXjcp3X0AjQ)",
    "https://accounts.google.com",
    "myapp",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user,
      "117367810048642074939",
    },
  },
  // Okta example
  {
    1699395367,
    R"({"keys": [{"kty": "RSA", "alg": "RS256", "kid": "f4-dpAc7AqynmGcjjS9LLPCBEW2ezVfIbAqTHPyKe_Y", "use": "sig", "e": "AQAB", "n": "jRkJD3j3D5Zt2ZutvOxaJvgG9iCAi2NjXMAF8lSwFH_2YDc5XFAuFuvMeJp0O-RRPFuOqltp-TmC9v6b7lJaLbvenAf8YtwS5j1hfwekTDpEDjtDrd_57VsbMqCt8_2aEG5qOO8nMB2bNSNKwWL6OsDf8cZZl_v9ifeFD5L12JIzPaCskhL2SZVuHtUTwcuDgjLTGTVrPVmwJri8ZulmKu8t3DOqwwsr1ezqwcVtNucDxB-GeDKAkUCIdEbTEvpcVaQt-Z4i9AA-9tbXuXOQwLvy8ijt-Kaf0V7527LshRejwFCz7Ifz-orWaGyXbGOQDiJl6-hLd8kI8uTfU3zwWw"}]})",
    R"(eyJraWQiOiJmNC1kcEFjN0FxeW5tR2NqalM5TExQQ0JFVzJlelZmSWJBcVRIUHlLZV9ZIiwiYWxnIjoiUlMyNTYifQ.eyJ2ZXIiOjEsImp0aSI6IkFULk1ZcVRWaTVFT2V5U0NjeHZ2QUNENGVfN1pFTl8zanVSTmFfbDFnb3lTQXciLCJpc3MiOiJodHRwczovL2Rldi02OTc4NTY3OS5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6ImFwaTovL2RlZmF1bHQiLCJpYXQiOjE2OTkzOTUzNjcsImV4cCI6MTY5OTM5ODk2NywiY2lkIjoiMG9hZDVuZ3B2ZktZMXBqNlI1ZDciLCJzY3AiOlsibXlDdXN0b21TY29wZSJdLCJzdWIiOiIwb2FkNW5ncHZmS1kxcGo2UjVkNyJ9.Z70D28OtRHmgUZrBQ9XbdEONk-UfdPiGlFAuyREjn3GWVsGi4K9wwvLs8vGgcNR3r1IWBOTXPL-MXtptQDBhRwDkZDQWRUqaoBBul5mHFjSXGeSk1_YM4se4_jH9FZCP0vpavRIlsBpNwiOT9I9I9fkJp9qGA7IQ6LrxezKYpBbQUXcy7bw0oom84ej0ai2zP1_Yphtw6fYgZNOJ8xpf-wtBxzyZgW4Vb3ABnlMcS294h_VR39Cx09yfHXLqVgYactujzXxcAjYD2fCyFWHJ-5ZCZZxHWrM_wToCvoe7vytGTrAbOo2_hLhYw78o6cGXLGoHOe-akCpmGd9HzNIMCg)",
    "https://dev-69785679.okta.com/oauth2/default",
    "api://default",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user,
      "0oad5ngpvfKY1pj6R5d7",
    },
  },
  // AD-FS example
  {
    1700152921,
    R"({"keys": [{"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "oIi4qk4BIlAgCct70pqb7gMUfxY", "x5t": "oIi4qk4BIlAgCct70pqb7gMUfxY", "n": "wThO8OU2i4pmyKO7CmiuZJEi6A36vMPXAeng6-9_eU41OF5dgYm4QKelhd4OKKQ8_uQyaOTsyOGERL5gqfGoHd2RA2ic76khzDyI0f0a0JeU4w-wUxaNUI9j643K9XwXmtI5M07MWXFJqGkrOq7kdls9Hnb7n1X4-e2cFpk4AVA9RLWkfeaVDFdYbflyfI1DB9owJq--B3sdP0vjsE7ooLvWc3c7hCuyE98sMKDLacaipOBeJDuD3RdZ9IRH7I4s_6WpNzdtKNtev8JzKC75lXsd8gjDnncYFeLfrtzGe-2mfy7G71Xz2cDUyxaSsYZ3dAbf2vWJFPIi20p9wUU6I_r-hBmfdmHrb4EjEdEZtPJxxiu23jZsONXIN6B4ikMY65gjn2KL_Rf9TKznDfUjJBqdZIMUmn1mFcIc-srAvLIQa3Jq9G2KCzFLWLB-DAHGv4AFGj6XJWk9ybWJdpmmrcPl_s96NhHvkVt016BU3EpGAlDG_Rur57zkismNlPS0j-I-pVqG1ErBsdDqlqjEgKSgubT7NkMqsRGP7l40sqGfXmq6G4ZD_Fr1Y7nTF2CHa3GDF_y59YoxGwkxcvcvIqKH9igo3RIZsuXoIwf6g8Ge7PlWDGtNBSP0sU96Vld1K2Eecn1FqdhPGL0pHMfVj5CxIYZtTbr3KkQB77wMNXk", "e": "AQAB", "x5c": ["MIIE9DCCAtygAwIBAgIQT06T4p1LDYJL8Ha7h1T8TjANBgkqhkiG9w0BAQsFADA2MTQwMgYDVQQDEytBREZTIFNpZ25pbmcgLSBFQzJBTUFaLTVBMkZLSTYuUkVEUEFOREEuQ09NMB4XDTIzMTEwOTIyNTgxOVoXDTI0MTEwODIyNTgxOVowNjE0MDIGA1UEAxMrQURGUyBTaWduaW5nIC0gRUMyQU1BWi01QTJGS0k2LlJFRFBBTkRBLkNPTTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAME4TvDlNouKZsijuwpormSRIugN+rzD1wHp4Ovvf3lONTheXYGJuECnpYXeDiikPP7kMmjk7MjhhES+YKnxqB3dkQNonO+pIcw8iNH9GtCXlOMPsFMWjVCPY+uNyvV8F5rSOTNOzFlxSahpKzqu5HZbPR52+59V+PntnBaZOAFQPUS1pH3mlQxXWG35cnyNQwfaMCavvgd7HT9L47BO6KC71nN3O4QrshPfLDCgy2nGoqTgXiQ7g90XWfSER+yOLP+lqTc3bSjbXr/Ccygu+ZV7HfIIw553GBXi367cxnvtpn8uxu9V89nA1MsWkrGGd3QG39r1iRTyIttKfcFFOiP6/oQZn3Zh62+BIxHRGbTyccYrtt42bDjVyDegeIpDGOuYI59ii/0X/Uys5w31IyQanWSDFJp9ZhXCHPrKwLyyEGtyavRtigsxS1iwfgwBxr+ABRo+lyVpPcm1iXaZpq3D5f7PejYR75FbdNegVNxKRgJQxv0bq+e85IrJjZT0tI/iPqVahtRKwbHQ6paoxICkoLm0+zZDKrERj+5eNLKhn15quhuGQ/xa9WO50xdgh2txgxf8ufWKMRsJMXL3LyKih/YoKN0SGbLl6CMH+oPBnuz5VgxrTQUj9LFPelZXdSthHnJ9RanYTxi9KRzH1Y+QsSGGbU269ypEAe+8DDV5AgMBAAEwDQYJKoZIhvcNAQELBQADggIBAE2yzb07icvwvh0v4XFq+Z6D1dBfogy0Xx7MYmdHRQUFwSIoCPOxe0r5Ec6WurdANl3+ByrRot+XHfxwACSo8xF55E+sRq9pf9oxsRzZ1BrQrx/K2k8Efc+otQAfzc8ZuPOUHMCyGoEkFHIIG+WG28KylfF8tKCGgCEiqHl4fitY4U/Qaib0+ZpIo6A44H8P8t93Hh+aQrituvLiacriI4cRL4hX7qiRoCBVzjN64jGpVvhE1xavs8GCFo17g7h2e9Dj4Etlrj2qGopjr740T1KYfic3l9TS8Npp55FupbfYNWKf7MfHfaFurYRu1hx8nKiCQtbA/bULTorFFroHrKBD+50PqLLFZmJ9XzWxKAG8eNno4lwxDlwWLzeV9u/fPXAL2CB3axK+BZ1N7aJ9dcd4IhfcxOEMTvhIQ02bIwE59h6w+iwwk0O/jlXyScIUdCBNnunnArsTKJtLfuN6+WXkTjpgpPZSwgj+kb9obE2Q3J0EvlNDAARedZsM9ycDENU6oJHbYT/lWGr3yWEYHySDJyRmEEDA8JnN2OwqO7O9QRyEnogY4IX7zh7085EAQoMhXyoo2ihtUbsFBGtwjCFnda8WywbxPxogp2b0q08kb+yZvEFg4/Ip4ExTHBhuu97nbrHZ2iOcZvZvpmyLtGGKtdGXyBqggn6UdJ4x9GSm"]}]})",
    R"(eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im9JaTRxazRCSWxBZ0NjdDcwcHFiN2dNVWZ4WSIsImtpZCI6Im9JaTRxazRCSWxBZ0NjdDcwcHFiN2dNVWZ4WSJ9.eyJhdWQiOiJodHRwczovL2xvY2FsaG9zdDozMDMwIiwiaXNzIjoiaHR0cDovL0VDMkFNQVotNUEyRktJNi5SRURQQU5EQS5DT00vYWRmcy9zZXJ2aWNlcy90cnVzdCIsImlhdCI6MTcwMDE1MjkyMSwibmJmIjoxNzAwMTUyOTIxLCJleHAiOjE3MDAxNTY1MjEsImFwcHR5cGUiOiJDb25maWRlbnRpYWwiLCJhcHBpZCI6Ijc2YjI5NjZiLWIxYjAtNGFlYy1hNWRhLTY2MWNmZmUwOTVlMCIsImF1dGhtZXRob2QiOiJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvYXV0aGVudGljYXRpb25tZXRob2QvcGFzc3dvcmQiLCJhdXRoX3RpbWUiOiIyMDIzLTExLTE2VDE2OjQyOjAxLjIzOVoiLCJ2ZXIiOiIxLjAiLCJzY3AiOiJvcGVuaWQifQ.vUqRYrGhkNOtKEWaTA1cCT24aH8BqbknETp6P3ndlLCPoLi_Ui3B06KyxilVU4o8kEDmWfsrQGLeBUnSF_Q_eODsMSIHWysw3vMUU-BIP2yZKKWvSphUA1X7uqBV1qTu-jJRqgC-9ubXoXMB7iiwhQenxvmtDC_1OvGDGEmb9dQ655Fo3wUfYuDlu1QbhlvbOKC48JAbuHQh1J_ZHWNHCOQdihDg2vsUhm9XO20CzC5I6fSa7FPWkwyl3LrWSwB4TSJfeNwTIftTJ5D_v_WQIXclo2wYNjdWxNB1_DrkNM0D7rHdGFkGxW5c4MlFHZ9Zw7x6FybjBamFOriCVZTohCfDVBQIwx2KHvy5Zlcyt5xmDie7ghOgiWxFbzd9jxssbIc2EhIRM3B3bVh4ulTlpABqw3GCwUR-ameW7V5fWnNr3UD9UqvO9F2eQCeQdQzXKGHCgamUzk0K2zlIZjV12JRYKwwvJN5OuyNHeVzS38E4FXNZzyXmJinBsv7800avvcqD1ivR9RhOnU2BIt-KTia2e3v7D_GHhS4e5RP4F59aNNVZbFCZK5ElRTESsmurIQ449p208acvGr3wLePbPXUx3KuC5VWaFam4m1NCXgJy6yvkZl61xLvUVcmxUQXjAistrBRzQ1lhnmsh8vhcxGluCNRqyU-ks5YdG-xpoGk)",
    "http://EC2AMAZ-5A2FKI6.REDPANDA.COM/adfs/services/trust",
    "https://localhost:3030",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user, "76b2966b-b1b0-4aec-a5da-661cffe095e0"},
    security::oidc::principal_mapping_rule{
      json::Pointer{"/appid"}, security::tls::rule{}},
  },
});
BOOST_DATA_TEST_CASE(test_oidc_authenticate, bdata::make(oidc_auth_data), d) {
    auto jwks = oidc::jwks::make(ss::sstring{d.jwks});
    BOOST_REQUIRE(!jwks.has_error());

    auto jws = oidc::jws::make(ss::sstring{d.jws});
    BOOST_REQUIRE(!jws.has_error());

    oidc::verifier v;
    auto update = v.update_keys(std::move(jwks).assume_value());
    BOOST_REQUIRE(!update.has_error());

    auto auth = security::oidc::authenticate(
      std::move(jws).assume_value(),
      v,
      d.mapping,
      d.issuer,
      d.audience,
      d.clock_skew_tolerance,
      seastar::lowres_system_clock::from_time_t(d.now));
    if (d.auth_err != oidc::errc::success) {
        BOOST_REQUIRE_EQUAL(d.auth_err, auth.error());
        return;
    }
    BOOST_REQUIRE(!auth.has_error());
    BOOST_REQUIRE(auth.assume_value().principal == d.principal);
}

struct auth_error_test_data {
    time_t now;
    std::string_view token_payload;
    ss::sstring issuer;
    ss::sstring audience;
    std::chrono::seconds clock_skew_tolerance;
    oidc::errc auth_err;
    security::acl_principal principal;
    friend std::ostream&
    operator<<(std::ostream& os, const auth_error_test_data& r) {
        fmt::print(
          os,
          "token_payload: {}, issuer: {}, audience: {}, clock_skew_tolerance: "
          "{}, auth_err: {}",
          r.token_payload,
          r.issuer,
          r.audience,
          r.clock_skew_tolerance,
          r.auth_err);
        return os;
    }
};
const auto oidc_auth_error_data = std::to_array<auth_error_test_data>({
  // Correct
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "redpanda", "exp": 1695887942, "iat": 1695887942})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::success,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Incorrect issuer
  {1695887942,
   R"({"iss": "wrong", "sub": "subject", "aud": "redpanda", "exp": 1695887942, "iat": 1695887942})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::jwt_invalid_iss,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Incorrect aud
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "wrong", "exp": 1695887942, "iat": 1695887942})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::jwt_invalid_aud,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Expired
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "redpanda", "exp": 1695887941, "iat": 1695887942})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::jwt_invalid_exp,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Expired = skew should tolerate it
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "redpanda", "exp": 1695887941, "iat": 1695887942})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   1s,
   oidc::errc::success,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Issued in the future
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "redpanda", "exp": 1695887942, "iat": 1695887943})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::jwt_invalid_iat,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Issued in the future - tolerance should allow it
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "redpanda", "exp": 1695887942, "iat": 1695887943})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   1s,
   oidc::errc::success,
   security::acl_principal{security::principal_type::user, "subject"}},
  // NBF in the future
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "redpanda", "exp": 1695887944, "iat": 1695887942, "nbf": 1695887943})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::jwt_invalid_nbf,
   security::acl_principal{security::principal_type::user, "subject"}},
  // NBF in the future - tolerance should allow it
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": "redpanda", "exp": 1695887944, "iat": 1695887942, "nbf": 1695887943})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   1s,
   oidc::errc::success,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Correct aud list
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": ["wrong", "redpanda"], "exp": 1695887942, "iat": 1695887942})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::success,
   security::acl_principal{security::principal_type::user, "subject"}},
  // Correct aud list
  {1695887942,
   R"({"iss": "http://docker-rp-1:8080/realms/demorealm", "sub": "subject", "aud": ["redpanda", "wrong"], "exp": 1695887942, "iat": 1695887942})",
   "http://docker-rp-1:8080/realms/demorealm",
   "redpanda",
   0s,
   oidc::errc::success,
   security::acl_principal{security::principal_type::user, "subject"}},
});
BOOST_DATA_TEST_CASE(
  test_oidc_authenticate_errors, bdata::make(oidc_auth_error_data), d) {
    json::Document header;
    header.Parse(R"({"alg": "RS256", "typ": "JWT", "kid": "42"})");
    json::Document payload;
    payload.Parse(d.token_payload.data(), d.token_payload.length());
    auto jwt = oidc::jwt::make(std::move(header), std::move(payload));
    BOOST_REQUIRE(!jwt.has_error());
    auto auth = oidc::authenticate(
      jwt.assume_value(),
      security::oidc::principal_mapping_rule{},
      d.issuer,
      d.audience,
      d.clock_skew_tolerance,
      ss::lowres_system_clock::from_time_t(d.now));

    if (d.auth_err != oidc::errc::success) {
        BOOST_REQUIRE(auth.has_error());
        BOOST_REQUIRE_EQUAL(d.auth_err, auth.error());
        return;
    }
    BOOST_REQUIRE(!auth.has_error());
}

// BOOST_AUTO_TEST_CASE(test_)
