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
   // Keycloak
   {R"({"issuer":"http://docker-rp-1:8080/realms/demorealm","authorization_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/auth","token_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token","introspection_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token/introspect","userinfo_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/userinfo","end_session_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/logout","frontchannel_logout_session_supported":true,"frontchannel_logout_supported":true,"jwks_uri":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/certs","check_session_iframe":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/login-status-iframe.html","grant_types_supported":["authorization_code","implicit","refresh_token","password","client_credentials","urn:ietf:params:oauth:grant-type:device_code","urn:openid:params:grant-type:ciba"],"acr_values_supported":["0","1"],"response_types_supported":["code","none","id_token","token","id_token token","code id_token","code token","code id_token token"],"subject_types_supported":["public","pairwise"],"id_token_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"id_token_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"id_token_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"userinfo_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512","none"],"userinfo_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"userinfo_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"request_object_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512","none"],"request_object_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"request_object_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"response_modes_supported":["query","fragment","form_post","query.jwt","fragment.jwt","form_post.jwt","jwt"],"registration_endpoint":"http://docker-rp-1:8080/realms/demorealm/clients-registrations/openid-connect","token_endpoint_auth_methods_supported":["private_key_jwt","client_secret_basic","client_secret_post","tls_client_auth","client_secret_jwt"],"token_endpoint_auth_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"introspection_endpoint_auth_methods_supported":["private_key_jwt","client_secret_basic","client_secret_post","tls_client_auth","client_secret_jwt"],"introspection_endpoint_auth_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"authorization_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"authorization_encryption_alg_values_supported":["RSA-OAEP","RSA-OAEP-256","RSA1_5"],"authorization_encryption_enc_values_supported":["A256GCM","A192GCM","A128GCM","A128CBC-HS256","A192CBC-HS384","A256CBC-HS512"],"claims_supported":["aud","sub","iss","auth_time","name","given_name","family_name","preferred_username","email","acr"],"claim_types_supported":["normal"],"claims_parameter_supported":true,"scopes_supported":["openid","web-origins","microprofile-jwt","roles","email","offline_access","profile","phone","acr","address"],"request_parameter_supported":true,"request_uri_parameter_supported":true,"require_request_uri_registration":true,"code_challenge_methods_supported":["plain","S256"],"tls_client_certificate_bound_access_tokens":true,"revocation_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/revoke","revocation_endpoint_auth_methods_supported":["private_key_jwt","client_secret_basic","client_secret_post","tls_client_auth","client_secret_jwt"],"revocation_endpoint_auth_signing_alg_values_supported":["PS384","ES384","RS384","HS256","HS512","ES256","RS256","HS384","ES512","PS256","PS512","RS512"],"backchannel_logout_supported":true,"backchannel_logout_session_supported":true,"device_authorization_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/auth/device","backchannel_token_delivery_modes_supported":["poll","ping"],"backchannel_authentication_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/ciba/auth","backchannel_authentication_request_signing_alg_values_supported":["PS384","ES384","RS384","ES256","RS256","ES512","PS256","PS512","RS512"],"require_pushed_authorization_requests":false,"pushed_authorization_request_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/par/request","mtls_endpoint_aliases":{"token_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token","revocation_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/revoke","introspection_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/token/introspect","device_authorization_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/auth/device","registration_endpoint":"http://docker-rp-1:8080/realms/demorealm/clients-registrations/openid-connect","userinfo_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/userinfo","pushed_authorization_request_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/par/request","backchannel_authentication_endpoint":"http://docker-rp-1:8080/realms/demorealm/protocol/openid-connect/ext/ciba/auth"}})",
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

const auto jwks_data = std::to_array<parse_test_data>(
  {// Auth0
   {R"({"keys":[{"kty":"RSA","use":"sig","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","e":"AQAB","kid":"tMQzailSAdaW4nojXxES9","x5t":"_LDYs6FFeEvJ-ng_a5-cxjUYaRw","x5c":["MIIDHTCCAgWgAwIBAgIJGbkt6m2aGpdZMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDRaFw0zNzA1MTUxMDM4NDRaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM7PGZNYQ/YYfFxn18ttSucs63mX+0fb2E0EzVRinQwx3we2EWwtRUcIuqRnGtxJUB7R+ZeEvdzFGkLZDfCWCPuWMhs8QTlcosHXP6ifozVmM1XWV+z7gqPod4pASZA+TfX/W/s79HP7h4bZbsx0JIWgsVSZ+beF0ORwJtan8SYKLVV260wxgHWRIe6eUSNpFPEQBhOvES1FbABl3Xn7O90BlaTP7hjjZanzEJNOXsMktoqqNeDHzMFJ+tIERJNcO64jBib3XRAE3vF3zk2cBK7H/GXBMVzFTnAZiJ+veMtQmcei4pq1iM1sp77guofDhQgu+iP04jx6ojGi16mjWm0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUIVqxco8b4MbVi3oKXvpvTQcAgxAwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQAz2efVzpAKHu7l7iPlBSRNRbXDnrxLR54ZtEX025l0yPdfM9fhPECYe9s2ze2TDupu2Is6XFk7ur5lUnON3qEKUU1kLRygz8oaj9lqsXdrfg9aTwXpxywWUelUKjLUL7FFZrXITSlG8SvwioTocpWPDaaDw0xefXnZRay5jjc9pl4E9uvd6K50SyQr7mY1ZEmNSYSftaoJGorFROaZs8Q0dc998JleYG1kFN0788eycCn4aRa0IKD/RfMXYj0j61T66vKnCALUfzFVtd/BUNwWdu0kRQceeuca4A+GWWxvYbDa4wJ/hEzWXT71BHUM6OhW4ls91wNTO9jdId/WJ3Rx"],"alg":"RS256"},{"kty":"RSA","use":"sig","n":"sP1lZhMdFVBrS06tFjwtuY0oRxDcZ8vPzUyUA5-vULpihTFDM-Jkeskvi3lAsZVkIv8iJVGSqdoBQyr3c27DWfDsUnH1HY1vGI6oB2m61uemCir104P07J6sZwO46hRnjp5vub2vJMjN_o4BOD2XiYXsTLg2gXsuh32HHKOr7ljbEZm4ygLeDVknGsSRIROxRWE8VPWjTQYRktAzwW8SMXI1wWxvg8wI6sKI4ydBMhQHO8ZomcIzdo66H31a45j2Jxn5JxKy-fMJbMg3qfTVh_9FMIOAjdVqtPN1g0TkoI8Y1H_iqkGq7tvqURnHBsbVkxwnaisJFJ1r67P7QnK3pw","e":"AQAB","kid":"NKxtg1GbhZJBVcnBjtSqI","x5t":"ENffeDpSw-aWSjJq0pCEhtDnYP0","x5c":["MIIDHTCCAgWgAwIBAgIJYGCzfjL18UZhMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDVaFw0zNzA1MTUxMDM4NDVaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALD9ZWYTHRVQa0tOrRY8LbmNKEcQ3GfLz81MlAOfr1C6YoUxQzPiZHrJL4t5QLGVZCL/IiVRkqnaAUMq93Nuw1nw7FJx9R2NbxiOqAdputbnpgoq9dOD9OyerGcDuOoUZ46eb7m9ryTIzf6OATg9l4mF7Ey4NoF7Lod9hxyjq+5Y2xGZuMoC3g1ZJxrEkSETsUVhPFT1o00GEZLQM8FvEjFyNcFsb4PMCOrCiOMnQTIUBzvGaJnCM3aOuh99WuOY9icZ+ScSsvnzCWzIN6n01Yf/RTCDgI3VarTzdYNE5KCPGNR/4qpBqu7b6lEZxwbG1ZMcJ2orCRSda+uz+0Jyt6cCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU3JMo7j8KyWFC2F184jYmV55OHjcwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQATHVMl6HagdRkYMP+ZZtdKN4ZSnc5HW0ttANDA5fM19OUFKEdRhQdlhsutD8yQtM4/XDIQ29p7q/665IgA3NJvIOQ98+aDub3Gs92yCnSZqCpSvaJGWkczjL5HQQAEDpSW+WAqAuoazkNdlPmeU0fkA/W92BaZaLw7oDiUrz/JT9pXcnN1SBOALfoj3BiGvvTRNFctFqX7nE8PCwj5tIrzYUVRGD8iNPj342G91D3Q+awp+YJNQxZ5MahWbdcoUJXTgIIOGkIOd0vZ1KcKUyADGMZp0U/pSAWbXXaJtzf8VZjBO0ySZGOMy73HYogUrOQGHoKecLuDIEWX75pOOH3d"],"alg":"RS256"}]})",
    oidc::errc::success},
   // Google RSA
   {R"({"keys":[{"kty":"RSA","kid":"838c06c62046c2d948affe137dd5310129f4d5d1","use":"sig","n":"hsYvCPtkUV7SIxwkOkJsJfhwV_CMdXU5i0UmY2QEs-Pa7v0-0y-s4EjEDtsQ8Yow6hc670JhkGBcMzhU4DtrqNGROXebyOse5FX0m0UvWo1qXqNTf28uBKB990mY42Icr8sGjtOw8ajyT9kufbmXi3eZKagKpG0TDGK90oBEfoGzCxoFT87F95liNth_GoyU5S8-G3OqIqLlQCwxkI5s-g2qvg_aooALfh1rhvx2wt4EJVMSrdnxtPQSPAtZBiw5SwCnVglc6OnalVNvAB2JArbqC9GAzzz9pApAk28SYg5a4hPiPyqwRv-4X1CXEK8bO5VesIeRX0oDf7UoM-pVAw","e":"AQAB","alg":"RS256"},{"n":"lWXY0XOj_ikSIDIvGOhfuRhQJAAj6BWsbbZ6P-PXRclzV32-QLB4GZHPPcH37Lou5pQsTQPvTETAfCLnglIRSbP8x1zA5tUakRlm5RiGF4kcWh5k60x8u0Uslx-d6EueKuY-KLHUVDuMULlHkYAScIdYnXz-Cnr6PFZj8RQezzdPVPH53Q8a_Z9b-vpGzsMS5gszITb-72OQNokojXdPVctl5WzSx-JnWbJxPiwHx_dSWgmTnyiYrZLqrqfampGdroaamtIXy0W8CAe0uCqcD1LunpfX-Q-RD1IycxnEaXSuUKhNhCcxtHWrozEyeD23Zja2WlcvHdYuTzyrvrvS9Q","kid":"7c0b6913fe13820a333399ace426e70535a9a0bf","use":"sig","e":"AQAB","alg":"RS256","kty":"RSA"}]})",
    oidc::errc::success},
   // Keycloak
   {R"({"keys":[{"kid":"EWGWeiW8jqUC_T7Fawi5ecRbrDOanB_uqtTgAOgJDPk","kty":"RSA","alg":"RSA-OAEP","use":"enc","n":"0OceMDMwIZCADl3oZnevxMBzB5GTLwFYevc7PFbw394YY4-Sdt4xqudPMSf2WhU22CidYiMVP1xvcZP7W_5eNLS9nNg3gsswgPiL52ZbWeWoOYx1WDVZzZur3YyjOR1iYvwhR572JumAfDE64y2cHpN2meVana9Zv7S4_yc9pFwGdPTJh7O76lamMjigfYKGF8BKSsFYPkxh_RgaRVqOaaF-CtXPBWmwqvJZ1r0h7GewcP5QeyaUUrZtTo7z0H6VovUGdwguL-OxhWVWZNyGDudtc50PPu3BBn47NpZIa4qCBEsMpm0mJ7YUvwAG-Ktd93uSquJkXvFjaVbTYj_iqQ","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4llfDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDQ5x4wMzAhkIAOXehmd6/EwHMHkZMvAVh69zs8VvDf3hhjj5J23jGq508xJ/ZaFTbYKJ1iIxU/XG9xk/tb/l40tL2c2DeCyzCA+IvnZltZ5ag5jHVYNVnNm6vdjKM5HWJi/CFHnvYm6YB8MTrjLZwek3aZ5Vqdr1m/tLj/Jz2kXAZ09MmHs7vqVqYyOKB9goYXwEpKwVg+TGH9GBpFWo5poX4K1c8FabCq8lnWvSHsZ7Bw/lB7JpRStm1OjvPQfpWi9QZ3CC4v47GFZVZk3IYO521znQ8+7cEGfjs2lkhrioIESwymbSYnthS/AAb4q133e5Kq4mRe8WNpVtNiP+KpAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAGXwcq9tUEOYmO9U8yiVqTxHT1mYHmSJYJITHyRH21tYZKqx8vtj83Xnywh1uioBMkD9YBZL3T5j1wNU3HOvucox62uaWTihd/fliGEMPcAJNqQIaoScOM0Ur6Mh03/qTU3fb53iyKvhd/MrOnO71hwXTFDOi5neIAopLqZHNPBwRiIsxHvlDZ0pN/maUYsTpDVBBYPZZpv3XklKyAmIrAL1655Xz+l6OU/586IY3b6N0EMMLQ0DWZedLK7ueneNV1m6bnc1uzBtj23VcafJN3AkgpQXrAKeREBWn8idyxCWjJZUkJxGUVUAYfBQ90J+WQl/YPtCOwwRcifdNl+9TyM="],"x5t":"2pddNGN6l8gxd7dKncQNOU67zn0","x5t#S256":"08bCcr5IDx1OPuLJcT3t5rSeJbY7Y1JHPF-aBCQfdio"},{"kid":"kf46eDlB9jtz2PpktQ6NQSfRwoU_tT3NqxlI8C4MGvQ","kty":"RSA","alg":"RS256","use":"sig","n":"3yqePLS9Q5Nf-q-ujodgMbblIwwPsEUCcXSxBS0eqrrSTNuPaHr-v_y_yyfKfxqhEmK_MBVvlXXmmwWZk6rWlR2hSdQW6Ih7UWLVaNOT35slcCfjQWQa3O0ZHxzemhgj0HHQ9x8t_AcK1wJd_hMjl3qwG12V1l-9-vSVIOrnl47YXC57K_j8MuNBPr1YdD4Rm9GGFxPQPRpc3nxTO2tFkkeGubfN6eJ226OvaA1uP6SlZEeA4KSjd-Bl0AAKotKcF9orHOuiceAarKs6uGZ84GocGaUtEEOli4e5bJrnNOXZDzCo34VdhFXFmJ2vU3g1XkY8e9wgA0fs9b3OhjqXIw","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4lk7DANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDfKp48tL1Dk1/6r66Oh2AxtuUjDA+wRQJxdLEFLR6qutJM249oev6//L/LJ8p/GqESYr8wFW+VdeabBZmTqtaVHaFJ1BboiHtRYtVo05PfmyVwJ+NBZBrc7RkfHN6aGCPQcdD3Hy38BwrXAl3+EyOXerAbXZXWX7369JUg6ueXjthcLnsr+Pwy40E+vVh0PhGb0YYXE9A9GlzefFM7a0WSR4a5t83p4nbbo69oDW4/pKVkR4DgpKN34GXQAAqi0pwX2isc66Jx4Bqsqzq4ZnzgahwZpS0QQ6WLh7lsmuc05dkPMKjfhV2EVcWYna9TeDVeRjx73CADR+z1vc6GOpcjAgMBAAEwDQYJKoZIhvcNAQELBQADggEBALGuaok4wAivQTlQON+CqhCYeG7ulkfwWoEdBlwLGmBmletdteIX3rC9ESQx9dHTMgk3+qFfH8BIfMVpiH202P5VfGygEdza3zj9C0ot/9HANjpff60XpPdqHmdwq/wpgaZR7ZfJqLjs1BwMZuGC/aQiGh70JBm7tvgXN+qrOrRnhsZiBT4Uec3mCIgEJlsrHqqBiJBohVK5c+EGh+NtvZAW5YNZgXV9SNXDwUS1z4AERoWnV4DtDuGWe0Q0IrFD/B8jFriOAOnu37RAhVt1Gvk/MM5UaHWsFAzyv8t99e0deFafiYmCqaERo1IoHAqCbIgU4nBid8IN5elj5+B4oq4="],"x5t":"8pMnWd5FLQJEK-62faplcAnJg3s","x5t#S256":"RiXQ3fgtOOATBmwUxB6sCnDP9aTthPtAdHo4ZXj0uPY"}]})",
    oidc::errc::success},
   // Empty object
   {R"({})", oidc::errc::jwks_invalid},
   // Not object
   {R"("not_object")", oidc::errc::jwks_invalid}});
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

    CryptoPP::AutoSeededRandomPool rng;
    auto verifiers = oidc::detail::make_rs256_verifier(doc, rng);
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
    operator<<(std::ostream& os, verify_test_data const& r) {
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
const auto oidc_verify_data = std::to_array<verify_test_data>(
  {// Auth0 example
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
    oidc::errc::success}});
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
    friend std::ostream& operator<<(std::ostream& os, auth_test_data const& r) {
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
const auto oidc_auth_data = std::to_array<auth_test_data>(
  {// Auth0 example
   {1695887942,
    R"({"keys":[{"kty":"RSA","use":"sig","n":"zs8Zk1hD9hh8XGfXy21K5yzreZf7R9vYTQTNVGKdDDHfB7YRbC1FRwi6pGca3ElQHtH5l4S93MUaQtkN8JYI-5YyGzxBOVyiwdc_qJ-jNWYzVdZX7PuCo-h3ikBJkD5N9f9b-zv0c_uHhtluzHQkhaCxVJn5t4XQ5HAm1qfxJgotVXbrTDGAdZEh7p5RI2kU8RAGE68RLUVsAGXdefs73QGVpM_uGONlqfMQk05ewyS2iqo14MfMwUn60gREk1w7riMGJvddEATe8XfOTZwErsf8ZcExXMVOcBmIn694y1CZx6LimrWIzWynvuC6h8OFCC76I_TiPHqiMaLXqaNabQ","e":"AQAB","kid":"tMQzailSAdaW4nojXxES9","x5t":"_LDYs6FFeEvJ-ng_a5-cxjUYaRw","x5c":["MIIDHTCCAgWgAwIBAgIJGbkt6m2aGpdZMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDRaFw0zNzA1MTUxMDM4NDRaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM7PGZNYQ/YYfFxn18ttSucs63mX+0fb2E0EzVRinQwx3we2EWwtRUcIuqRnGtxJUB7R+ZeEvdzFGkLZDfCWCPuWMhs8QTlcosHXP6ifozVmM1XWV+z7gqPod4pASZA+TfX/W/s79HP7h4bZbsx0JIWgsVSZ+beF0ORwJtan8SYKLVV260wxgHWRIe6eUSNpFPEQBhOvES1FbABl3Xn7O90BlaTP7hjjZanzEJNOXsMktoqqNeDHzMFJ+tIERJNcO64jBib3XRAE3vF3zk2cBK7H/GXBMVzFTnAZiJ+veMtQmcei4pq1iM1sp77guofDhQgu+iP04jx6ojGi16mjWm0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUIVqxco8b4MbVi3oKXvpvTQcAgxAwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQAz2efVzpAKHu7l7iPlBSRNRbXDnrxLR54ZtEX025l0yPdfM9fhPECYe9s2ze2TDupu2Is6XFk7ur5lUnON3qEKUU1kLRygz8oaj9lqsXdrfg9aTwXpxywWUelUKjLUL7FFZrXITSlG8SvwioTocpWPDaaDw0xefXnZRay5jjc9pl4E9uvd6K50SyQr7mY1ZEmNSYSftaoJGorFROaZs8Q0dc998JleYG1kFN0788eycCn4aRa0IKD/RfMXYj0j61T66vKnCALUfzFVtd/BUNwWdu0kRQceeuca4A+GWWxvYbDa4wJ/hEzWXT71BHUM6OhW4ls91wNTO9jdId/WJ3Rx"],"alg":"RS256"},{"kty":"RSA","use":"sig","n":"sP1lZhMdFVBrS06tFjwtuY0oRxDcZ8vPzUyUA5-vULpihTFDM-Jkeskvi3lAsZVkIv8iJVGSqdoBQyr3c27DWfDsUnH1HY1vGI6oB2m61uemCir104P07J6sZwO46hRnjp5vub2vJMjN_o4BOD2XiYXsTLg2gXsuh32HHKOr7ljbEZm4ygLeDVknGsSRIROxRWE8VPWjTQYRktAzwW8SMXI1wWxvg8wI6sKI4ydBMhQHO8ZomcIzdo66H31a45j2Jxn5JxKy-fMJbMg3qfTVh_9FMIOAjdVqtPN1g0TkoI8Y1H_iqkGq7tvqURnHBsbVkxwnaisJFJ1r67P7QnK3pw","e":"AQAB","kid":"NKxtg1GbhZJBVcnBjtSqI","x5t":"ENffeDpSw-aWSjJq0pCEhtDnYP0","x5c":["MIIDHTCCAgWgAwIBAgIJYGCzfjL18UZhMA0GCSqGSIb3DQEBCwUAMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTAeFw0yMzA5MDYxMDM4NDVaFw0zNzA1MTUxMDM4NDVaMCwxKjAoBgNVBAMTIWRldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALD9ZWYTHRVQa0tOrRY8LbmNKEcQ3GfLz81MlAOfr1C6YoUxQzPiZHrJL4t5QLGVZCL/IiVRkqnaAUMq93Nuw1nw7FJx9R2NbxiOqAdputbnpgoq9dOD9OyerGcDuOoUZ46eb7m9ryTIzf6OATg9l4mF7Ey4NoF7Lod9hxyjq+5Y2xGZuMoC3g1ZJxrEkSETsUVhPFT1o00GEZLQM8FvEjFyNcFsb4PMCOrCiOMnQTIUBzvGaJnCM3aOuh99WuOY9icZ+ScSsvnzCWzIN6n01Yf/RTCDgI3VarTzdYNE5KCPGNR/4qpBqu7b6lEZxwbG1ZMcJ2orCRSda+uz+0Jyt6cCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU3JMo7j8KyWFC2F184jYmV55OHjcwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQATHVMl6HagdRkYMP+ZZtdKN4ZSnc5HW0ttANDA5fM19OUFKEdRhQdlhsutD8yQtM4/XDIQ29p7q/665IgA3NJvIOQ98+aDub3Gs92yCnSZqCpSvaJGWkczjL5HQQAEDpSW+WAqAuoazkNdlPmeU0fkA/W92BaZaLw7oDiUrz/JT9pXcnN1SBOALfoj3BiGvvTRNFctFqX7nE8PCwj5tIrzYUVRGD8iNPj342G91D3Q+awp+YJNQxZ5MahWbdcoUJXTgIIOGkIOd0vZ1KcKUyADGMZp0U/pSAWbXXaJtzf8VZjBO0ySZGOMy73HYogUrOQGHoKecLuDIEWX75pOOH3d"],"alg":"RS256"}]})",
    R"(eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRNUXphaWxTQWRhVzRub2pYeEVTOSJ9.eyJpc3MiOiJodHRwczovL2Rldi1sdHhjaGNsczRpZ3pobzc4LnVzLmF1dGgwLmNvbS8iLCJzdWIiOiIzSkplSTR0bU1DNnY4bUNWQ1NEbkFHVmYydnJuSjBCVEBjbGllbnRzIiwiYXVkIjoibG9jYWxob3N0IiwiaWF0IjoxNjk1ODg3OTQyLCJleHAiOjE2OTU5NzQzNDIsImF6cCI6IjNKSmVJNHRtTUM2djhtQ1ZDU0RuQUdWZjJ2cm5KMEJUIiwic2NvcGUiOiJlbWFpbDIiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMifQ.W6rhgOjWcTPmpeLHiGSd668HHnZJvHgP0QSKU0F1vnin_UMIpei9IONfN28YSHlAabdUs2JAV70AvVeXB9IqUcEi_Cfubf3JRpmRcvfyApnmfcRktg1Rq0DVGVl6uBPlqX12SAQ4GPYe4BysUuMb8X-FU99wF6waCAQw8XLu_Texqy8QOQhW9vZtm5HC54-APn9PV6ZAtG989ihePOsauXUHNe2sqF_iJ1_7-nkqRbgb_Je-8UjahAkC54y5LPTMVFQvTB5lntf-sUyHl5oPH7P58M8eNUocOOGADUmrfKMYeSLacM_9mPvZR_uMbVX0iNt18KO6hKkIvAPrb4U8SA)",
    "https://dev-ltxchcls4igzho78.us.auth0.com/",
    "localhost",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user,
      "3JJeI4tmMC6v8mCVCSDnAGVf2vrnJ0BT@clients"}},
   // Keycloak example
   {1697645170,
    R"({"keys":[{"kid":"EWGWeiW8jqUC_T7Fawi5ecRbrDOanB_uqtTgAOgJDPk","kty":"RSA","alg":"RSA-OAEP","use":"enc","n":"0OceMDMwIZCADl3oZnevxMBzB5GTLwFYevc7PFbw394YY4-Sdt4xqudPMSf2WhU22CidYiMVP1xvcZP7W_5eNLS9nNg3gsswgPiL52ZbWeWoOYx1WDVZzZur3YyjOR1iYvwhR572JumAfDE64y2cHpN2meVana9Zv7S4_yc9pFwGdPTJh7O76lamMjigfYKGF8BKSsFYPkxh_RgaRVqOaaF-CtXPBWmwqvJZ1r0h7GewcP5QeyaUUrZtTo7z0H6VovUGdwguL-OxhWVWZNyGDudtc50PPu3BBn47NpZIa4qCBEsMpm0mJ7YUvwAG-Ktd93uSquJkXvFjaVbTYj_iqQ","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4llfDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDQ5x4wMzAhkIAOXehmd6/EwHMHkZMvAVh69zs8VvDf3hhjj5J23jGq508xJ/ZaFTbYKJ1iIxU/XG9xk/tb/l40tL2c2DeCyzCA+IvnZltZ5ag5jHVYNVnNm6vdjKM5HWJi/CFHnvYm6YB8MTrjLZwek3aZ5Vqdr1m/tLj/Jz2kXAZ09MmHs7vqVqYyOKB9goYXwEpKwVg+TGH9GBpFWo5poX4K1c8FabCq8lnWvSHsZ7Bw/lB7JpRStm1OjvPQfpWi9QZ3CC4v47GFZVZk3IYO521znQ8+7cEGfjs2lkhrioIESwymbSYnthS/AAb4q133e5Kq4mRe8WNpVtNiP+KpAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAGXwcq9tUEOYmO9U8yiVqTxHT1mYHmSJYJITHyRH21tYZKqx8vtj83Xnywh1uioBMkD9YBZL3T5j1wNU3HOvucox62uaWTihd/fliGEMPcAJNqQIaoScOM0Ur6Mh03/qTU3fb53iyKvhd/MrOnO71hwXTFDOi5neIAopLqZHNPBwRiIsxHvlDZ0pN/maUYsTpDVBBYPZZpv3XklKyAmIrAL1655Xz+l6OU/586IY3b6N0EMMLQ0DWZedLK7ueneNV1m6bnc1uzBtj23VcafJN3AkgpQXrAKeREBWn8idyxCWjJZUkJxGUVUAYfBQ90J+WQl/YPtCOwwRcifdNl+9TyM="],"x5t":"2pddNGN6l8gxd7dKncQNOU67zn0","x5t#S256":"08bCcr5IDx1OPuLJcT3t5rSeJbY7Y1JHPF-aBCQfdio"},{"kid":"kf46eDlB9jtz2PpktQ6NQSfRwoU_tT3NqxlI8C4MGvQ","kty":"RSA","alg":"RS256","use":"sig","n":"3yqePLS9Q5Nf-q-ujodgMbblIwwPsEUCcXSxBS0eqrrSTNuPaHr-v_y_yyfKfxqhEmK_MBVvlXXmmwWZk6rWlR2hSdQW6Ih7UWLVaNOT35slcCfjQWQa3O0ZHxzemhgj0HHQ9x8t_AcK1wJd_hMjl3qwG12V1l-9-vSVIOrnl47YXC57K_j8MuNBPr1YdD4Rm9GGFxPQPRpc3nxTO2tFkkeGubfN6eJ226OvaA1uP6SlZEeA4KSjd-Bl0AAKotKcF9orHOuiceAarKs6uGZ84GocGaUtEEOli4e5bJrnNOXZDzCo34VdhFXFmJ2vU3g1XkY8e9wgA0fs9b3OhjqXIw","e":"AQAB","x5c":["MIICoTCCAYkCBgGLQ4lk7DANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wHhcNMjMxMDE4MTYwNDE5WhcNMzMxMDE4MTYwNTU5WjAUMRIwEAYDVQQDDAlkZW1vcmVhbG0wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDfKp48tL1Dk1/6r66Oh2AxtuUjDA+wRQJxdLEFLR6qutJM249oev6//L/LJ8p/GqESYr8wFW+VdeabBZmTqtaVHaFJ1BboiHtRYtVo05PfmyVwJ+NBZBrc7RkfHN6aGCPQcdD3Hy38BwrXAl3+EyOXerAbXZXWX7369JUg6ueXjthcLnsr+Pwy40E+vVh0PhGb0YYXE9A9GlzefFM7a0WSR4a5t83p4nbbo69oDW4/pKVkR4DgpKN34GXQAAqi0pwX2isc66Jx4Bqsqzq4ZnzgahwZpS0QQ6WLh7lsmuc05dkPMKjfhV2EVcWYna9TeDVeRjx73CADR+z1vc6GOpcjAgMBAAEwDQYJKoZIhvcNAQELBQADggEBALGuaok4wAivQTlQON+CqhCYeG7ulkfwWoEdBlwLGmBmletdteIX3rC9ESQx9dHTMgk3+qFfH8BIfMVpiH202P5VfGygEdza3zj9C0ot/9HANjpff60XpPdqHmdwq/wpgaZR7ZfJqLjs1BwMZuGC/aQiGh70JBm7tvgXN+qrOrRnhsZiBT4Uec3mCIgEJlsrHqqBiJBohVK5c+EGh+NtvZAW5YNZgXV9SNXDwUS1z4AERoWnV4DtDuGWe0Q0IrFD/B8jFriOAOnu37RAhVt1Gvk/MM5UaHWsFAzyv8t99e0deFafiYmCqaERo1IoHAqCbIgU4nBid8IN5elj5+B4oq4="],"x5t":"8pMnWd5FLQJEK-62faplcAnJg3s","x5t#S256":"RiXQ3fgtOOATBmwUxB6sCnDP9aTthPtAdHo4ZXj0uPY"}]})",
    R"(eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJrZjQ2ZURsQjlqdHoyUHBrdFE2TlFTZlJ3b1VfdFQzTnF4bEk4QzRNR3ZRIn0.eyJleHAiOjE2OTc2NDU0NjksImlhdCI6MTY5NzY0NTE3MCwiYXV0aF90aW1lIjowLCJqdGkiOiJkZGYxZWE2MC01NzcwLTQ0MzctYTMzYy02MWM2MjllYmM3YWIiLCJpc3MiOiJodHRwOi8vZG9ja2VyLXJwLTE6ODA4MC9yZWFsbXMvZGVtb3JlYWxtIiwiYXVkIjoibXlhcHAiLCJzdWIiOiJiMDBmNjU0Yy04NDJiLTRlYjEtODhmMi0zMDRkMTcyNDZiYjUiLCJ0eXAiOiJJRCIsImF6cCI6Im15YXBwIiwiYXRfaGFzaCI6ImFHb2tGUGVpa2FDUm85WUZxbHlnbEEiLCJhY3IiOiIxIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRIb3N0IjoiMTcyLjE4LjAuMzYiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtbXlhcHAiLCJjbGllbnRBZGRyZXNzIjoiMTcyLjE4LjAuMzYiLCJlbWFpbCI6Im15YXBwQGN1c3RvbWVyLmNvbSIsImNsaWVudF9pZCI6Im15YXBwIn0.bYoI1XzW6Bw-dOnPAwBoB3tjM4xrt3loeUgwuipOmcm-yTheLSCf4qK3LOlzmuZdyF7TduDiizl4q8XIDe6XydrYL0tw4ebnQjcQo0wJNSZZQ3olZ65iJUoFHqK0YZuI4a4VDqMq0viEo2c2zFo3reVsr2XTtBgtU-iVyWpvmz6x-CceaGaKZpjDOcS8ixFcpSx-_mraQ18vr2yNHDO1DExg3Pc6FdbaiYApfldR9QIe2Oa5VTlVWXaqmjbFv3dA-FODIznEZ4tiN_8fZz5-BhkTTEupQGbCrYTIhR9rW_2xC_b8MhJCBMVFkMYATxLmJH9WPN5Fnzs2kiP7Xvs_eg)",
    "http://docker-rp-1:8080/realms/demorealm",
    "myapp",
    0s,
    oidc::errc::success,
    security::acl_principal{
      security::principal_type::user,
      "b00f654c-842b-4eb1-88f2-304d17246bb5"}}});
BOOST_DATA_TEST_CASE(test_oidc_authenticate, bdata::make(oidc_auth_data), d) {
    auto jwks = oidc::jwks::make(ss::sstring{d.jwks});
    BOOST_REQUIRE(!jwks.has_error());

    auto jws = oidc::jws::make(ss::sstring{d.jws});
    BOOST_REQUIRE(!jws.has_error());

    oidc::verifier v;
    auto update = v.update_keys(std::move(jwks).assume_value());
    BOOST_REQUIRE(!update.has_error());

    security::oidc::principal_mapping_rule mapping;

    auto auth = security::oidc::authenticate(
      std::move(jws).assume_value(),
      v,
      mapping,
      d.issuer,
      d.audience,
      d.clock_skew_tolerance,
      seastar::lowres_system_clock::from_time_t(d.now));
    if (d.auth_err != oidc::errc::success) {
        BOOST_REQUIRE_EQUAL(d.auth_err, auth.error());
        return;
    }
    BOOST_REQUIRE(!auth.has_error());
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
    operator<<(std::ostream& os, auth_error_test_data const& r) {
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
