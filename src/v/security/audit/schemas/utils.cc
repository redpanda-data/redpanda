/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "security/audit/schemas/utils.h"

#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/types.h"
#include "utils/request_auth.h"

#include <boost/algorithm/string/predicate.hpp>

namespace security::audit {

namespace {

api_activity::activity_id http_method_to_activity_id(std::string_view method) {
    return string_switch<api_activity::activity_id>(method)
      .match_all("POST", "post", api_activity::activity_id::create)
      .match_all("GET", "get", api_activity::activity_id::read)
      .match_all("PUT", "put", api_activity::activity_id::update)
      .match_all("DELETE", "delete", api_activity::activity_id::delete_id)
      .default_match(api_activity::activity_id::unknown);
}

uniform_resource_locator
uri_from_ss_http_request(const ss::http::request& req) {
    return {
      .hostname = req.get_header("Host"),
      .path = req._url,
      .port = port_t{req.get_server_address().port()},
      .scheme = req.get_protocol_name(),
      .url_string = req.get_url()};
}

http_request from_ss_http_request(const ss::http::request& req) {
    using ss_http_headers_t = decltype(req._headers);
    using ss_http_headers_value_t = ss_http_headers_t::value_type;
    const auto get_headers = [](const ss_http_headers_t& headers) {
        const auto sanitize_header =
          [](const ss_http_headers_value_t& kv) -> ss::sstring {
            constexpr auto sensitive_headers = std::to_array<std::string_view>(
              {"Authorization", "Cookie"});

            if (
              std::find_if(
                sensitive_headers.begin(),
                sensitive_headers.end(),
                [&kv](std::string_view s) {
                    return boost::iequals(s, kv.first);
                })
              != sensitive_headers.end()) {
                return "******";
            } else {
                return kv.second;
            }
        };
        std::vector<http_header> audit_headers;
        std::transform(
          headers.begin(),
          headers.end(),
          std::back_inserter(audit_headers),
          [sanitize_header = std::move(sanitize_header)](const auto& kv) {
              return http_header{
                .name = kv.first, .value = sanitize_header(kv)};
          });
        return audit_headers;
    };
    return http_request{
      .http_headers = get_headers(req._headers),
      .http_method = req._method,
      .url = uri_from_ss_http_request(req),
      .user_agent = req.get_header("User-Agent"),
      .version = req._version};
}

network_endpoint from_ss_endpoint(const ss::socket_address& sa) {
    return network_endpoint{
      .addr = net::unresolved_address(
        fmt::format("{}", sa.addr()), sa.port(), sa.addr().in_family())};
}

/// TODO: Via ACLs metadata return correct response
api_activity_unmapped unmapped_data() { return api_activity_unmapped{}; }

user user_from_request_auth_result(const request_auth_result& r) {
    auto& username = r.get_username();

    return {
      .name = username.empty() ? "{{anonymous}}" : username,
      .type_id = r.is_authenticated()
                   ? (r.is_superuser() ? user::type::admin : user::type::user)
                   : user::type::unknown,
    };
}

actor actor_from_request_auth_result(
  const request_auth_result& r,
  bool authorized,
  const std::optional<std::string_view>& reason) {
    auto u = user_from_request_auth_result(r);
    std::vector<authorization_result> auths{
      {.decision = authorized ? "authorized" : "denied",
       .policy = policy{
         .desc = ss::sstring{reason.value_or(
           r.is_auth_required() ? "" : "Auth Disabled")},
         .name = "Admin httpd authorizer"}}};

    return {.authorizations = std::move(auths), .user = std::move(u)};
}

template<typename Clock>
timestamp_t create_timestamp_t(std::chrono::time_point<Clock> time_point) {
    return timestamp_t(std::chrono::duration_cast<std::chrono::milliseconds>(
                         time_point.time_since_epoch())
                         .count());
}

template<typename Clock>
timestamp_t create_timestamp_t() {
    return create_timestamp_t(Clock::now());
}

} // namespace

api_activity make_api_activity_event(
  ss::httpd::const_req req,
  const request_auth_result& auth_result,
  bool authorized,
  const std::optional<std::string_view>& reason) {
    auto act = actor_from_request_auth_result(auth_result, authorized, reason);
    return {
      http_method_to_activity_id(req._method),
      std::move(act),
      api{.operation = req._method},
      from_ss_endpoint(req.get_server_address()),
      from_ss_http_request(req),
      {},
      severity_id::informational,
      from_ss_endpoint(req.get_client_address()),
      authorized ? api_activity::status_id::success
                 : api_activity::status_id::failure,
      create_timestamp_t<std::chrono::system_clock>(),
      unmapped_data()};
}

authentication make_authentication_event(
  ss::httpd::const_req req, const request_auth_result& r) {
    return {
      authentication::activity_id::logon,
      r.get_sasl_mechanism(),
      from_ss_endpoint(req.get_server_address()),
      boost::iequals(req.get_protocol_name(), "https")
        ? authentication::used_cleartext::no
        : authentication::used_cleartext::yes, // If HTTPS then not cleartext
      authentication::used_mfa::no,
      from_ss_endpoint(req.get_client_address()),
      severity_id::informational,
      r.is_authenticated() ? authentication::status_id::success
                           : authentication::status_id::failure,
      std::nullopt,
      create_timestamp_t<std::chrono::system_clock>(),
      user_from_request_auth_result(r)};
}

authentication make_authentication_failure_event(
  ss::httpd::const_req req,
  const security::credential_user& r,
  const ss::sstring& reason) {
    return {
      authentication::activity_id::logon,
      authentication::auth_protocol_id::unknown,
      from_ss_endpoint(req.get_server_address()),
      boost::iequals(req.get_protocol_name(), "https")
        ? authentication::used_cleartext::no
        : authentication::used_cleartext::yes, // If HTTPS then not cleartext
      authentication::used_mfa::no,
      from_ss_endpoint(req.get_client_address()),
      severity_id::informational,
      authentication::status_id::failure,
      reason,
      create_timestamp_t<std::chrono::system_clock>(),
      user{.name = r, .type_id = user::type::unknown}};
}

} // namespace security::audit
