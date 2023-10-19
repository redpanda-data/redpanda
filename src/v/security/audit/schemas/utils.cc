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
#include "security/audit/schemas/schemas.h"
#include "security/audit/schemas/types.h"
#include "security/request_auth.h"

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

network_endpoint from_ss_endpoint(
  const ss::socket_address& sa,
  std::optional<std::string_view> svc_name = std::nullopt) {
    return network_endpoint{
      .addr = net::unresolved_address(
        fmt::format("{}", sa.addr()), sa.port(), sa.addr().in_family()),
      .svc_name = ss::sstring{svc_name.value_or("")}};
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

std::ostream& operator<<(std::ostream& os, const category_uid& uid) {
    switch (uid) {
    case category_uid::system_activity:
        return os << "system_activity";
    case category_uid::findings:
        return os << "findings";
    case category_uid::iam:
        return os << "iam";
    case category_uid::network_activity:
        return os << "network_activity";
    case category_uid::discovery:
        return os << "discovery";
    case category_uid::application_activity:
        return os << "application_activity";
    }
}

std::ostream& operator<<(std::ostream& os, const class_uid& uid) {
    switch (uid) {
    case class_uid::file_system_activity:
        return os << "file_system_activity";
    case class_uid::kernel_extension_activity:
        return os << "kernel_extension_activity";
    case class_uid::kernel_activity:
        return os << "kernel_activity";
    case class_uid::memory_activity:
        return os << "memory_activity";
    case class_uid::module_activity:
        return os << "module_activity";
    case class_uid::scheduled_job_activity:
        return os << "scheduled_job_activity";
    case class_uid::process_activity:
        return os << "process_activity";
    case class_uid::security_finding:
        return os << "security_finding";
    case class_uid::account_change:
        return os << "account_change";
    case class_uid::authentication:
        return os << "authentication";
    case class_uid::authorize_session:
        return os << "authorize_session";
    case class_uid::entity_management:
        return os << "entity_management";
    case class_uid::user_access_management:
        return os << "user_access_management";
    case class_uid::group_management:
        return os << "group_management";
    case class_uid::network_activity:
        return os << "network_activity";
    case class_uid::http_activity:
        return os << "http_activity";
    case class_uid::dns_activity:
        return os << "dns_activity";
    case class_uid::dhcp_activity:
        return os << "dhcp_activity";
    case class_uid::rdp_activity:
        return os << "rdp_activity";
    case class_uid::smb_activity:
        return os << "smb_activity";
    case class_uid::ssh_activity:
        return os << "ssh_activity";
    case class_uid::ftp_activity:
        return os << "ftp_activity";
    case class_uid::email_activity:
        return os << "email_activity";
    case class_uid::network_file_activity:
        return os << "network_file_activity";
    case class_uid::email_file_activity:
        return os << "email_file_activity";
    case class_uid::email_url_activity:
        return os << "email_url_activity";
    case class_uid::device_inventory_info:
        return os << "device_inventory_info";
    case class_uid::device_config_state:
        return os << "device_config_state";
    case class_uid::web_resource_activity:
        return os << "web_resource_activity";
    case class_uid::application_lifecycle:
        return os << "application_lifecycle";
    case class_uid::api_activity:
        return os << "api_activity";
    case class_uid::web_resource_access_activity:
        return os << "web_resource_access_activity";
    }
}

std::ostream& operator<<(std::ostream& os, const ocsf_base_impl& impl) {
    return os << "{category: " << impl.get_category_uid()
              << ", class: " << impl.get_class_uid()
              << ", type_uid: " << impl.get_type_uid()() << "}";
}

api_activity make_api_activity_event(
  ss::httpd::const_req req,
  const request_auth_result& auth_result,
  const ss::sstring& svc_name,
  bool authorized,
  const std::optional<std::string_view>& reason) {
    auto act = actor_from_request_auth_result(auth_result, authorized, reason);
    return {
      http_method_to_activity_id(req._method),
      std::move(act),
      api{.operation = req._method, .service = {.name = svc_name}},
      from_ss_endpoint(req.get_server_address(), svc_name),
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
  ss::httpd::const_req req,
  const request_auth_result& r,
  const ss::sstring& svc_name) {
    return {
      authentication::activity_id::logon,
      r.get_sasl_mechanism(),
      from_ss_endpoint(req.get_server_address(), svc_name),
      boost::iequals(req.get_protocol_name(), "https")
        ? authentication::used_cleartext::no
        : authentication::used_cleartext::yes, // If HTTPS then not cleartext
      authentication::used_mfa::no,
      from_ss_endpoint(req.get_client_address()),
      service{.name = svc_name},
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
  const ss::sstring& svc_name,
  const ss::sstring& reason) {
    return {
      authentication::activity_id::logon,
      authentication::auth_protocol_id::unknown,
      from_ss_endpoint(req.get_server_address(), svc_name),
      boost::iequals(req.get_protocol_name(), "https")
        ? authentication::used_cleartext::no
        : authentication::used_cleartext::yes, // If HTTPS then not cleartext
      authentication::used_mfa::no,
      from_ss_endpoint(req.get_client_address()),
      service{.name = svc_name},
      severity_id::informational,
      authentication::status_id::failure,
      reason,
      create_timestamp_t<std::chrono::system_clock>(),
      user{.name = r, .type_id = user::type::unknown}};
}

} // namespace security::audit
