/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/audit/schemas/hashing_utils.h"

#include "hashing/combine.h"
#include "security/acl.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"

namespace security::audit {

template<typename T>
size_t audit_type_base_hash(
  category_uid ct_uid, class_uid c_uid, severity_id s_id, T activity_id) {
    /// metadata field explicity omitted since it is static
    size_t h = 0;
    return hash::combine(
      h, ct_uid, c_uid, s_id, get_ocsf_type(c_uid, activity_id));
}

size_t actor_hash(const security::auth_result& result) {
    /// user
    size_t h = 0;
    hash::combine(h, result.principal.name(), result.is_superuser);

    // policy
    enum class auth_state { disabled = 0, superuser = 1, nomatches = 2 };
    if (result.authorization_disabled) {
        hash::combine(h, auth_state::disabled);
    } else if (result.is_superuser) {
        hash::combine(h, auth_state::superuser);
    } else if (result.empty_matches) {
        hash::combine(h, auth_state::nomatches);
    } else {
        hash::combine(h, result.acl);
        hash::combine(h, result.resource_pattern);
    }
    /// authorization result (decision + policy)
    return hash::combine(h, result.authorized);
}

size_t api_activity_event_base_hash(
  std::string_view operation_name,
  const security::auth_result& auth_result,
  const ss::socket_address& local_address,
  std::string_view service_name,
  ss::net::inet_address client_addr) {
    auto crud = api_activity::op_to_crud(auth_result.operation);
    size_t h = audit_type_base_hash(
      category_uid::application_activity,
      class_uid::api_activity,
      severity_id::informational,
      crud);
    return hash::combine(
      h,
      actor_hash(auth_result),
      operation_name,
      service_name,
      local_address.addr(),
      client_addr,
      auth_result.authorized,
      auth_result.acl,
      auth_result.resource_pattern);
}

size_t http_request_hash(const ss::http::request& req) {
    size_t h = 0;
    /// request headers
    return hash::combine(
      h,
      req._headers,
      req._method,
      req._url,
      req.get_server_address().addr(),
      req.get_protocol_name(),
      req.get_url(),
      req._version);
}

size_t api_activity::hash(
  ss::httpd::const_req req,
  const request_auth_result& auth_result,
  const ss::sstring& svc_name,
  bool authorized,
  const std::optional<std::string_view>& reason) {
    const auto activity_id = http_method_to_activity_id(req._method);
    size_t h = audit_type_base_hash(
      category_uid::application_activity,
      class_uid::api_activity,
      severity_id::informational,
      activity_id);
    return hash::combine(
      h,
      auth_result.is_auth_required(),
      reason,
      svc_name,
      auth_result.get_username(),
      auth_result.is_superuser(),
      req.get_server_address().addr(),
      http_request_hash(req),
      req.get_client_address().addr(),
      authorized);
}

size_t api_activity::hash(
  ss::httpd::const_req req,
  const ss::sstring& user,
  const ss::sstring& svc_name) {
    const auto activity_id = http_method_to_activity_id(req._method);
    size_t h = audit_type_base_hash(
      category_uid::application_activity,
      class_uid::api_activity,
      severity_id::informational,
      activity_id);
    return hash::combine(
      h,
      user.empty() ? "{{anonymous}}" : user,
      svc_name,
      req._method,
      req.get_server_address().addr(),
      http_request_hash(req),
      req.get_client_address().addr());
}

size_t application_lifecycle::hash(
  application_lifecycle::activity_id activity_id,
  const std::optional<ss::sstring>& feature_name) {
    size_t h = audit_type_base_hash(
      category_uid::application_activity,
      class_uid::application_lifecycle,
      severity_id::informational,
      activity_id);
    return hash::combine(
      h,
      activity_id,
      feature_name,
      config::node().node_id().value_or(model::node_id{0})());
}

size_t authentication::hash(const authentication_event_options& options) {
    size_t h = audit_type_base_hash(
      category_uid::iam,
      class_uid::authentication,
      severity_id::informational,
      authentication::activity_id::logon);
    return hash::combine(
      h,
      authentication::activity_id::logon,
      options.auth_protocol,
      options.server_addr.host(),
      static_cast<bool>(options.is_cleartext),
      static_cast<bool>(authentication::used_mfa::no),
      options.svc_name.value_or(""),
      options.client_addr.host(),
      options.error_reason.has_value(),
      options.error_reason,
      options.user.name,
      options.user.type_id,
      options.user.domain,
      options.user.uid);
}
} // namespace security::audit
