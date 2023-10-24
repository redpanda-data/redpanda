/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "kafka/protocol/types.h"
#include "net/unresolved_address.h"
#include "security/acl.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/types.h"
#include "security/audit/types.h"
#include "security/authorizer.h"
#include "security/mtls.h"
#include "security/request_auth.h"
#include "security/types.h"
#include "utils/string_switch.h"

#include <seastar/http/handlers.hh>
#include <seastar/http/request.hh>

#include <iterator>
#include <type_traits>

namespace security::audit {

event_type kafka_api_to_event_type(kafka::api_key);

enum class audit_resource_type : int8_t {
    topic,
    group,
    cluster,
    transactional_id,
    acl_binding,
    acl_binding_filter
};

std::ostream& operator<<(std::ostream&, audit_resource_type);

template<typename T>
concept AuditableResource = std::is_same_v<T, model::topic>
                            || std::is_same_v<T, kafka::group_id>
                            || std::is_same_v<T, security::acl_cluster_name>
                            || std::is_same_v<T, kafka::transactional_id>
                            || std::is_same_v<T, security::acl_binding>
                            || std::is_same_v<T, security::acl_binding_filter>;

template<AuditableResource T>
consteval audit_resource_type get_audit_resource_type() {
    if constexpr (std::is_same_v<T, model::topic>) {
        return audit_resource_type::topic;
    } else if constexpr (std::is_same_v<T, kafka::group_id>) {
        return audit_resource_type::group;
    } else if constexpr (std::is_same_v<T, security::acl_cluster_name>) {
        return audit_resource_type::cluster;
    } else if constexpr (std::is_same_v<T, kafka::transactional_id>) {
        return audit_resource_type::transactional_id;
    } else if constexpr (std::is_same_v<T, security::acl_binding>) {
        return audit_resource_type::acl_binding;
    } else if constexpr (std::is_same_v<T, security::acl_binding_filter>) {
        return audit_resource_type::acl_binding_filter;
    } else {
        static_assert(utils::unsupported_type<T>::value, "Unsupported type");
    }
}

template<typename T>
resource_detail transform_to_resource_detail(const T& v) {
    const auto get_resource_name = [](auto&& v) {
        if constexpr (std::is_same_v<T, security::acl_binding>) {
            return "create acl";
        } else if constexpr (std::is_same_v<T, security::acl_binding_filter>) {
            return "delete acl";
        } else {
            return v();
        }
    };

    const auto get_resource_data =
      [](auto&& v) -> std::optional<acl_binding_detail> {
        if constexpr (std::is_same_v<T, security::acl_binding>) {
            return acl_binding_detail{
              .resource_type = fmt::format("{}", v.pattern().resource()),
              .resource_name = v.pattern().name(),
              .pattern_type = fmt::format("{}", v.pattern().pattern()),
              .acl_principal = fmt::format("{}", v.entry().principal()),
              .acl_host = fmt::format("{}", v.entry().host()),
              .acl_operation = fmt::format("{}", v.entry().operation()),
              .acl_permission = fmt::format("{}", v.entry().permission())};
        } else if constexpr (std::is_same_v<T, security::acl_binding_filter>) {
            const auto print_or_nothing =
              [](const auto& val) -> std::optional<ss::sstring> {
                if (val.has_value()) {
                    return fmt::format("{}", val.value());
                } else {
                    return std::nullopt;
                }
            };
            return acl_binding_detail{
              .resource_type = print_or_nothing(v.pattern().resource()),
              .resource_name = print_or_nothing(v.pattern().name()),
              .pattern_type = print_or_nothing(v.pattern().pattern()),
              .acl_principal = print_or_nothing(v.entry().principal()),
              .acl_host = print_or_nothing(v.entry().host()),
              .acl_operation = print_or_nothing(v.entry().operation()),
              .acl_permission = print_or_nothing(v.entry().permission())};
        } else {
            return std::nullopt;
        }
    };

    return {
      .name = get_resource_name(v),
      .type = fmt::format("{}", get_audit_resource_type<T>()),
      .data = get_resource_data(v),
    };
}

template<AuditableResource T>
std::vector<resource_detail>
create_resource_details(const std::vector<T>& resources) {
    std::vector<resource_detail> resource_details;
    resource_details.reserve(resources.size());
    std::transform(
      resources.begin(),
      resources.end(),
      std::back_inserter(resource_details),
      [](auto&& v) { return transform_to_resource_detail(v); });

    return resource_details;
}

template<typename Func>
concept returns_auditable_resource_vector = requires(Func func) {
    {
        func()
    } -> std::same_as<
      std::vector<typename std::remove_cvref_t<decltype(func())>::value_type>>;

    requires AuditableResource<
      typename std::remove_cvref_t<decltype(func())>::value_type>;
};

api_activity make_api_activity_event(
  ss::httpd::const_req req,
  const request_auth_result& auth_result,
  const ss::sstring& svc_name,
  bool authorized,
  const std::optional<std::string_view>& reason);

authentication make_authentication_event(
  ss::httpd::const_req req,
  const request_auth_result& r,
  const ss::sstring& svc_name);

authentication make_authentication_event(
  const ss::sstring& auth_protocol,
  const ss::socket_address& local_address,
  std::string_view service_name,
  ss::net::inet_address client_addr,
  uint16_t client_port,
  std::optional<std::string_view> client_id,
  authentication::used_cleartext is_cleartext,
  security::audit::user user);

authentication make_authentication_event(
  const security::tls::mtls_state& mtls_state,
  const ss::socket_address& local_address,
  std::string_view service_name,
  ss::net::inet_address client_addr,
  uint16_t client_port,
  std::optional<std::string_view> client_id);

authentication make_authentication_failure_event(
  ss::httpd::const_req req,
  const security::credential_user& r,
  const ss::sstring& svc_name,
  const ss::sstring& reason);

authentication make_authentication_failure_event(
  const ss::sstring& auth_protocol,
  const ss::socket_address& local_address,
  std::string_view service_name,
  ss::net::inet_address client_addr,
  uint16_t client_port,
  std::optional<std::string_view> client_id,
  authentication::used_cleartext is_cleartext,
  const ss::sstring& reason,
  security::audit::user user);

/// Creates a security::audit::api_activity event from an authorization result
///
/// Fills the appropriate structures for an authorized connection event
api_activity make_api_activity_event(
  std::string_view operation_name,
  const security::auth_result& auth_result,
  const ss::socket_address& local_address,
  std::string_view service_name,
  ss::net::inet_address client_addr,
  uint16_t client_port,
  std::optional<std::string_view> client_id,
  std::vector<resource_detail> additional_resources);

} // namespace security::audit
