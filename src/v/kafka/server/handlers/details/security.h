/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/protocol/schemata/create_acls_request.h"
#include "security/acl.h"

namespace kafka::details {

/*
 * Conversions throw acl_conversion_error and the exception message via (what())
 * is generally what should be returned as the error message in kafka responses.
 *
 * Using an exception here eliminates the need to write c/go-style error
 * handling for the large number of fields that need to be converted.
 */
struct acl_conversion_error : std::exception {
    explicit acl_conversion_error(ss::sstring msg)
      : msg{std::move(msg)} {}
    const char* what() const noexcept final { return msg.c_str(); }
    ss::sstring msg;
};

inline security::acl_principal to_acl_principal(const ss::sstring& principal) {
    std::string_view view(principal);
    if (unlikely(!view.starts_with("User:"))) {
        throw acl_conversion_error(
          fmt::format("Invalid principal name: {{{}}}", principal));
    }
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    auto user = principal.substr(5);
    if (unlikely(user.empty())) {
        throw acl_conversion_error(
          fmt::format("Principal name cannot be empty"));
    }
    if (user == "*") {
        return security::acl_wildcard_user;
    }
    return security::acl_principal(
      security::principal_type::user, std::move(user));
}

inline security::acl_host to_acl_host(const ss::sstring& host) {
    if (host == "*") {
        return security::acl_host::wildcard_host();
    }
    try {
        return security::acl_host(host);
    } catch (const std::invalid_argument& e) {
        throw acl_conversion_error(
          fmt::format("Invalid host {}: {}", host, e.what()));
    }
}

inline security::resource_type to_resource_type(int8_t type) {
    switch (type) {
    case 2:
        return security::resource_type::topic;
    case 3:
        return security::resource_type::group;
    case 4:
        return security::resource_type::cluster;
    case 5: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::resource_type::transactional_id;
    default:
        throw acl_conversion_error(
          fmt::format("Invalid resource type: {}", type));
    }
}

inline security::pattern_type to_pattern_type(int8_t type) {
    switch (type) {
    case 3:
        return security::pattern_type::literal;
    case 4:
        return security::pattern_type::prefixed;
    default:
        throw acl_conversion_error(
          fmt::format("Invalid resource pattern type: {}", type));
    }
}

inline security::acl_operation to_acl_operation(int8_t op) {
    switch (op) {
    case 2:
        return security::acl_operation::all;
    case 3:
        return security::acl_operation::read;
    case 4:
        return security::acl_operation::write;
    case 5: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::create;
    case 6: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::remove;
    case 7: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::alter;
    case 8: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::describe;
    case 9: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::cluster_action;
    case 10: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::describe_configs;
    case 11: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::alter_configs;
    case 12: // NOLINT(cppcoreguidelines-avoid-magic-numbers)
        return security::acl_operation::idempotent_write;
    default:
        throw acl_conversion_error(fmt::format("Invalid operation: {}", op));
    }
}

inline security::acl_permission to_acl_permission(int8_t perm) {
    switch (perm) {
    case 2:
        return security::acl_permission::deny;
    case 3:
        return security::acl_permission::allow;
    default:
        throw acl_conversion_error(fmt::format("Invalid permission: {}", perm));
    }
}

/*
 * convert kafka acl message into redpanda internal acl representation
 */
inline security::acl_binding to_acl_binding(const creatable_acl& acl) {
    if (acl.resource_name.empty()) {
        throw acl_conversion_error("Empty resource name");
    }

    security::resource_pattern pattern(
      to_resource_type(acl.resource_type),
      acl.resource_name,
      to_pattern_type(acl.resource_pattern_type));

    if (
      pattern.resource() == security::resource_type::cluster
      && pattern.name() != security::default_cluster_name) {
        throw acl_conversion_error(
          fmt::format("Invalid cluster name: {}", pattern.name()));
    }

    security::acl_entry entry(
      to_acl_principal(acl.principal),
      to_acl_host(acl.host),
      to_acl_operation(acl.operation),
      to_acl_permission(acl.permission_type));

    return security::acl_binding(std::move(pattern), std::move(entry));
}

} // namespace kafka::details
