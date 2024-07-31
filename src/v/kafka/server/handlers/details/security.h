/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/protocol/schemata/create_acls_request.h"
#include "kafka/protocol/schemata/delete_acls_request.h"
#include "kafka/protocol/schemata/describe_acls_request.h"
#include "kafka/server/request_context.h"
#include "model/validation.h"
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
    constexpr std::string_view user_prefix{"User:"};
    constexpr std::string_view role_prefix{"RedpandaRole:"};
    auto usr = view.starts_with(user_prefix);
    auto rol = !usr && view.starts_with(role_prefix);

    if (unlikely(!usr && !rol)) {
        throw acl_conversion_error(
          fmt::format("Invalid principal name: {{{}}}", principal));
    }

    auto name = principal.substr(usr ? user_prefix.size() : role_prefix.size());
    if (unlikely(name.empty())) {
        throw acl_conversion_error(
          fmt::format("Principal name cannot be empty"));
    }
    if (name == "*") {
        if (usr) {
            return security::acl_wildcard_user;
        } else {
            throw acl_conversion_error(
              fmt::format("Illegal wildcard role: {{{}}}", principal));
        }
    }
    return security::acl_principal(
      usr ? security::principal_type::user : security::principal_type::role,
      std::move(name));
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

    if (pattern.resource() == security::resource_type::topic) {
        auto errc = model::validate_kafka_topic_name(
          model::topic_view(pattern.name()));
        if (pattern.name() != "*" && errc) {
            throw acl_conversion_error(fmt::format(
              "ACL topic {} does not conform to kafka topic schema: {}",
              pattern.name(),
              errc.message()));
        }
    }

    security::acl_entry entry(
      to_acl_principal(acl.principal),
      to_acl_host(acl.host),
      to_acl_operation(acl.operation),
      to_acl_permission(acl.permission_type));

    return security::acl_binding(std::move(pattern), std::move(entry));
}

/*
 * build resource pattern filter bits
 */
inline security::resource_pattern_filter
to_resource_pattern_filter(const describe_acls_request_data& request) {
    std::optional<security::resource_type> resource_type;
    switch (request.resource_type) {
    case 1:
        // wildcard
        break;
    default:
        resource_type = to_resource_type(request.resource_type);
    }

    std::optional<security::resource_pattern_filter::pattern_filter_type>
      pattern_filter;
    switch (request.resource_pattern_type) {
    case 1:
        // wildcard
        break;
    case 2:
        // match
        pattern_filter = security::resource_pattern_filter::pattern_match{};
        break;
    default:
        pattern_filter = to_pattern_type(request.resource_pattern_type);
    }

    return security::resource_pattern_filter(
      resource_type, request.resource_name_filter, pattern_filter);
}

/*
 * build acl entry filter bits
 */
inline security::acl_entry_filter
to_acl_entry_filter(const describe_acls_request_data& request) {
    std::optional<security::acl_principal> principal;
    if (request.principal_filter) {
        principal = to_acl_principal(*request.principal_filter);
    }

    std::optional<security::acl_host> host;
    if (request.host_filter) {
        host = to_acl_host(*request.host_filter);
    }

    std::optional<security::acl_operation> operation;
    switch (request.operation) {
    case 1:
        // wildcard
        break;
    default:
        operation = to_acl_operation(request.operation);
    }

    std::optional<security::acl_permission> permission;
    switch (request.permission_type) {
    case 1:
        // wildcard
        break;
    default:
        permission = to_acl_permission(request.permission_type);
    }

    return security::acl_entry_filter(
      std::move(principal), host, operation, permission);
}

/*
 * convert kafka describe acl request into an internal acl filter
 */
inline security::acl_binding_filter
to_acl_binding_filter(const describe_acls_request_data& request) {
    return security::acl_binding_filter(
      to_resource_pattern_filter(request), to_acl_entry_filter(request));
}

inline int8_t to_kafka_resource_type(security::resource_type type) {
    switch (type) {
    case security::resource_type::topic:
        return 2;
    case security::resource_type::group:
        return 3;
    case security::resource_type::cluster:
        return 4;
    case security::resource_type::transactional_id:
        return 5;
    }
    __builtin_unreachable();
}

inline int8_t to_kafka_pattern_type(security::pattern_type type) {
    switch (type) {
    case security::pattern_type::literal:
        return 3;
    case security::pattern_type::prefixed:
        return 4;
    }
    __builtin_unreachable();
}

inline int8_t to_kafka_operation(security::acl_operation op) {
    switch (op) {
    case security::acl_operation::all:
        return 2;
    case security::acl_operation::read:
        return 3;
    case security::acl_operation::write:
        return 4;
    case security::acl_operation::create:
        return 5;
    case security::acl_operation::remove:
        return 6;
    case security::acl_operation::alter:
        return 7;
    case security::acl_operation::describe:
        return 8;
    case security::acl_operation::cluster_action:
        return 9;
    case security::acl_operation::describe_configs:
        return 10;
    case security::acl_operation::alter_configs:
        return 11;
    case security::acl_operation::idempotent_write:
        return 12;
    }
    __builtin_unreachable();
}

inline int8_t to_kafka_permission(security::acl_permission perm) {
    switch (perm) {
    case security::acl_permission::deny:
        return 2;
    case security::acl_permission::allow:
        return 3;
    }
    __builtin_unreachable();
}

inline ss::sstring to_kafka_principal(const security::acl_principal& p) {
    switch (p.type()) {
    case security::principal_type::user:
        return fmt::format("User:{}", p.name());
    case security::principal_type::ephemeral_user:
        return fmt::format("Ephemeral user:{}", p.name());
    case security::principal_type::role:
        return fmt::format("RedpandaRole:{}", p.name());
    }
    __builtin_unreachable();
}

inline ss::sstring to_kafka_host(security::acl_host host) {
    if (host.address()) {
        return fmt::format("{}", *host.address());
    } else {
        return "*";
    }
}

/**
 * In some Kafka APIs, set of allowed operations is encoded as a int32 bit
 * field. i.e. each allowed operation corresponds to `1` at position defined by
 * operation underlying value.
 *
 * f.e.
 * allowed read, write and alter would be
 *
 *  00000000 00000000 00000000 10001100
 */
inline int32_t to_bit_field(const std::vector<security::acl_operation>& ops) {
    static constexpr int available_bits = std::numeric_limits<int32_t>::digits;
    std::bitset<available_bits> bitfield;

    for (auto o : ops) {
        auto kafka_acl_operation = to_kafka_operation(o);
        vassert(
          kafka_acl_operation <= available_bits,
          "can not encode {} as a bit in {} bit integer",
          o,
          available_bits);

        bitfield.set(kafka_acl_operation);
    }
    // cast to signed, Kafka uses signed integers
    return static_cast<int32_t>(bitfield.to_ulong());
}

/**
 *  list of acl operations for specific resource
 */
template<typename T>
const std::vector<security::acl_operation>& get_allowed_operations() {
    static const std::vector<security::acl_operation> topic_resource_ops{
      security::acl_operation::read,
      security::acl_operation::write,
      security::acl_operation::create,
      security::acl_operation::describe,
      security::acl_operation::remove,
      security::acl_operation::alter,
      security::acl_operation::describe_configs,
      security::acl_operation::alter_configs,
    };

    static const std::vector<security::acl_operation> group_resource_ops{
      security::acl_operation::read,
      security::acl_operation::describe,
      security::acl_operation::remove,
    };

    static const std::vector<security::acl_operation>
      transactional_id_resource_ops{
        security::acl_operation::write,
        security::acl_operation::describe,
      };

    static const std::vector<security::acl_operation> cluster_resource_ops{
      security::acl_operation::create,
      security::acl_operation::cluster_action,
      security::acl_operation::describe_configs,
      security::acl_operation::alter_configs,
      security::acl_operation::idempotent_write,
      security::acl_operation::alter,
      security::acl_operation::describe,
    };

    auto resource_type = security::get_resource_type<T>();

    switch (resource_type) {
    case security::resource_type::cluster:
        return cluster_resource_ops;
    case security::resource_type::group:
        return group_resource_ops;
    case security::resource_type::topic:
        return topic_resource_ops;
    case security::resource_type::transactional_id:
        return transactional_id_resource_ops;
    };

    __builtin_unreachable();
}

template<typename T>
std::vector<security::acl_operation>
authorized_operations(request_context& ctx, const T& resource) {
    std::vector<security::acl_operation> allowed_operations;
    auto& ops = get_allowed_operations<T>();

    std::copy_if(
      ops.begin(),
      ops.end(),
      std::back_inserter(allowed_operations),
      [&ctx, &resource](security::acl_operation op) {
          return ctx.authorized(
            op, resource, authz_quiet::no, audit_authz_check::no);
      });

    return allowed_operations;
}

/*
 * build resource pattern filter bits from delete acl request filter
 */
inline security::resource_pattern_filter
to_resource_pattern_filter(const delete_acls_filter& filter) {
    std::optional<security::resource_type> resource_type;
    switch (filter.resource_type_filter) {
    case 1:
        // wildcard
        break;
    default:
        resource_type = to_resource_type(filter.resource_type_filter);
    }

    std::optional<security::resource_pattern_filter::pattern_filter_type>
      pattern_filter;
    switch (filter.pattern_type_filter) {
    case 1:
        // wildcard
        break;
    case 2:
        // match
        pattern_filter = security::resource_pattern_filter::pattern_match{};
        break;
    default:
        pattern_filter = to_pattern_type(filter.pattern_type_filter);
    }

    return security::resource_pattern_filter(
      resource_type, filter.resource_name_filter, pattern_filter);
}

/*
 * build acl entry filter bits from delete acls request filter
 */
inline security::acl_entry_filter
to_acl_entry_filter(const delete_acls_filter& filter) {
    std::optional<security::acl_principal> principal;
    if (filter.principal_filter) {
        principal = to_acl_principal(*filter.principal_filter);
    }

    std::optional<security::acl_host> host;
    if (filter.host_filter) {
        host = to_acl_host(*filter.host_filter);
    }

    std::optional<security::acl_operation> operation;
    switch (filter.operation) {
    case 1:
        // wildcard
        break;
    default:
        operation = to_acl_operation(filter.operation);
    }

    std::optional<security::acl_permission> permission;
    switch (filter.permission_type) {
    case 1:
        // wildcard
        break;
    default:
        permission = to_acl_permission(filter.permission_type);
    }

    return security::acl_entry_filter(
      std::move(principal), host, operation, permission);
}

/*
 * convert kafka describe acl request into an internal acl filter
 */
inline security::acl_binding_filter
to_acl_binding_filter(const delete_acls_filter& filter) {
    return security::acl_binding_filter(
      to_resource_pattern_filter(filter), to_acl_entry_filter(filter));
}

} // namespace kafka::details
