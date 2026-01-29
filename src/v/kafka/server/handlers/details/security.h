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

#pragma once
#include "kafka/protocol/schemata/create_acls_request.h"
#include "kafka/protocol/schemata/delete_acls_request.h"
#include "kafka/protocol/schemata/describe_acls_request.h"
#include "kafka/protocol/schemata/describe_acls_response.h"
#include "kafka/protocol/types.h"
#include "model/validation.h"
#include "security/acl.h"
#include "security/scram_credential.h"
namespace kafka {

/*
 * authz failures should be quiet or logged at a reduced severity level.
 */
using authz_quiet = ss::bool_class<struct authz_quiet_tag>;

using audit_authz_check = ss::bool_class<struct audit_authz_check_tag>;

using superuser_required = ss::bool_class<struct superuser_required_tag>;

namespace details {

constexpr auto sr_subject_wire_value = 100;
constexpr auto sr_registry_wire_value = 101;

using acl_conversion_error = security::acl_conversion_error;

inline security::acl_principal to_acl_principal(const ss::sstring& principal) {
    return security::acl_principal::from_string(principal);
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

inline security::resource_type to_registry_resource_type(int8_t type) {
    switch (type) {
    case sr_subject_wire_value:
        return security::resource_type::sr_subject;
    case sr_registry_wire_value:
        return security::resource_type::sr_registry;
    default:
        throw acl_conversion_error(
          fmt::format("Invalid registry resource type: {}", type));
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
            throw acl_conversion_error(
              fmt::format(
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
        resource_type = request.describe_registry_acls
                          ? to_registry_resource_type(request.resource_type)
                          : to_resource_type(request.resource_type);
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
      resource_type,
      request.resource_name_filter,
      pattern_filter,
      request.describe_registry_acls
        ? security::resource_pattern_filter::resource_subsystem::schema_registry
        : security::resource_pattern_filter::resource_subsystem::kafka);
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
    case security::resource_type::sr_subject:
    case security::resource_type::sr_registry:
        vassert(
          false, "Schema Registry resources are not supported in kafka ACLs");
    }
}

inline int8_t to_kafka_registry_resource_type(security::resource_type type) {
    switch (type) {
    case security::resource_type::sr_subject:
        return sr_subject_wire_value;
    case security::resource_type::sr_registry:
        return sr_registry_wire_value;
    case security::resource_type::topic:
    case security::resource_type::group:
    case security::resource_type::cluster:
    case security::resource_type::transactional_id:
        vunreachable("Request only for Schema Registry resources");
    }
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
    return fmt::format("{:a}", p);
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

template<typename T>
using authorized_function = std::function<bool(
  security::acl_operation, const T&, authz_quiet, audit_authz_check)>;

template<typename T>
std::vector<security::acl_operation>
authorized_operations(authorized_function<T> fn, const T& resource) {
    std::vector<security::acl_operation> allowed_operations;
    auto& ops = security::get_allowed_operations<T>();

    std::copy_if(
      ops.begin(),
      ops.end(),
      std::back_inserter(allowed_operations),
      [&fn, &resource](security::acl_operation op) {
          return fn(op, resource, authz_quiet::yes, audit_authz_check::no);
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

std::optional<security::scram_algorithm_t>
kafka_to_security_mechanism(kafka::scram_mechanism mechanism);

scram_mechanism key_size_to_mechanism(size_t key_size);

describe_acls_resource acl_entry_to_resource(
  security::resource_pattern pattern,
  chunked_vector<security::acl_entry> acl_entries,
  bool describe_registry_resource);

} // namespace details
} // namespace kafka
