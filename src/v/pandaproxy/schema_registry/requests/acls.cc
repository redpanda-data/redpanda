/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "pandaproxy/schema_registry/requests/acls.h"

#include "absl/strings/ascii.h"
#include "json/types.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/types.h"
#include "security/acl.h"
#include "strings/utf8.h"

#include <algorithm>

namespace pandaproxy::schema_registry {

struct acl_control_character_thrower {
    explicit acl_control_character_thrower(std::string_view field)
      : field_name(field) {}
    [[noreturn]] void conversion_error() const {
        throw exception(
          error_code::acl_invalid,
          fmt::format("Control characters not allowed in '{}'", field_name));
    }
    std::string_view field_name;
};

namespace {

template<typename T>
T parse_security_enum(std::string_view value, std::string_view type_name) {
    auto result = security::from_string_view<T>(absl::AsciiStrToLower(value));
    if (!result) {
        throw exception(
          error_code::acl_invalid,
          fmt::format("Invalid {}: {}", type_name, value));
    }
    return *result;
}

} // namespace

security::acl_operation to_acl_operation(std::string_view op) {
    auto res = parse_security_enum<security::acl_operation>(
      op, "ACL operation");

    static const auto valid_ops = [] {
        auto ops = std::vector<security::acl_operation>{};
        ops.push_back(security::acl_operation::all);
        ops.append_range(security::get_allowed_operations<context_subject>());
        ops.append_range(security::get_allowed_operations<registry_resource>());
        std::ranges::sort(ops);
        ops.erase(std::ranges::unique(ops).begin(), ops.end());
        return ops;
    }();
    if (!std::ranges::contains(valid_ops, res)) {
        throw exception(
          error_code::acl_invalid,
          fmt::format("Invalid acl operation: {}", op));
    }
    return res;
}

security::resource_type to_resource_type(std::string_view type) {
    auto res = parse_security_enum<security::resource_type>(
      type, "resource type");
    if (
      res != security::resource_type::sr_subject
      && res != security::resource_type::sr_registry) {
        throw exception(
          error_code::acl_invalid,
          fmt::format("Invalid resource type: {}", res));
    }
    return res;
}

security::pattern_type to_pattern_type(std::string_view type) {
    return parse_security_enum<security::pattern_type>(type, "pattern type");
}

security::acl_permission to_acl_permission(std::string_view perm) {
    return parse_security_enum<security::acl_permission>(
      perm, "ACL permission");
}

security::acl_host to_acl_host(std::string_view host) {
    if (host == "*") {
        return security::acl_host::wildcard_host();
    }
    try {
        return security::acl_host(ss::sstring{host});
    } catch (const std::invalid_argument& e) {
        throw exception(
          error_code::acl_invalid, fmt::format("Invalid ACL host: {}", host));
    }
}

security::acl_principal to_acl_principal(std::string_view principal) {
    try {
        return security::acl_principal::from_string(principal);
    } catch (const security::acl_conversion_error& e) {
        throw exception(error_code::acl_invalid, e.msg);
    }
}

acl::acl(const security::acl_binding& binding) {
    const auto& entry = binding.entry();
    const auto& pattern = binding.pattern();

    principal = entry.principal();
    resource = pattern.name();
    resource_type = pattern.resource();
    pattern_type = pattern.pattern();
    host = entry.host();
    operation = entry.operation();
    permission = entry.permission();
}

template<typename Encoding>
void acl_handler<Encoding>::validate_current_field(std::string_view sv) {
    std::string_view field_name = "[not-set]";
    switch (_state) {
    case state::principal:
        field_name = "principal";
        break;
    case state::resource:
        field_name = "resource";
        break;
    case state::resource_type:
        field_name = "resource_type";
        break;
    case state::pattern_type:
        field_name = "pattern_type";
        break;
    case state::host:
        field_name = "host";
        break;
    case state::operation:
        field_name = "operation";
        break;
    case state::permission:
        field_name = "permission";
        break;
    case state::empty:
    case state::array:
    case state::object:
        return;
    }
    validate_no_control(sv, acl_control_character_thrower(field_name));
}

template<typename Encoding>
bool acl_handler<Encoding>::Key(const Ch* str, ::json::SizeType len, bool) {
    auto sv = std::string_view{str, len};
    if (_state == state::object) {
        if (sv == "principal") {
            _state = state::principal;
            return true;
        } else if (sv == "resource") {
            _state = state::resource;
            return true;
        } else if (sv == "resource_type") {
            _state = state::resource_type;
            return true;
        } else if (sv == "pattern_type") {
            _state = state::pattern_type;
            return true;
        } else if (sv == "host") {
            _state = state::host;
            return true;
        } else if (sv == "operation") {
            _state = state::operation;
            return true;
        } else if (sv == "permission") {
            _state = state::permission;
            return true;
        }
    }
    return false;
}

template<typename Encoding>
bool acl_handler<Encoding>::String(const Ch* str, ::json::SizeType len, bool) {
    std::string_view sv{str, len};

    // Validate for control characters
    validate_current_field(sv);

    switch (_state) {
    case state::principal:
        _current_acl.principal = to_acl_principal(sv);
        _state = state::object;
        return true;
    case state::resource:
        _current_acl.resource = ss::sstring{sv};
        _state = state::object;
        return true;
    case state::resource_type:
        _current_acl.resource_type = to_resource_type(sv);
        _state = state::object;
        return true;
    case state::pattern_type:
        _current_acl.pattern_type = to_pattern_type(sv);
        _state = state::object;
        return true;
    case state::host:
        _current_acl.host = to_acl_host(sv);
        _state = state::object;
        return true;
    case state::operation:
        _current_acl.operation = to_acl_operation(sv);
        _state = state::object;
        return true;
    case state::permission:
        _current_acl.permission = to_acl_permission(sv);
        _state = state::object;
        return true;
    case state::empty:
    case state::array:
    case state::object:
        return false;
    }
}

template<typename Encoding>
bool acl_handler<Encoding>::StartArray() {
    if (_state == state::empty) {
        _state = state::array;
        return true;
    }
    return false;
}

template<typename Encoding>
bool acl_handler<Encoding>::EndArray(::json::SizeType) {
    if (_state == state::array) {
        _state = state::empty;
        return true;
    }
    return false;
}

template<typename Encoding>
bool acl_handler<Encoding>::StartObject() {
    if (_state == state::array) {
        _state = state::object;
        _current_acl = acl{};
        return true;
    }
    return false;
}

namespace {
void validate_missing_required_fields(const acl& acl) {
    auto require = [](auto& field, std::string_view field_name) {
        if (!field) {
            throw exception(
              error_code::acl_invalid,
              fmt::format("Invalid ACL principal: {}", field_name));
        }
    };

    require(acl.principal, "principal");
    require(acl.resource, "resource");
    require(acl.resource_type, "resource_type");
    require(acl.pattern_type, "pattern_type");
    require(acl.host, "host");
    require(acl.operation, "operation");
    require(acl.permission, "permission");
}
} // namespace

template<typename Encoding>
bool acl_handler<Encoding>::EndObject(::json::SizeType) {
    if (_state == state::object) {
        if (_require_fields) {
            validate_missing_required_fields(_current_acl);
        }
        result.emplace_back(std::move(_current_acl));
        _state = state::array;
        return true;
    }
    return false;
}

template class acl_handler<rapidjson::UTF8<char>>;

} // namespace pandaproxy::schema_registry
