/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/strings/ascii.h"
#include "container/chunked_vector.h"
#include "json/types.h"
#include "json/writer.h"
#include "pandaproxy/json/rjson_parse.h"
#include "security/acl.h"

namespace pandaproxy::schema_registry {

security::acl_operation to_acl_operation(std::string_view op);
security::resource_type to_resource_type(std::string_view type);
security::pattern_type to_pattern_type(std::string_view type);
security::acl_permission to_acl_permission(std::string_view perm);
security::acl_host to_acl_host(std::string_view host);
security::acl_principal to_acl_principal(std::string_view principal);

struct acl {
    std::optional<security::acl_principal> principal;
    std::optional<ss::sstring> resource;
    std::optional<security::resource_type> resource_type;
    std::optional<security::pattern_type> pattern_type;
    std::optional<security::acl_host> host;
    std::optional<security::acl_operation> operation;
    std::optional<security::acl_permission> permission;

    acl() noexcept = default;

    explicit acl(const security::acl_binding& binding);
};

template<typename Buffer>
void rjson_serialize(::json::Writer<Buffer>& w, const acl& entry) {
    w.StartObject();

    if (entry.principal) {
        w.Key("principal");
        w.String(fmt::format("{:a}", *entry.principal));
    }

    if (entry.resource) {
        w.Key("resource");
        w.String(*entry.resource);
    }

    if (entry.resource_type) {
        w.Key("resource_type");
        w.String(
          absl::AsciiStrToUpper(fmt::format("{}", *entry.resource_type)));
    }

    if (entry.pattern_type) {
        w.Key("pattern_type");
        w.String(absl::AsciiStrToUpper(fmt::format("{}", *entry.pattern_type)));
    }

    if (entry.host) {
        w.Key("host");
        if (entry.host->address()) {
            w.String(fmt::format("{}", *entry.host->address()));
        } else {
            w.String("*");
        }
    }

    if (entry.operation) {
        w.Key("operation");
        w.String(absl::AsciiStrToUpper(fmt::format("{}", *entry.operation)));
    }

    if (entry.permission) {
        w.Key("permission");
        w.String(absl::AsciiStrToUpper(fmt::format("{}", *entry.permission)));
    }

    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class acl_handler : public json::base_handler<Encoding> {
public:
    using require_fields = ss::bool_class<struct require_fields_tag>;
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = chunked_vector<acl>;

    explicit acl_handler(require_fields require_fields)
      : json::base_handler<Encoding>{json::serialization_format::none}
      , result()
      , _require_fields(require_fields) {}

    bool Key(const Ch* str, ::json::SizeType len, bool);
    bool String(const Ch* str, ::json::SizeType len, bool);
    bool StartArray();
    bool EndArray(::json::SizeType);
    bool StartObject();
    bool EndObject(::json::SizeType);
    bool Null() { return false; }

    rjson_parse_result result;

private:
    enum class state {
        empty = 0,
        array,
        object,
        principal,
        resource,
        resource_type,
        pattern_type,
        host,
        operation,
        permission,
    };

    void validate_current_field(std::string_view sv);

    state _state = state::empty;
    acl _current_acl;
    require_fields _require_fields;
};

} // namespace pandaproxy::schema_registry
