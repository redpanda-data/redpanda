/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/errc.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/types.h"
#include "json/document.h"
#include "json/json.h"
#include "model/fundamental.h"
#include "net/unresolved_address.h"
#include "security/acl.h"
#include "utils/base64.h"

#include <seastar/net/inet_address.hh>

#include <absl/container/node_hash_map.h>

#include <type_traits>

namespace json {

inline char const* to_str(rapidjson::Type const t) {
    static char const* str[] = {
      "Null", "False", "True", "Object", "Array", "String", "Number"};
    return str[t];
}

inline void read_value(json::Value const& v, int64_t& target) {
    target = v.GetInt64();
}

inline void read_value(json::Value const& v, uint64_t& target) {
    target = v.GetUint64();
}

inline void read_value(json::Value const& v, uint32_t& target) {
    target = v.GetUint();
}

inline void read_value(json::Value const& v, int32_t& target) {
    target = v.GetInt();
}

inline void read_value(json::Value const& v, int16_t& target) {
    target = v.GetInt();
}

inline void read_value(json::Value const& v, uint16_t& target) {
    target = v.GetUint();
}

inline void read_value(json::Value const& v, int8_t& target) {
    target = v.GetInt();
}

inline void read_value(json::Value const& v, uint8_t& target) {
    target = v.GetUint();
}

inline void read_value(json::Value const& v, bool& target) {
    target = v.GetBool();
}

inline void read_value(json::Value const& v, ss::sstring& target) {
    target = v.GetString();
}

inline void read_value(json::Value const& v, iobuf& target) {
    target = bytes_to_iobuf(base64_to_bytes(v.GetString()));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::chrono::nanoseconds& v) {
    rjson_serialize(w, v.count());
}

inline void read_value(json::Value const& v, std::chrono::nanoseconds& target) {
    target = std::chrono::nanoseconds(v.GetInt64());
}

template<typename T, typename Tag, typename IsConstexpr>
void read_value(
  json::Value const& v, detail::base_named_type<T, Tag, IsConstexpr>& target) {
    auto t = T{};
    read_value(v, t);
    target = detail::base_named_type<T, Tag, IsConstexpr>{t};
}

inline void
read_value(json::Value const& v, std::chrono::milliseconds& target) {
    target = std::chrono::milliseconds(v.GetUint64());
}

template<typename T>
void read_value(json::Value const& v, std::vector<T>& target) {
    for (auto const& e : v.GetArray()) {
        auto t = T{};
        read_value(e, t);
        target.push_back(t);
    }
}

template<typename T>
void read_value(json::Value const& v, std::optional<T>& target) {
    if (v.IsNull()) {
        target = std::nullopt;
    } else {
        auto t = T{};
        read_value(v, t);
        target = t;
    }
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const iobuf& buf) {
    w.String(bytes_to_base64(iobuf_to_bytes(buf)));
}

template<typename Writer, typename T>
void write_member(Writer& w, char const* key, T const& value) {
    w.String(key);
    rjson_serialize(w, value);
}

// Reads the enum's underlying type directly.
template<typename Enum>
inline auto read_enum_ut(json::Value const& v, char const* key, Enum)
  -> std::underlying_type_t<Enum> {
    std::underlying_type_t<Enum> value;
    read_member(v, key, value);
    return value;
}

template<typename Enum>
requires(std::is_enum_v<Enum>) inline void read_value(
  json::Value const& v, Enum& e) {
    std::underlying_type_t<Enum> value;
    read_value(v, value);
    e = Enum(value);
}

template<typename Enum>
requires(std::is_enum_v<Enum>) inline void read_member(
  json::Value const& v, char const* key, Enum& e) {
    std::underlying_type_t<Enum> value;
    read_member(v, key, value);
    e = Enum(value);
}

template<typename T>
requires(!std::is_enum_v<T>) void read_member(
  json::Value const& v, char const* key, T& target) {
    auto const it = v.FindMember(key);
    if (it != v.MemberEnd()) {
        read_value(it->value, target);
    } else {
        target = {};
        std::cout << "key " << key << " not found, default initializing"
                  << std::endl;
    }
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const model::ntp& ntp) {
    w.StartObject();
    w.Key("ns");
    w.String(ntp.ns());
    w.Key("topic");
    w.String(ntp.tp.topic());
    w.Key("partition");
    w.Int(ntp.tp.partition());
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::ntp& obj) {
    read_member(rd, "ns", obj.ns);
    read_member(rd, "topic", obj.tp.topic);
    read_member(rd, "partition", obj.tp.partition);
}

template<typename T>
void read_value(json::Value const& v, ss::bool_class<T>& target) {
    target = ss::bool_class<T>(v.GetBool());
}

template<typename T, typename V>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::unordered_map<T, V>& m) {
    w.StartArray();
    for (const auto& e : m) {
        w.StartObject();
        w.Key("key");
        rjson_serialize(w, e.first);
        w.Key("value");
        rjson_serialize(w, e.second);
        w.EndObject();
    }
    w.EndArray();
}

template<typename T, typename V>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const absl::node_hash_map<T, V>& m) {
    w.StartArray();
    for (const auto& e : m) {
        w.StartObject();
        w.Key("key");
        rjson_serialize(w, e.first);
        w.Key("value");
        rjson_serialize(w, e.second);
        w.EndObject();
    }
    w.EndArray();
}

template<typename V>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const absl::node_hash_set<V>& m) {
    w.StartArray();
    for (const V& e : m) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

template<typename T, typename V>
inline void read_value(json::Value const& rd, std::unordered_map<T, V>& obj) {
    for (const auto& e : rd.GetArray()) {
        T key;
        read_member(e, "key", key);
        V value;
        read_member(e, "value", value);
        obj.emplace(std::move(key), std::move(value));
    }
}

template<typename T, typename V>
inline void read_value(json::Value const& rd, absl::node_hash_map<T, V>& obj) {
    for (const auto& e : rd.GetArray()) {
        T key;
        read_member(e, "key", key);
        V value;
        read_member(e, "value", value);
        obj.emplace(std::move(key), std::move(value));
    }
}

template<typename V>
inline void read_value(json::Value const& rd, absl::node_hash_set<V>& obj) {
    for (const auto& e : rd.GetArray()) {
        auto v = V{};
        read_value(e, v);
        obj.insert(v);
    }
}

template<typename T>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const ss::bool_class<T>& b) {
    rjson_serialize(w, bool(b));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::broker_properties& b) {
    w.StartObject();
    w.Key("cores");
    rjson_serialize(w, b.cores);
    w.Key("available_memory_gb");
    rjson_serialize(w, b.available_memory_gb);
    w.Key("available_disk_gb");
    rjson_serialize(w, b.available_disk_gb);
    w.Key("mount_paths");
    rjson_serialize(w, b.mount_paths);
    w.Key("etc_props");
    rjson_serialize(w, b.etc_props);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::broker_properties& obj) {
    read_member(rd, "cores", obj.cores);
    read_member(rd, "available_memory_gb", obj.available_memory_gb);
    read_member(rd, "available_disk_gb", obj.available_disk_gb);
    read_member(rd, "mount_paths", obj.mount_paths);
    read_member(rd, "etc_props", obj.etc_props);
}

inline void
read_value(json::Value const& rd, ss::net::inet_address::family& obj) {
    obj = static_cast<ss::net::inet_address::family>(rd.GetInt());
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const ss::net::inet_address::family& b) {
    w.Int(static_cast<int>(b));
}

inline void read_value(json::Value const& rd, net::unresolved_address& obj) {
    ss::sstring host;
    uint16_t port{0};
    read_member(rd, "address", host);
    read_member(rd, "port", port);
    obj = net::unresolved_address(std::move(host), port);
}

inline void read_value(json::Value const& rd, model::broker_endpoint& obj) {
    ss::sstring host;
    uint16_t port{0};

    read_member(rd, "name", obj.name);
    read_member(rd, "address", host);
    read_member(rd, "port", port);

    obj.address = net::unresolved_address(std::move(host), port);
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const model::broker& b) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, b.id());
    w.Key("kafka_advertised_listeners");
    rjson_serialize(w, b.kafka_advertised_listeners());
    w.Key("rpc_address");
    rjson_serialize(w, b.rpc_address());
    w.Key("rack");
    rjson_serialize(w, b.rack());
    w.Key("properties");
    rjson_serialize(w, b.properties());
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::broker& obj) {
    model::node_id id;
    std::vector<model::broker_endpoint> kafka_advertised_listeners;
    net::unresolved_address rpc_address;
    std::optional<model::rack_id> rack;
    model::broker_properties properties;

    read_member(rd, "id", id);
    read_member(rd, "kafka_advertised_listeners", kafka_advertised_listeners);
    read_member(rd, "rpc_address", rpc_address);
    read_member(rd, "rack", rack);
    read_member(rd, "properties", properties);

    obj = model::broker(
      id,
      std::move(kafka_advertised_listeners),
      std::move(rpc_address),
      std::move(rack),
      std::move(properties));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::topic_namespace& t) {
    w.StartObject();
    w.Key("ns");
    rjson_serialize(w, t.ns);
    w.Key("tp");
    rjson_serialize(w, t.tp);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::topic_namespace& obj) {
    model::ns ns;
    model::topic tp;
    read_member(rd, "ns", ns);
    read_member(rd, "tp", tp);
    obj = model::topic_namespace(std::move(ns), std::move(tp));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::partition_balancer_violations::unavailable_node& un) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, un.id);
    w.Key("unavailable_since");
    rjson_serialize(w, un.unavailable_since.value());
    w.EndObject();
}

inline void read_value(
  json::Value const& rd,
  cluster::partition_balancer_violations::unavailable_node& un) {
    read_member(rd, "id", un.id);
    read_member(rd, "unavailable_since", un.unavailable_since);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::partition_balancer_violations::full_node& fn) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, fn.id);
    w.Key("disk_used_percent");
    rjson_serialize(w, fn.disk_used_percent);
    w.EndObject();
}

inline void read_value(
  json::Value const& rd,
  cluster::partition_balancer_violations::full_node& fn) {
    read_member(rd, "id", fn.id);
    read_member(rd, "disk_used_percent", fn.disk_used_percent);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::partition_balancer_violations& violations) {
    w.StartObject();
    w.Key("unavailable_nodes");
    rjson_serialize(w, violations.unavailable_nodes);
    w.Key("full_nodes");
    rjson_serialize(w, violations.full_nodes);
    w.EndObject();
}

inline void read_value(
  json::Value const& rd, cluster::partition_balancer_violations& violations) {
    read_member(rd, "unavailable_nodes", violations.unavailable_nodes);
    read_member(rd, "full_nodes", violations.full_nodes);
}

inline void read_value(json::Value const& rd, security::acl_host& host) {
    ss::sstring address;
    read_member(rd, "address", address);
    host = security::acl_host(address);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const security::acl_host& host) {
    w.StartObject();
    std::stringstream ss;
    vassert(host.address(), "Unset optional address unexpected.");
    ss << host.address().value();
    if (!ss.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "failed to serialize acl_host, state: {}",
          ss.rdstate()));
    }
    w.Key("address");
    rjson_serialize(w, ss.str());
    w.EndObject();
}

inline void
read_value(json::Value const& rd, security::acl_principal& principal) {
    security::principal_type type{};
    read_member(rd, "type", type);
    ss::sstring name;
    read_member(rd, "name", name);
    principal = security::acl_principal(type, std::move(name));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const security::acl_principal& principal) {
    w.StartObject();
    w.Key("type");
    rjson_serialize(w, principal.type());
    w.Key("name");
    rjson_serialize(w, principal.name());
    w.EndObject();
}

inline void read_value(json::Value const& rd, security::acl_entry& entry) {
    security::acl_principal principal;
    security::acl_host host;
    security::acl_operation operation{};
    security::acl_permission permission{};
    read_member(rd, "principal", principal);
    read_member(rd, "host", host);
    read_member(rd, "operation", operation);
    read_member(rd, "permission", permission);
    entry = security::acl_entry(
      std::move(principal), host, operation, permission);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const security::acl_entry& entry) {
    w.StartObject();
    w.Key("principal");
    rjson_serialize(w, entry.principal());
    w.Key("host");
    rjson_serialize(w, entry.host());
    w.Key("operation");
    rjson_serialize(w, entry.operation());
    w.Key("permission");
    rjson_serialize(w, entry.permission());
    w.EndObject();
}

inline void
read_value(json::Value const& rd, security::resource_pattern& pattern) {
    ss::sstring name;
    security::resource_type resource{};
    security::pattern_type pattern_type{};
    read_member(rd, "resource", resource);
    read_member(rd, "name", name);
    read_member(rd, "pattern", pattern_type);
    pattern = security::resource_pattern(
      resource, std::move(name), pattern_type);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const security::resource_pattern& pattern) {
    w.StartObject();
    w.Key("resource");
    rjson_serialize(w, pattern.resource());
    w.Key("name");
    rjson_serialize(w, pattern.name());
    w.Key("pattern");
    rjson_serialize(w, pattern.pattern());
    w.EndObject();
}

inline void read_value(json::Value const& rd, security::acl_binding& binding) {
    security::resource_pattern pattern;
    security::acl_entry entry;
    read_member(rd, "pattern", pattern);
    read_member(rd, "entry", entry);
    binding = security::acl_binding(std::move(pattern), std::move(entry));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const security::acl_binding& data) {
    w.StartObject();
    w.Key("pattern");
    rjson_serialize(w, data.pattern());
    w.Key("entry");
    rjson_serialize(w, data.entry());
    w.EndObject();
}

inline void
read_value(json::Value const& rd, cluster::create_acls_cmd_data& data) {
    read_member(rd, "bindings", data.bindings);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::create_acls_cmd_data& data) {
    w.StartObject();
    w.Key("bindings");
    rjson_serialize(w, data.bindings);
    w.EndObject();
}

inline void
read_value(json::Value const& rd, cluster::delete_acls_result& data) {
    read_member(rd, "error", data.error);
    read_member(rd, "bindings", data.bindings);
}

inline void read_value(
  json::Value const& rd,
  security::resource_pattern_filter::pattern_filter_type& pattern_filter) {
    using type = security::resource_pattern_filter::serialized_pattern_type;
    type ser_filter{};
    read_member(rd, "pattern_filter", ser_filter);
    switch (ser_filter) {
    case type::literal:
        pattern_filter = security::pattern_type::literal;
        break;
    case type::prefixed:
        pattern_filter = security::pattern_type::prefixed;
        break;
    case type::match:
        pattern_filter = security::resource_pattern_filter::pattern_match{};
        break;
    default:
        vassert(
          false, "unexpected serialized pattern filter type {}", ser_filter);
    }
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const security::resource_pattern_filter::pattern_filter_type&
    pattern_filter) {
    security::resource_pattern_filter::serialized_pattern_type out{};
    if (std::holds_alternative<
          security::resource_pattern_filter::pattern_match>(pattern_filter)) {
        out = security::resource_pattern_filter::serialized_pattern_type::match;
    } else {
        auto source_pattern = std::get<security::pattern_type>(pattern_filter);
        out = security::resource_pattern_filter::to_pattern(source_pattern);
    }
    w.StartObject();
    w.Key("pattern_filter");
    rjson_serialize(w, out);
    w.EndObject();
}

inline void
read_value(json::Value const& rd, security::resource_pattern_filter& pattern) {
    std::optional<security::resource_type> resource;
    std::optional<ss::sstring> name;
    std::optional<security::resource_pattern_filter::pattern_filter_type>
      pattern_filter_type;
    read_member(rd, "name", name);
    read_member(rd, "resource", resource);
    read_member(rd, "pattern", pattern_filter_type);
    pattern = security::resource_pattern_filter(
      resource, std::move(name), pattern_filter_type);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const security::resource_pattern_filter& pattern) {
    w.StartObject();
    w.Key("resource");
    rjson_serialize(w, pattern.resource());
    w.Key("name");
    rjson_serialize(w, pattern.name());
    w.Key("pattern");
    rjson_serialize(w, pattern.pattern());
    w.EndObject();
}

inline void
read_value(json::Value const& rd, security::acl_entry_filter& filter) {
    std::optional<security::acl_principal> principal;
    std::optional<security::acl_host> host;
    std::optional<security::acl_operation> operation;
    std::optional<security::acl_permission> permission;
    read_member(rd, "principal", principal);
    read_member(rd, "host", host);
    read_member(rd, "operation", operation);
    read_member(rd, "permission", permission);
    filter = security::acl_entry_filter(
      std::move(principal), host, operation, permission);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const security::acl_entry_filter& filter) {
    w.StartObject();
    w.Key("principal");
    rjson_serialize(w, filter.principal());
    w.Key("host");
    rjson_serialize(w, filter.host());
    w.Key("operation");
    rjson_serialize(w, filter.operation());
    w.Key("permission");
    rjson_serialize(w, filter.permission());
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::delete_acls_result& data) {
    w.StartObject();
    w.Key("error");
    rjson_serialize(w, data.error);
    w.Key("bindings");
    rjson_serialize(w, data.bindings);
    w.EndObject();
}

inline void
read_value(json::Value const& rd, security::acl_binding_filter& binding) {
    security::resource_pattern_filter pattern_filter;
    security::acl_entry_filter entry_filter;
    read_member(rd, "pattern", pattern_filter);
    read_member(rd, "entry", entry_filter);
    binding = security::acl_binding_filter(pattern_filter, entry_filter);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const security::acl_binding_filter& data) {
    w.StartObject();
    w.Key("pattern");
    rjson_serialize(w, data.pattern());
    w.Key("entry");
    rjson_serialize(w, data.entry());
    w.EndObject();
}

inline void
read_value(json::Value const& rd, cluster::delete_acls_cmd_data& data) {
    read_member(rd, "filters", data.filters);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::delete_acls_cmd_data& data) {
    w.StartObject();
    w.Key("filters");
    rjson_serialize(w, data.filters);
    w.EndObject();
}

enum class tristate_status : uint8_t { disabled = 0, not_set, set };

template<typename T>
void read_value(json::Value const& rd, tristate<T>& target) {
    tristate_status ts{tristate_status::disabled};
    auto ts_val = read_enum_ut(rd, "status", ts);
    switch (ts_val) {
    case 0:
        target = tristate<T>();
        break;
    case 1:
        target = tristate<T>(std::nullopt);
        break;
    case 2:
        T value;
        read_member(rd, "value", value);
        target = tristate<T>(std::optional<T>(std::move(value)));
        break;
    default:
        vassert(false, "Unknown enum value for tristate_status: {}", ts_val);
    }
}

template<typename T>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const tristate<T>& t) {
    w.StartObject();
    w.Key("status");
    if (t.is_disabled()) {
        w.Int(uint8_t(tristate_status::disabled));
    } else if (!t.has_optional_value()) {
        w.Int(uint8_t(tristate_status::not_set));
    } else {
        w.Int(uint8_t(tristate_status::set));
        w.Key("value");
        rjson_serialize(w, t.value());
    }
    w.EndObject();
}

#define json_write(_fname) json::write_member(wr, #_fname, obj._fname)
#define json_read(_fname) json::read_member(rd, #_fname, obj._fname)

} // namespace json
