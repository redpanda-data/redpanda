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
#include "container/json.h"
#include "json/document.h"
#include "json/json.h"
#include "model/fundamental.h"
#include "security/acl.h"
#include "utils/base64.h"
#include "utils/unresolved_address.h"

#include <seastar/net/inet_address.hh>

#include <absl/container/node_hash_map.h>

#include <type_traits>

namespace json {

inline const char* to_str(const rapidjson::Type t) {
    static const char* str[] = {
      "Null", "False", "True", "Object", "Array", "String", "Number"};
    return str[t];
}

inline void read_value(const json::Value& v, int64_t& target) {
    target = v.GetInt64();
}

inline void read_value(const json::Value& v, uint64_t& target) {
    target = v.GetUint64();
}

inline void read_value(const json::Value& v, uint32_t& target) {
    target = v.GetUint();
}

inline void read_value(const json::Value& v, int32_t& target) {
    target = v.GetInt();
}

inline void read_value(const json::Value& v, int16_t& target) {
    target = v.GetInt();
}

inline void read_value(const json::Value& v, uint16_t& target) {
    target = v.GetUint();
}

inline void read_value(const json::Value& v, int8_t& target) {
    target = v.GetInt();
}

inline void read_value(const json::Value& v, uint8_t& target) {
    target = v.GetUint();
}

inline void read_value(const json::Value& v, bool& target) {
    target = v.GetBool();
}

inline void read_value(const json::Value& v, ss::sstring& target) {
    target = v.GetString();
}

inline void read_value(const json::Value& v, iobuf& target) {
    target = bytes_to_iobuf(base64_to_bytes(v.GetString()));
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const std::chrono::nanoseconds& v) {
    rjson_serialize(w, v.count());
}

inline void read_value(const json::Value& v, std::chrono::nanoseconds& target) {
    target = std::chrono::nanoseconds(v.GetInt64());
}

template<typename T, typename Tag, typename IsConstexpr>
void read_value(
  const json::Value& v, detail::base_named_type<T, Tag, IsConstexpr>& target) {
    auto t = T{};
    read_value(v, t);
    target = detail::base_named_type<T, Tag, IsConstexpr>{std::move(t)};
}

inline void
read_value(const json::Value& v, std::chrono::milliseconds& target) {
    target = std::chrono::milliseconds(v.GetUint64());
}

template<typename T>
void read_value(const json::Value& v, std::vector<T>& target) {
    for (const auto& e : v.GetArray()) {
        auto t = T{};
        read_value(e, t);
        target.push_back(std::move(t));
    }
}

template<typename T, size_t chunk_size = 128>
void read_value(const json::Value& v, ss::chunked_fifo<T, chunk_size>& target) {
    for (const auto& e : v.GetArray()) {
        auto t = T{};
        read_value(e, t);
        target.push_back(std::move(t));
    }
}

template<typename T, size_t fragment_size_bytes>
void read_value(
  const json::Value& v, fragmented_vector<T, fragment_size_bytes>& target) {
    for (const auto& e : v.GetArray()) {
        auto t = T{};
        read_value(e, t);
        target.push_back(std::move(t));
    }
}

template<typename T>
void read_value(const json::Value& v, std::optional<T>& target) {
    if (v.IsNull()) {
        target = std::nullopt;
    } else {
        auto t = T{};
        read_value(v, t);
        target = std::move(t);
    }
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const iobuf& buf) {
    w.String(bytes_to_base64(iobuf_to_bytes(buf)));
}

template<typename Writer, typename T>
void write_member(Writer& w, const char* key, const T& value) {
    w.String(key);
    rjson_serialize(w, value);
}

// Reads the enum's underlying type directly.
template<typename Enum>
inline auto read_enum_ut(const json::Value& v, const char* key, Enum)
  -> std::underlying_type_t<Enum> {
    std::underlying_type_t<Enum> value;
    read_member(v, key, value);
    return value;
}

template<typename Enum>
requires(std::is_enum_v<Enum>)
inline void read_value(const json::Value& v, Enum& e) {
    std::underlying_type_t<Enum> value;
    read_value(v, value);
    e = Enum(value);
}

template<typename Enum>
requires(std::is_enum_v<Enum>)
inline void read_member(const json::Value& v, const char* key, Enum& e) {
    std::underlying_type_t<Enum> value;
    read_member(v, key, value);
    e = Enum(value);
}

template<typename T>
requires(!std::is_enum_v<T>)
void read_member(const json::Value& v, const char* key, T& target) {
    const auto it = v.FindMember(key);
    if (it != v.MemberEnd()) {
        read_value(it->value, target);
    } else {
        target = T{};
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

inline void read_value(const json::Value& rd, model::ntp& obj) {
    read_member(rd, "ns", obj.ns);
    read_member(rd, "topic", obj.tp.topic);
    read_member(rd, "partition", obj.tp.partition);
}

template<typename T>
void read_value(const json::Value& v, ss::bool_class<T>& target) {
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

template<typename T, typename V>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const absl::flat_hash_map<T, V>& m) {
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

template<typename V>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const absl::btree_set<V>& m) {
    w.StartArray();
    for (const V& e : m) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

template<typename T, typename V>
inline void read_value(const json::Value& rd, std::unordered_map<T, V>& obj) {
    for (const auto& e : rd.GetArray()) {
        T key;
        read_member(e, "key", key);
        V value;
        read_member(e, "value", value);
        obj.emplace(std::move(key), std::move(value));
    }
}

template<typename T, typename V>
inline void read_value(const json::Value& rd, absl::node_hash_map<T, V>& obj) {
    for (const auto& e : rd.GetArray()) {
        T key;
        read_member(e, "key", key);
        V value;
        read_member(e, "value", value);
        obj.emplace(std::move(key), std::move(value));
    }
}

template<typename T, typename V>
inline void read_value(const json::Value& rd, absl::flat_hash_map<T, V>& obj) {
    for (const auto& e : rd.GetArray()) {
        T key;
        read_member(e, "key", key);
        V value;
        read_member(e, "value", value);
        obj.emplace(std::move(key), std::move(value));
    }
}

template<typename T, typename V>
inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const chunked_hash_map<T, V>& m) {
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
inline void read_value(const json::Value& rd, absl::node_hash_set<V>& obj) {
    for (const auto& e : rd.GetArray()) {
        auto v = V{};
        read_value(e, v);
        obj.insert(std::move(v));
    }
}

template<typename V>
inline void read_value(const json::Value& rd, absl::btree_set<V>& obj) {
    for (const auto& e : rd.GetArray()) {
        auto v = V{};
        read_value(e, v);
        obj.insert(std::move(v));
    }
}

template<typename T, typename V>
inline void read_value(const json::Value& rd, chunked_hash_map<T, V>& obj) {
    for (const auto& e : rd.GetArray()) {
        T key;
        read_member(e, "key", key);
        V value;
        read_member(e, "value", value);
        obj.emplace(std::move(key), std::move(value));
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
    w.Key("available_memory_bytes");
    rjson_serialize(w, b.available_memory_bytes);
    w.Key("in_fips_mode");
    rjson_serialize(w, b.in_fips_mode);
    w.EndObject();
}

inline void read_value(const json::Value& rd, model::broker_properties& obj) {
    read_member(rd, "cores", obj.cores);
    read_member(rd, "available_memory_gb", obj.available_memory_gb);
    read_member(rd, "available_disk_gb", obj.available_disk_gb);
    read_member(rd, "mount_paths", obj.mount_paths);
    read_member(rd, "etc_props", obj.etc_props);
    read_member(rd, "available_memory_bytes", obj.available_memory_bytes);
    read_member(rd, "in_fips_mode", obj.in_fips_mode);
}

inline void
read_value(const json::Value& rd, ss::net::inet_address::family& obj) {
    obj = static_cast<ss::net::inet_address::family>(rd.GetInt());
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const ss::net::inet_address::family& b) {
    w.Int(static_cast<int>(b));
}

inline void read_value(const json::Value& rd, net::unresolved_address& obj) {
    ss::sstring host;
    uint16_t port{0};
    read_member(rd, "address", host);
    read_member(rd, "port", port);
    obj = net::unresolved_address(std::move(host), port);
}

inline void read_value(const json::Value& rd, model::broker_endpoint& obj) {
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

inline void read_value(const json::Value& rd, model::broker& obj) {
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

inline void read_value(const json::Value& rd, model::vcluster_id& v) {
    ss::sstring xid_str;
    read_value(rd, xid_str);
    v = model::vcluster_id(xid::from_string(xid_str));
}

enum class tristate_status : uint8_t { disabled = 0, not_set, set };

template<typename T>
void read_value(const json::Value& rd, tristate<T>& target) {
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

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::vcluster_id& v) {
    rjson_serialize(w, ssx::sformat("{}", v));
}

#define json_write(_fname) json::write_member(wr, #_fname, obj._fname)
#define json_read(_fname) json::read_member(rd, #_fname, obj._fname)

} // namespace json
