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

#include "compat/json.h"
#include "model/record.h"

namespace json {

inline void read_value(json::Value const& rd, model::compression& e) {
    std::underlying_type_t<model::compression> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::compression::none;
        break;
    case 1:
        e = model::compression::gzip;
        break;
    case 2:
        e = model::compression::snappy;
        break;
    case 3:
        e = model::compression::lz4;
        break;
    case 4:
        e = model::compression::zstd;
        break;
    case std::numeric_limits<decltype(value)>::max():
        e = model::compression::producer;
        break;
    default:
        vassert(false, "Unknown enum value model::compression: {}", value);
    }
}

inline void read_value(json::Value const& rd, model::timestamp_type& e) {
    std::underlying_type_t<model::timestamp_type> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::timestamp_type::create_time;
        break;
    case 1:
        e = model::timestamp_type::append_time;
        break;
    default:
        vassert(false, "Unknown enum value model::timestamp_type: {}", value);
    }
}

inline void
read_value(json::Value const& rd, model::cleanup_policy_bitflags& e) {
    std::underlying_type_t<model::cleanup_policy_bitflags> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::cleanup_policy_bitflags::none;
        break;
    case 1U:
        e = model::cleanup_policy_bitflags::deletion;
        break;
    case 1U << 1U:
        e = model::cleanup_policy_bitflags::compaction;
        break;
    default:
        vassert(
          false,
          "Unknown enum value model::cleanup_policy_bitflags: {}",
          value);
    }
}

inline void read_value(json::Value const& rd, model::compaction_strategy& e) {
    std::underlying_type_t<model::compaction_strategy> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::compaction_strategy::offset;
        break;
    case 1:
        e = model::compaction_strategy::timestamp;
        break;
    case 2:
        e = model::compaction_strategy::header;
        break;
    default:
        vassert(
          false, "Unknown enum value model::compaction_strategy: {}", value);
    }
}

inline void read_value(json::Value const& rd, model::shadow_indexing_mode& e) {
    std::underlying_type_t<model::shadow_indexing_mode> value;
    read_value(rd, value);
    switch (value) {
    case 0:
        e = model::shadow_indexing_mode::disabled;
        break;
    case 1:
        e = model::shadow_indexing_mode::archival;
        break;
    case 2:
        e = model::shadow_indexing_mode::fetch;
        break;
    case 3:
        e = model::shadow_indexing_mode::full;
        break;
    case 0xfe:
        e = model::shadow_indexing_mode::drop_archival;
        break;

    case 0xfd:
        e = model::shadow_indexing_mode::drop_fetch;
        break;

    case 0xfc:
        e = model::shadow_indexing_mode::drop_full;
        break;
    default:
        vassert(
          false, "Unknown enum value model::shadow_indexing_mode: {}", value);
    }
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::producer_identity& v) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, v.id);
    w.Key("epoch");
    rjson_serialize(w, v.epoch);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::producer_identity& obj) {
    read_member(rd, "id", obj.id);
    read_member(rd, "epoch", obj.epoch);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::broker_shard& v) {
    w.StartObject();
    w.Key("node_id");
    rjson_serialize(w, v.node_id);
    w.Key("shard");
    rjson_serialize(w, v.shard);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::broker_shard& obj) {
    read_member(rd, "node_id", obj.node_id);
    read_member(rd, "shard", obj.shard);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::partition_metadata& v) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, v.id);
    w.Key("replicas");
    rjson_serialize(w, v.replicas);
    w.Key("leader_node");
    rjson_serialize(w, v.leader_node);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::partition_metadata& obj) {
    read_member(rd, "id", obj.id);
    read_member(rd, "replicas", obj.replicas);
    read_member(rd, "leader_node", obj.leader_node);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::topic_metadata& tm) {
    w.StartObject();
    write_member(w, "tp_ns", tm.tp_ns);
    write_member(w, "partitions", tm.partitions);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::topic_metadata& tm) {
    read_member(rd, "tp_ns", tm.tp_ns);
    read_member(rd, "partitions", tm.partitions);
}

// NOTE: These are exceptions to overloads of rjson_serialize(enum) definitions
// defined in v/config/cluster_serializtion.h which print string representations
// of the listed enumerations below. The compat framework expects all enums in
// their json representation to be their respective underlying types. To avoid
// breaking something by modifying the other definitions these types will be
// explicity serialized to json with these special methods

template<typename T>
inline constexpr bool is_exceptional_enum
  = std::is_enum_v<
      T> && (std::is_same_v<T, model::compression> || std::is_same_v<T, model::timestamp_type> || std::is_same_v<T, model::cleanup_policy_bitflags>);

template<typename T>
inline constexpr bool is_exceptional_enum_wrapped_opt
  = is_exceptional_enum<typename T::value_type>;

template<typename T>
void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const std::optional<T>& t) {
    if (t) {
        rjson_serialize_exceptional_type(w, *t);
    } else {
        w.Null();
    }
}

template<typename T>
void rjson_serialize_exceptional_type(
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
        rjson_serialize_exceptional_type(w, t.value());
    }
    w.EndObject();
}

template<typename Writer, typename T>
void write_exceptional_member_type(Writer& w, char const* key, T const& value) {
    w.String(key);
    rjson_serialize_exceptional_type(w, value);
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const model::compression& c) {
    using underlying_t = std::underlying_type_t<model::compression>;
    rjson_serialize(w, static_cast<underlying_t>(c));
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w, const model::timestamp_type& c) {
    using underlying_t = std::underlying_type_t<model::timestamp_type>;
    rjson_serialize(w, static_cast<underlying_t>(c));
}

inline void rjson_serialize_exceptional_type(
  json::Writer<json::StringBuffer>& w,
  const model::cleanup_policy_bitflags& c) {
    using underlying_t = std::underlying_type_t<model::cleanup_policy_bitflags>;
    rjson_serialize(w, static_cast<underlying_t>(c));
}

} // namespace json
