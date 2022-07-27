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

#include "json/document.h"
#include "json/json.h"
#include "model/fundamental.h"

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

template<typename T, typename Tag, typename IsConstexpr>
void read_value(
  json::Value const& v, detail::base_named_type<T, Tag, IsConstexpr>& target) {
    auto t = T{};
    read_value(v, t);
    target = detail::base_named_type<T, Tag, IsConstexpr>{t};
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

template<typename Writer, typename T>
void write_member(Writer& w, char const* key, T const& value) {
    w.String(key);
    rjson_serialize(w, value);
}

template<typename T>
void read_member(json::Value const& v, char const* key, T& target) {
    auto const it = v.FindMember(key);
    if (it != v.MemberEnd()) {
        read_value(it->value, target);
    } else {
        target = {};
        std::cout << "key " << key << " not found, default initializing";
    }
}

template<typename Enum>
inline auto read_member_enum(json::Value const& v, char const* key, Enum)
  -> std::underlying_type_t<Enum> {
    std::underlying_type_t<Enum> value;
    read_member(v, key, value);
    return value;
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

#define json_write(_fname) json::write_member(wr, #_fname, obj._fname)
#define json_read(_fname) json::read_member(rd, #_fname, obj._fname)

} // namespace json
