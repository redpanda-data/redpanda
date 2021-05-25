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

#pragma once

#include "bytes/iobuf_parser.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/string_switch.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace pandaproxy::schema_registry {

using topic_key_magic = named_type<int32_t, struct topic_key_magic_tag>;
using topic_key_type = named_type<ss::sstring, struct topic_key_type_tag>;

struct schema_key {
    topic_key_type keytype{"SCHEMA"};
    subject sub;
    schema_version version;
    topic_key_magic magic{1};
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const schema_registry::schema_key& key) {
    w.StartObject();
    w.Key("keytype");
    ::json::rjson_serialize(w, key.keytype);
    w.Key("subject");
    ::json::rjson_serialize(w, key.sub());
    w.Key("version");
    ::json::rjson_serialize(w, key.version);
    w.Key("magic");
    ::json::rjson_serialize(w, key.magic);
    w.EndObject();
}

template<typename Encoding = rapidjson::UTF8<>>
class schema_key_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        keytype,
        subject,
        version,
        magic,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = schema_key;
    rjson_parse_result result;

    schema_key_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::object: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("keytype", state::keytype)
                                     .match("subject", state::subject)
                                     .match("version", state::version)
                                     .match("magic", state::magic)
                                     .default_match(std::nullopt)};
            if (s.has_value()) {
                _state = *s;
            }
            return s.has_value();
        }
        case state::empty:
        case state::keytype:
        case state::subject:
        case state::version:
        case state::magic:
            return false;
        }
        return false;
    }

    bool Uint(int i) {
        switch (_state) {
        case state::version: {
            result.version = schema_version{i};
            _state = state::object;
            return true;
        }
        case state::magic: {
            result.magic = topic_key_magic{i};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::object:
        case state::keytype:
        case state::subject:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::keytype: {
            result.keytype = topic_key_type{ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::subject: {
            result.sub = subject{ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::object:
        case state::version:
        case state::magic:
            return false;
        }
        return false;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(rapidjson::SizeType) {
        return std::exchange(_state, state::empty) == state::object;
    }
};

inline schema_key schema_key_from_iobuf(iobuf&& iobuf) {
    auto p = iobuf_parser(std::move(iobuf));
    auto str = p.read_string(p.bytes_left());
    return json::rjson_parse(str.data(), schema_key_handler<>{});
}

inline iobuf schema_key_to_iobuf(const schema_key& key) {
    auto str = json::rjson_serialize(key);
    iobuf buf;
    buf.append(str.data(), str.size());
    return buf;
}

struct schema_value {
    subject sub;
    schema_version version;
    schema_type type{schema_type::avro};
    schema_id id;
    schema_definition schema;
    bool deleted{false};
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const schema_registry::schema_value& val) {
    w.StartObject();
    w.Key("subject");
    ::json::rjson_serialize(w, val.sub);
    w.Key("version");
    ::json::rjson_serialize(w, val.version);
    w.Key("id");
    ::json::rjson_serialize(w, val.id);
    w.Key("schema");
    ::json::rjson_serialize(w, val.schema);
    w.Key("deleted");
    ::json::rjson_serialize(w, val.deleted);
    if (val.type != schema_type::avro) {
        w.Key("schemaType");
        ::json::rjson_serialize(w, to_string_view(val.type));
    }
    w.EndObject();
}

template<typename Encoding = rapidjson::UTF8<>>
class schema_value_handler final : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        subject,
        version,
        type,
        id,
        definition,
        deleted,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = schema_value;
    rjson_parse_result result;

    schema_value_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::object: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("subject", state::subject)
                                     .match("version", state::version)
                                     .match("schemaType", state::type)
                                     .match("schema", state::definition)
                                     .match("id", state::id)
                                     .match("deleted", state::deleted)
                                     .default_match(std::nullopt)};
            if (s.has_value()) {
                _state = *s;
            }
            return s.has_value();
        }
        case state::empty:
        case state::subject:
        case state::version:
        case state::type:
        case state::id:
        case state::definition:
        case state::deleted:
            return false;
        }
        return false;
    }

    bool Uint(int i) {
        switch (_state) {
        case state::version: {
            result.version = schema_version{i};
            _state = state::object;
            return true;
        }
        case state::id: {
            result.id = schema_id{i};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::object:
        case state::subject:
        case state::type:
        case state::definition:
        case state::deleted:
            return false;
        }
        return false;
    }

    bool Bool(bool b) {
        switch (_state) {
        case state::deleted: {
            result.deleted = b;
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::object:
        case state::subject:
        case state::version:
        case state::type:
        case state::id:
        case state::definition:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::subject: {
            result.sub = subject{ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::definition: {
            result.schema = schema_definition{ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::type: {
            std::optional<schema_type> type{
              string_switch<std::optional<schema_type>>(sv)
                .match(to_string_view(schema_type::avro), schema_type::avro)
                .match(to_string_view(schema_type::json), schema_type::json)
                .match(
                  to_string_view(schema_type::protobuf), schema_type::protobuf)
                .default_match(std::nullopt)};
            if (type.has_value()) {
                result.type = *type;
                _state = state::object;
            }
            return type.has_value();
        }
        case state::empty:
        case state::object:
        case state::version:
        case state::id:
        case state::deleted:
            return false;
        }
        return false;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(rapidjson::SizeType) {
        return std::exchange(_state, state::empty) == state::object;
    }
};

inline schema_value schema_value_from_iobuf(iobuf&& iobuf) {
    auto p = iobuf_parser(std::move(iobuf));
    auto str = p.read_string(p.bytes_left());
    return json::rjson_parse(str.data(), schema_value_handler<>{});
}

inline iobuf schema_value_to_iobuf(
  subject sub,
  schema_version ver,
  schema_id id,
  schema_definition schema,
  schema_type type,
  bool deleted) {
    auto str = json::rjson_serialize(schema_value{
      .sub{std::move(sub)},
      .version{ver},
      .type = type,
      .id{id},
      .schema{std::move(schema)},
      .deleted = deleted});
    iobuf val;
    val.append(str.data(), str.size());
    return val;
}

} // namespace pandaproxy::schema_registry
