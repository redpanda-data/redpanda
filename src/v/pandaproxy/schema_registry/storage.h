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
#include "json/json.h"
#include "model/record_utils.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"
#include "utils/string_switch.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/std-coroutine.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace pandaproxy::schema_registry {

using topic_key_magic = named_type<int32_t, struct topic_key_magic_tag>;
enum class topic_key_type { noop = 0, schema, config, delete_subject };
constexpr std::string_view to_string_view(topic_key_type kt) {
    switch (kt) {
    case topic_key_type::noop:
        return "NOOP";
    case topic_key_type::schema:
        return "SCHEMA";
    case topic_key_type::config:
        return "CONFIG";
    case topic_key_type::delete_subject:
        return "DELETE_SUBJECT";
    }
    return "{invalid}";
};
template<>
constexpr std::optional<topic_key_type>
from_string_view<topic_key_type>(std::string_view sv) {
    return string_switch<std::optional<topic_key_type>>(sv)
      .match(to_string_view(topic_key_type::noop), topic_key_type::noop)
      .match(to_string_view(topic_key_type::schema), topic_key_type::schema)
      .match(to_string_view(topic_key_type::config), topic_key_type::config)
      .match(
        to_string_view(topic_key_type::delete_subject),
        topic_key_type::delete_subject)
      .default_match(std::nullopt);
}

// Just peek at the keytype. Allow other fields through.
template<typename Encoding = rapidjson::UTF8<>>
class topic_key_type_handler
  : public rapidjson::
      BaseReaderHandler<Encoding, topic_key_type_handler<Encoding>> {
    enum class state {
        empty = 0,
        object,
        keytype,
    };
    state _state = state::empty;

public:
    using Ch = typename rapidjson::BaseReaderHandler<Encoding>::Ch;
    using rjson_parse_result = ss::sstring;
    rjson_parse_result result;

    topic_key_type_handler()
      : rapidjson::
        BaseReaderHandler<Encoding, topic_key_type_handler<Encoding>>{} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::object && sv == "keytype") {
            _state = state::keytype;
        }
        return true;
    }

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        if (_state == state::keytype) {
            result = ss::sstring{str, len};
            _state = state::object;
        }
        return true;
    }

    bool StartObject() {
        if (_state == state::empty) {
            _state = state::object;
        }
        return true;
    }

    bool EndObject(rapidjson::SizeType) {
        if (_state == state::object) {
            _state = state::empty;
        }
        return true;
    }
};

struct schema_key {
    static constexpr topic_key_type keytype{topic_key_type::schema};
    subject sub;
    schema_version version;
    topic_key_magic magic{1};

    friend bool operator==(const schema_key&, const schema_key&) = default;

    friend std::ostream& operator<<(std::ostream& os, const schema_key& v) {
        fmt::print(
          os,
          "keytype: {}, subject: {}, version: {}, magic: {}",
          to_string_view(v.keytype),
          v.sub,
          v.version,
          v.magic);
        return os;
    }
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const schema_registry::schema_key& key) {
    w.StartObject();
    w.Key("keytype");
    ::json::rjson_serialize(w, to_string_view(key.keytype));
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
            auto kt = from_string_view<topic_key_type>(sv);
            _state = state::object;
            return kt == result.keytype;
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

struct schema_value {
    subject sub;
    schema_version version;
    schema_type type{schema_type::avro};
    schema_id id;
    schema_definition schema;
    is_deleted deleted{false};

    friend bool operator==(const schema_value&, const schema_value&) = default;

    friend std::ostream& operator<<(std::ostream& os, const schema_value& v) {
        fmt::print(
          os,
          "subject: {}, version: {}, type: {}, id: {}, schema: {}, deleted: {}",
          v.sub,
          v.version,
          v.type,
          v.id,
          v.schema,
          v.deleted);
        return os;
    }
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
    ::json::rjson_serialize(w, bool(val.deleted));
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
            result.deleted = is_deleted(b);
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
            auto type = from_string_view<schema_type>(sv);
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

struct config_key {
    static constexpr topic_key_type keytype{topic_key_type::config};
    std::optional<subject> sub;
    topic_key_magic magic{0};

    friend bool operator==(const config_key&, const config_key&) = default;

    friend std::ostream& operator<<(std::ostream& os, const config_key& v) {
        fmt::print(
          os,
          "keytype: {}, subject: {}, magic: {}",
          to_string_view(v.keytype),
          v.sub.value_or(invalid_subject),
          v.magic);
        return os;
    }
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const schema_registry::config_key& key) {
    w.StartObject();
    w.Key("keytype");
    ::json::rjson_serialize(w, to_string_view(key.keytype));
    w.Key("subject");
    if (key.sub) {
        ::json::rjson_serialize(w, key.sub.value());
    } else {
        w.Null();
    }
    w.Key("magic");
    ::json::rjson_serialize(w, key.magic);
    w.EndObject();
}

template<typename Encoding = rapidjson::UTF8<>>
class config_key_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        keytype,
        subject,
        magic,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = config_key;
    rjson_parse_result result;

    config_key_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        std::optional<state> s{string_switch<std::optional<state>>(sv)
                                 .match("keytype", state::keytype)
                                 .match("subject", state::subject)
                                 .match("magic", state::magic)
                                 .default_match(std::nullopt)};
        return s.has_value() && std::exchange(_state, *s) == state::object;
    }

    bool Uint(int i) {
        result.magic = topic_key_magic{i};
        return std::exchange(_state, state::object) == state::magic;
    }

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::keytype: {
            auto kt = from_string_view<topic_key_type>(sv);
            _state = state::object;
            return kt == result.keytype;
        }
        case state::subject: {
            result.sub = subject{ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::object:
        case state::magic:
            return false;
        }
        return false;
    }

    bool Null() {
        // The subject, and only the subject, is nullable.
        return std::exchange(_state, state::object) == state::subject;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(rapidjson::SizeType) {
        return std::exchange(_state, state::empty) == state::object;
    }
};

struct config_value {
    compatibility_level compat{compatibility_level::none};

    friend bool operator==(const config_value&, const config_value&) = default;

    friend std::ostream& operator<<(std::ostream& os, const config_value& v) {
        fmt::print(os, "compatibility: {}", to_string_view(v.compat));
        return os;
    }
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const schema_registry::config_value& val) {
    w.StartObject();
    w.Key("compatibilityLevel");
    ::json::rjson_serialize(w, to_string_view(val.compat));
    w.EndObject();
}

template<typename Encoding = rapidjson::UTF8<>>
class config_value_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        compatibility,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = config_value;
    rjson_parse_result result;

    config_value_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::object && sv == "compatibilityLevel") {
            _state = state::compatibility;
            return true;
        }
        return false;
    }

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::compatibility) {
            auto s = from_string_view<compatibility_level>(sv);
            if (s.has_value()) {
                result.compat = *s;
                _state = state::object;
            }
            return s.has_value();
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

struct delete_subject_key {
    static constexpr topic_key_type keytype{topic_key_type::delete_subject};
    subject sub;
    topic_key_magic magic{0};

    friend bool operator==(const delete_subject_key&, const delete_subject_key&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const delete_subject_key& v) {
        fmt::print(
          os,
          "keytype: {}, subject: {}, magic: {}",
          to_string_view(v.keytype),
          v.sub,
          v.magic);
        return os;
    }
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const delete_subject_key& key) {
    w.StartObject();
    w.Key("keytype");
    ::json::rjson_serialize(w, to_string_view(key.keytype));
    w.Key("subject");
    ::json::rjson_serialize(w, key.sub());
    w.Key("magic");
    ::json::rjson_serialize(w, key.magic);
    w.EndObject();
}

template<typename Encoding = rapidjson::UTF8<>>
class delete_subject_key_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        keytype,
        subject,
        magic,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = delete_subject_key;
    rjson_parse_result result;

    delete_subject_key_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::object: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("keytype", state::keytype)
                                     .match("subject", state::subject)
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
        case state::magic:
            return false;
        }
        return false;
    }

    bool Uint(int i) {
        switch (_state) {
        case state::magic: {
            result.magic = topic_key_magic{i};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::keytype:
        case state::object:
        case state::subject:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::keytype: {
            auto kt = from_string_view<topic_key_type>(sv);
            _state = state::object;
            return kt == result.keytype;
        }
        case state::subject: {
            result.sub = subject{ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::object:
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

struct delete_subject_value {
    subject sub;
    schema_version version;

    friend bool
    operator==(const delete_subject_value&, const delete_subject_value&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const delete_subject_value& v) {
        fmt::print(os, "subject: {}, version: {}", v.sub, v.version);
        return os;
    }
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const delete_subject_value& val) {
    w.StartObject();
    w.Key("subject");
    ::json::rjson_serialize(w, val.sub);
    w.Key("version");
    ::json::rjson_serialize(w, val.version);
    w.EndObject();
}

template<typename Encoding = rapidjson::UTF8<>>
class delete_subject_value_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        subject,
        version,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = delete_subject_value;
    rjson_parse_result result;

    delete_subject_value_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::object: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("subject", state::subject)
                                     .match("version", state::version)
                                     .default_match(std::nullopt)};
            if (s.has_value()) {
                _state = *s;
            }
            return s.has_value();
        }
        case state::empty:
        case state::subject:
        case state::version:
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
        case state::empty:
        case state::object:
        case state::subject:
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
        case state::empty:
        case state::object:
        case state::version:
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

template<typename Handler>
auto from_json_iobuf(iobuf&& iobuf) {
    auto p = iobuf_parser(std::move(iobuf));
    auto str = p.read_string(p.bytes_left());
    return json::rjson_parse(str.data(), Handler{});
}

template<typename T>
auto to_json_iobuf(T t) {
    auto val_js = json::rjson_serialize(t);
    iobuf buf;
    buf.append(val_js.data(), val_js.size());
    return buf;
}

template<typename Key, typename Value>
model::record_batch as_record_batch(Key key, Value val) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};
    rb.add_raw_kv(to_json_iobuf(std::move(key)), to_json_iobuf(std::move(val)));
    return std::move(rb).build();
}

inline model::record_batch make_schema_batch(
  subject sub,
  schema_version ver,
  schema_id id,
  schema_definition schema,
  schema_type type,
  is_deleted deleted) {
    schema_key key{.sub{sub}, .version{ver}};
    return as_record_batch(
      std::move(key),
      schema_value{
        .sub{std::move(sub)},
        .version{ver},
        .type = type,
        .id{id},
        .schema{std::move(schema)},
        .deleted = deleted});
}

inline model::record_batch
make_config_batch(std::optional<subject> sub, compatibility_level compat) {
    return as_record_batch(
      config_key{.sub{std::move(sub)}}, config_value{.compat = compat});
}

inline model::record_batch
make_delete_subject_batch(subject sub, schema_version version) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    rb.add_raw_kv(
      to_json_iobuf(delete_subject_key{.sub{sub}}),
      to_json_iobuf(delete_subject_value{.sub{sub}, .version{version}}));
    rb.add_raw_kv(to_json_iobuf(config_key{.sub{sub}}), std::nullopt);
    return std::move(rb).build();
}

inline model::record_batch make_delete_subject_permanently_batch(
  subject sub, const std::vector<schema_version>& versions) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    std::for_each(versions.cbegin(), versions.cend(), [&](auto version) {
        rb.add_raw_kv(
          to_json_iobuf(schema_key{.sub{sub}, .version{version}}),
          std::nullopt);
    });
    return std::move(rb).build();
}

inline model::record_batch
make_delete_subject_version_batch(subject_schema schema) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    auto key = to_json_iobuf(
      schema_key{.sub{schema.sub}, .version{schema.version}});
    rb.add_raw_kv(
      std::move(key),
      to_json_iobuf(schema_value{
        .sub{std::move(schema.sub)},
        .version{schema.version},
        .type = schema.type,
        .id{schema.id},
        .schema{std::move(schema.definition)},
        .deleted{is_deleted::yes}}));
    return std::move(rb).build();
}

inline model::record_batch make_delete_subject_version_permanently_batch(
  const subject& sub, schema_version version) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    rb.add_raw_kv(
      to_json_iobuf(schema_key{.sub{sub}, .version{version}}), std::nullopt);
    return std::move(rb).build();
}

struct consume_to_store {
    explicit consume_to_store(store& s)
      : _store{s} {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        if (!b.header().attrs.is_control()) {
            co_await model::for_each_record(b, [this](model::record& rec) {
                return (*this)(std::move(rec));
            });
        }
        co_return ss::stop_iteration::no;
    }

    ss::future<> operator()(model::record record) {
        auto key = record.release_key();
        auto key_type_str = from_json_iobuf<topic_key_type_handler<>>(
          key.share(0, key.size_bytes()));

        auto key_type = from_string_view<topic_key_type>(key_type_str);
        if (!key_type.has_value()) {
            vlog(plog.error, "Ignoring keytype: {}", key_type_str);
            co_return;
        }

        switch (*key_type) {
        case topic_key_type::noop:
            co_return;
        case topic_key_type::schema:
            co_return co_await apply(
              from_json_iobuf<schema_key_handler<>>(std::move(key)),
              from_json_iobuf<schema_value_handler<>>(record.release_value()));
        case topic_key_type::config:
            co_return co_await apply(
              from_json_iobuf<config_key_handler<>>(std::move(key)),
              from_json_iobuf<config_value_handler<>>(record.release_value()));
        case topic_key_type::delete_subject:
            co_return;
        }
    }

    ss::future<> apply(schema_key key, schema_value val) {
        if (key.magic != 0 && key.magic != 1) {
            throw exception(
              error_code::topic_parse_error,
              fmt::format("key has unexpected magic: {}", key));
        }
        vlog(plog.debug, "Inserting key: {}", key);
        _store.upsert(
          std::move(val.sub),
          std::move(val.schema),
          val.type,
          val.id,
          val.version,
          val.deleted);
        co_return;
    }

    ss::future<> apply(config_key key, config_value val) {
        if (key.magic != 0) {
            throw exception(
              error_code::topic_parse_error,
              fmt::format("key has unexpected magic: {}", key));
        }
        vlog(plog.debug, "Applying config: {}", key);
        if (key.sub) {
            _store.set_compatibility(*key.sub, val.compat).value();
        } else {
            _store.set_compatibility(val.compat).value();
        }
        co_return;
    }

    void end_of_stream() {}
    store& _store;
};

} // namespace pandaproxy::schema_registry
