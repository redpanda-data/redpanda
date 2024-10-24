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

#include "base/vlog.h"
#include "json/iobuf_writer.h"
#include "json/json.h"
#include "json/types.h"
#include "json/writer.h"
#include "model/metadata.h"
#include "model/record_utils.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "storage/parser_utils.h"
#include "storage/record_batch_builder.h"
#include "strings/string_switch.h"

#include <seastar/core/coroutine.hh>

namespace pandaproxy::schema_registry {

using topic_key_magic = named_type<int32_t, struct topic_key_magic_tag>;
enum class topic_key_type { noop = 0, schema, config, mode, delete_subject };

constexpr std::string_view to_string_view(topic_key_type kt) {
    switch (kt) {
    case topic_key_type::noop:
        return "NOOP";
    case topic_key_type::schema:
        return "SCHEMA";
    case topic_key_type::config:
        return "CONFIG";
    case topic_key_type::mode:
        return "MODE";
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
      .match(to_string_view(topic_key_type::mode), topic_key_type::mode)
      .match(
        to_string_view(topic_key_type::delete_subject),
        topic_key_type::delete_subject)
      .default_match(std::nullopt);
}

// Just peek at the keytype. Allow other fields through.
template<typename Encoding = ::json::UTF8<>>
class topic_key_type_handler
  : public ::json::
      BaseReaderHandler<Encoding, topic_key_type_handler<Encoding>> {
    enum class state {
        empty = 0,
        object,
        keytype,
    };
    state _state = state::empty;

public:
    using Ch = typename ::json::BaseReaderHandler<Encoding>::Ch;
    using rjson_parse_result = ss::sstring;
    rjson_parse_result result;

    topic_key_type_handler()
      : ::json::
          BaseReaderHandler<Encoding, topic_key_type_handler<Encoding>>{} {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::object && sv == "keytype") {
            _state = state::keytype;
        }
        return true;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
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

    bool EndObject(::json::SizeType) {
        if (_state == state::object) {
            _state = state::empty;
        }
        return true;
    }
};

struct schema_key {
    static constexpr topic_key_type keytype{topic_key_type::schema};

    // The record is only valid if its offset in the topic matches `seq`
    std::optional<model::offset> seq;

    // The node differentiates conflicting writes to the same seq,
    // to prevent compaction from collapsing invalid writes into
    // preceding valid writes.
    std::optional<model::node_id> node;

    subject sub;
    schema_version version;
    topic_key_magic magic{1};

    friend bool operator==(const schema_key&, const schema_key&) = default;

    friend std::ostream& operator<<(std::ostream& os, const schema_key& v) {
        if (v.seq.has_value() && v.node.has_value()) {
            fmt::print(
              os,
              "seq: {}, node: {}, keytype: {}, subject: {}, version: {}, "
              "magic: {}",
              *v.seq,
              *v.node,
              to_string_view(v.keytype),
              v.sub,
              v.version,
              v.magic);
        } else {
            fmt::print(
              os,
              "unsequenced keytype: {}, subject: {}, version: {}, magic: {}",
              to_string_view(v.keytype),
              v.sub,
              v.version,
              v.magic);
        }
        return os;
    }
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const schema_registry::schema_key& key) {
    w.StartObject();
    w.Key("keytype");
    ::json::rjson_serialize(w, to_string_view(key.keytype));
    w.Key("subject");
    ::json::rjson_serialize(w, key.sub());
    w.Key("version");
    ::json::rjson_serialize(w, key.version);
    w.Key("magic");
    ::json::rjson_serialize(w, key.magic);
    if (key.seq.has_value()) {
        w.Key("seq");
        ::json::rjson_serialize(w, key.seq);
    }
    if (key.node.has_value()) {
        w.Key("node");
        ::json::rjson_serialize(w, key.node);
    }
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class schema_key_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        keytype,
        subject,
        version,
        seq,
        node,
        magic,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = schema_key;
    rjson_parse_result result;

    schema_key_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::object: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("keytype", state::keytype)
                                     .match("subject", state::subject)
                                     .match("version", state::version)
                                     .match("magic", state::magic)
                                     .match("seq", state::seq)
                                     .match("node", state::node)
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
        case state::seq:
        case state::node:
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
        case state::seq: {
            result.seq = model::offset{i};
            _state = state::object;
            return true;
        }
        case state::node: {
            result.node = model::node_id{i};
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

    bool String(const Ch* str, ::json::SizeType len, bool) {
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
        case state::seq:
        case state::node:
            return false;
        }
        return false;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(::json::SizeType) {
        return result.seq.has_value() == result.node.has_value()
               && std::exchange(_state, state::empty) == state::object;
    }
};

template<typename Tag>
struct schema_value {
    typed_schema<Tag> schema;
    schema_version version;
    schema_id id;
    is_deleted deleted{false};

    friend bool operator==(const schema_value&, const schema_value&) = default;

    friend std::ostream& operator<<(std::ostream& os, const schema_value& v) {
        fmt::print(
          os,
          "{}, version: {}, id: {}, deleted: {}",
          v.schema,
          v.version,
          v.id,
          v.deleted);
        return os;
    }
};

using unparsed_schema_value = schema_value<unparsed_schema_defnition_tag>;
using canonical_schema_value = schema_value<canonical_schema_definition_tag>;

template<typename Buffer, typename Tag>
void rjson_serialize(
  ::json::iobuf_writer<Buffer>& w, const schema_value<Tag>& val) {
    w.StartObject();
    w.Key("subject");
    ::json::rjson_serialize(w, val.schema.sub());
    w.Key("version");
    ::json::rjson_serialize(w, val.version);
    w.Key("id");
    ::json::rjson_serialize(w, val.id);
    auto type = val.schema.type();
    if (type != schema_type::avro) {
        w.Key("schemaType");
        ::json::rjson_serialize(w, to_string_view(type));
    }
    if (!val.schema.def().refs().empty()) {
        w.Key("references");
        w.StartArray();
        for (const auto& ref : val.schema.def().refs()) {
            w.StartObject();
            w.Key("name");
            ::json::rjson_serialize(w, ref.name);
            w.Key("subject");
            ::json::rjson_serialize(w, ref.sub);
            w.Key("version");
            ::json::rjson_serialize(w, ref.version);
            w.EndObject();
        }
        w.EndArray();
    }
    w.Key("schema");
    ::json::rjson_serialize(w, val.schema.def().raw());
    w.Key("deleted");
    ::json::rjson_serialize(w, bool(val.deleted));
    w.EndObject();
}

template<typename Tag, typename Encoding = ::json::UTF8<>>
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
        references,
        reference,
        reference_name,
        reference_subject,
        reference_version,
    };
    state _state = state::empty;

    struct mutable_schema {
        subject sub{invalid_subject};
        typename typed_schema_definition<Tag>::raw_string def;
        schema_type type{schema_type::avro};
        typename typed_schema_definition<Tag>::references refs;
    };
    mutable_schema _schema;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = schema_value<Tag>;
    rjson_parse_result result;

    schema_value_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
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
                                     .match("references", state::references)
                                     .default_match(std::nullopt)};
            if (s.has_value()) {
                _state = *s;
            }
            return s.has_value();
        }
        case state::reference: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("name", state::reference_name)
                                     .match("subject", state::reference_subject)
                                     .match("version", state::reference_version)
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
        case state::references:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
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
        case state::reference_version: {
            _schema.refs.back().version = schema_version{i};
            _state = state::reference;
            return true;
        }
        case state::empty:
        case state::object:
        case state::subject:
        case state::type:
        case state::definition:
        case state::deleted:
        case state::references:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
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
        case state::references:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::subject: {
            _schema.sub = subject{ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::definition: {
            _schema.def = typename typed_schema_definition<Tag>::raw_string{
              ss::sstring{sv}};
            _state = state::object;
            return true;
        }
        case state::type: {
            auto type = from_string_view<schema_type>(sv);
            if (type.has_value()) {
                _schema.type = *type;
                _state = state::object;
            }
            return type.has_value();
        }
        case state::reference_name: {
            _schema.refs.back().name = ss::sstring{sv};
            _state = state::reference;
            return true;
        }
        case state::reference_subject: {
            _schema.refs.back().sub = subject{ss::sstring{sv}};
            _state = state::reference;
            return true;
        }
        case state::empty:
        case state::object:
        case state::version:
        case state::id:
        case state::deleted:
        case state::references:
        case state::reference:
        case state::reference_version:
            return false;
        }
        return false;
    }

    bool StartObject() {
        switch (_state) {
        case state::empty: {
            _state = state::object;
            return true;
        }
        case state::references: {
            _schema.refs.emplace_back();
            _state = state::reference;
            return true;
        }
        case state::object:
        case state::subject:
        case state::version:
        case state::type:
        case state::id:
        case state::definition:
        case state::deleted:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
            return false;
        }
        return false;
    }

    bool EndObject(::json::SizeType) {
        switch (_state) {
        case state::object: {
            _state = state::empty;
            result.schema = {
              std::move(_schema.sub),
              {std::move(_schema.def), _schema.type, std::move(_schema.refs)}};
            return true;
        }
        case state::reference: {
            _state = state::references;
            const auto& ref{_schema.refs.back()};
            return !ref.name.empty() && ref.sub != invalid_subject
                   && ref.version != invalid_schema_version;
        }
        case state::empty:
        case state::subject:
        case state::version:
        case state::type:
        case state::id:
        case state::definition:
        case state::deleted:
        case state::references:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
            return false;
        }
        return false;
    }

    bool StartArray() { return _state == state::references; }

    bool EndArray(::json::SizeType) {
        return std::exchange(_state, state::object) == state::references;
    }
};

template<typename Encoding = ::json::UTF8<>>
using unparsed_schema_value_handler
  = schema_value_handler<unparsed_schema_defnition_tag, Encoding>;
template<typename Encoding = ::json::UTF8<>>
using canonical_schema_value_handler
  = schema_value_handler<canonical_schema_definition_tag, Encoding>;

struct config_key {
    static constexpr topic_key_type keytype{topic_key_type::config};
    std::optional<model::offset> seq;
    std::optional<model::node_id> node;
    std::optional<subject> sub;
    topic_key_magic magic{0};

    friend bool operator==(const config_key&, const config_key&) = default;

    friend std::ostream& operator<<(std::ostream& os, const config_key& v) {
        if (v.seq.has_value() && v.node.has_value()) {
            fmt::print(
              os,
              "seq: {} node: {} keytype: {}, subject: {}, magic: {}",
              *v.seq,
              *v.node,
              to_string_view(v.keytype),
              v.sub.value_or(invalid_subject),
              v.magic);
        } else {
            fmt::print(
              os,
              "unsequenced keytype: {}, subject: {}, magic: {}",
              to_string_view(v.keytype),
              v.sub.value_or(invalid_subject),
              v.magic);
        }
        return os;
    }
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const schema_registry::config_key& key) {
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
    if (key.seq.has_value()) {
        w.Key("seq");
        ::json::rjson_serialize(w, *key.seq);
    }
    if (key.node.has_value()) {
        w.Key("node");
        ::json::rjson_serialize(w, *key.node);
    }
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class config_key_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        keytype,
        seq,
        node,
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

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        std::optional<state> s{string_switch<std::optional<state>>(sv)
                                 .match("keytype", state::keytype)
                                 .match("seq", state::seq)
                                 .match("node", state::node)
                                 .match("subject", state::subject)
                                 .match("magic", state::magic)
                                 .default_match(std::nullopt)};
        return s.has_value() && std::exchange(_state, *s) == state::object;
    }

    bool Uint(int i) {
        switch (_state) {
        case state::magic: {
            result.magic = topic_key_magic{i};
            _state = state::object;
            return true;
        }
        case state::seq: {
            result.seq = model::offset{i};
            _state = state::object;
            return true;
        }
        case state::node: {
            result.node = model::node_id{i};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::subject:
        case state::keytype:
        case state::object:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
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
        case state::seq:
        case state::node:
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

    bool EndObject(::json::SizeType) {
        return result.seq.has_value() == result.node.has_value()
               && std::exchange(_state, state::empty) == state::object;
    }
};

struct config_value {
    compatibility_level compat{compatibility_level::none};
    std::optional<subject> sub;

    friend bool operator==(const config_value&, const config_value&) = default;

    friend std::ostream& operator<<(std::ostream& os, const config_value& v) {
        if (v.sub.has_value()) {
            fmt::print(os, "subject: {}, ", v.sub.value());
        }
        fmt::print(os, "compatibility: {}", to_string_view(v.compat));

        return os;
    }
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const schema_registry::config_value& val) {
    w.StartObject();
    if (val.sub.has_value()) {
        w.Key("subject");
        ::json::rjson_serialize(w, val.sub.value());
    }
    w.Key("compatibilityLevel");
    ::json::rjson_serialize(w, to_string_view(val.compat));
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class config_value_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        compatibility,
        subject,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = config_value;
    rjson_parse_result result;

    config_value_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        std::optional<state> s{
          string_switch<std::optional<state>>(sv)
            .match("compatibilityLevel", state::compatibility)
            .match("subject", state::subject)
            .default_match(std::nullopt)};
        return s.has_value() && std::exchange(_state, *s) == state::object;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::compatibility) {
            auto s = from_string_view<compatibility_level>(sv);
            if (s.has_value()) {
                result.compat = *s;
                _state = state::object;
            }
            return s.has_value();
        } else if (_state == state::subject) {
            result.sub.emplace(sv);
            _state = state::object;
            return true;
        }
        return false;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(::json::SizeType) {
        return std::exchange(_state, state::empty) == state::object;
    }
};

struct mode_key {
    static constexpr topic_key_type keytype{topic_key_type::mode};
    std::optional<model::offset> seq;
    std::optional<model::node_id> node;
    std::optional<subject> sub;
    topic_key_magic magic{0};

    friend bool operator==(const mode_key&, const mode_key&) = default;

    friend std::ostream& operator<<(std::ostream& os, const mode_key& v) {
        if (v.seq.has_value() && v.node.has_value()) {
            fmt::print(
              os,
              "seq: {} node: {} keytype: {}, subject: {}, magic: {}",
              *v.seq,
              *v.node,
              to_string_view(v.keytype),
              v.sub.value_or(invalid_subject),
              v.magic);
        } else {
            fmt::print(
              os,
              "unsequenced keytype: {}, subject: {}, magic: {}",
              to_string_view(v.keytype),
              v.sub.value_or(invalid_subject),
              v.magic);
        }
        return os;
    }
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const schema_registry::mode_key& key) {
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
    if (key.seq.has_value()) {
        w.Key("seq");
        ::json::rjson_serialize(w, *key.seq);
    }
    if (key.node.has_value()) {
        w.Key("node");
        ::json::rjson_serialize(w, *key.node);
    }
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class mode_key_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        keytype,
        seq,
        node,
        subject,
        magic,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = mode_key;
    rjson_parse_result result;

    mode_key_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        std::optional<state> s{string_switch<std::optional<state>>(sv)
                                 .match("keytype", state::keytype)
                                 .match("seq", state::seq)
                                 .match("node", state::node)
                                 .match("subject", state::subject)
                                 .match("magic", state::magic)
                                 .default_match(std::nullopt)};
        return s.has_value() && std::exchange(_state, *s) == state::object;
    }

    bool Uint(int i) {
        switch (_state) {
        case state::magic: {
            result.magic = topic_key_magic{i};
            _state = state::object;
            return true;
        }
        case state::seq: {
            result.seq = model::offset{i};
            _state = state::object;
            return true;
        }
        case state::node: {
            result.node = model::node_id{i};
            _state = state::object;
            return true;
        }
        case state::empty:
        case state::subject:
        case state::keytype:
        case state::object:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
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
        case state::seq:
        case state::node:
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

    bool EndObject(::json::SizeType) {
        return result.seq.has_value() == result.node.has_value()
               && std::exchange(_state, state::empty) == state::object;
    }
};

struct mode_value {
    mode mode{mode::read_write};
    std::optional<subject> sub;

    friend bool operator==(const mode_value&, const mode_value&) = default;

    friend std::ostream& operator<<(std::ostream& os, const mode_value& v) {
        if (v.sub.has_value()) {
            fmt::print(os, "subject: {}, ", v.sub.value());
        }
        fmt::print(os, "mode: {}", to_string_view(v.mode));

        return os;
    }
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const schema_registry::mode_value& val) {
    w.StartObject();
    if (val.sub.has_value()) {
        w.Key("subject");
        ::json::rjson_serialize(w, val.sub.value());
    }
    w.Key("mode");
    ::json::rjson_serialize(w, to_string_view(val.mode));
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class mode_value_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        object,
        mode,
        subject,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = mode_value;
    rjson_parse_result result;

    mode_value_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        std::optional<state> s{string_switch<std::optional<state>>(sv)
                                 .match("mode", state::mode)
                                 .match("subject", state::subject)
                                 .default_match(std::nullopt)};
        return s.has_value() && std::exchange(_state, *s) == state::object;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        if (_state == state::mode) {
            auto s = from_string_view<mode>(sv);
            if (s.has_value()) {
                result.mode = *s;
                _state = state::object;
            }
            return s.has_value();
        } else if (_state == state::subject) {
            result.sub.emplace(sv);
            _state = state::object;
            return true;
        }
        return false;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(::json::SizeType) {
        return std::exchange(_state, state::empty) == state::object;
    }
};

struct delete_subject_key {
    static constexpr topic_key_type keytype{topic_key_type::delete_subject};
    std::optional<model::offset> seq;
    std::optional<model::node_id> node;
    subject sub;
    topic_key_magic magic{0};

    friend bool operator==(const delete_subject_key&, const delete_subject_key&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const delete_subject_key& v) {
        if (v.seq.has_value() && v.node.has_value()) {
            fmt::print(
              os,
              "seq: {}, node: {}, keytype: {}, subject: {}, magic: {}",
              *v.seq,
              *v.node,
              to_string_view(v.keytype),
              v.sub,
              v.magic);
        } else {
            fmt::print(
              os,
              "unsequenced keytype: {}, subject: {}, magic: {}",
              to_string_view(v.keytype),
              v.sub,
              v.magic);
        }
        return os;
    }
};

template<typename Buffer>
void rjson_serialize(::json::Writer<Buffer>& w, const delete_subject_key& key) {
    w.StartObject();
    w.Key("keytype");
    ::json::rjson_serialize(w, to_string_view(key.keytype));
    w.Key("subject");
    ::json::rjson_serialize(w, key.sub());
    w.Key("magic");
    ::json::rjson_serialize(w, key.magic);
    if (key.seq.has_value()) {
        w.Key("seq");
        ::json::rjson_serialize(w, key.seq);
    }
    if (key.node.has_value()) {
        w.Key("node");
        ::json::rjson_serialize(w, key.node);
    }
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
class delete_subject_key_handler : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        seq,
        node,
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

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::object: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("keytype", state::keytype)
                                     .match("subject", state::subject)
                                     .match("magic", state::magic)
                                     .match("seq", state::seq)
                                     .match("node", state::node)
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
        case state::seq:
        case state::node:
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
        case state::seq: {
            result.seq = model::offset{i};
            _state = state::object;
            return true;
        }
        case state::node: {
            result.node = model::node_id{i};
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

    bool String(const Ch* str, ::json::SizeType len, bool) {
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
        case state::seq:
        case state::node:
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

    bool EndObject(::json::SizeType) {
        return result.seq.has_value() == result.node.has_value()
               && std::exchange(_state, state::empty) == state::object;
    }
};

struct delete_subject_value {
    subject sub;

    friend bool
    operator==(const delete_subject_value&, const delete_subject_value&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const delete_subject_value& v) {
        fmt::print(os, "subject: {}", v.sub);
        return os;
    }
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const delete_subject_value& val) {
    w.StartObject();
    w.Key("subject");
    ::json::rjson_serialize(w, val.sub);
    w.EndObject();
}

template<typename Encoding = ::json::UTF8<>>
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

    bool Key(const Ch* str, ::json::SizeType len, bool) {
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

    bool String(const Ch* str, ::json::SizeType len, bool) {
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

    bool Uint(int) {
        switch (_state) {
        case state::version: {
            // version ignored
            _state = state::object;
            return true;
        }
        case state::subject:
        case state::empty:
        case state::object:
            return false;
        }
        return false;
    }

    bool StartObject() {
        return std::exchange(_state, state::object) == state::empty;
    }

    bool EndObject(::json::SizeType) {
        return std::exchange(_state, state::empty) == state::object;
    }
};

template<typename Handler, typename... Args>
auto from_json_iobuf(iobuf&& iobuf, Args&&... args) {
    return json::rjson_parse(
      std::move(iobuf), Handler{std::forward<Args>(args)...});
}

template<typename T>
auto to_json_iobuf(T&& t) {
    return json::rjson_serialize_iobuf(std::forward<T>(t));
}

template<typename Key, typename Value>
model::record_batch as_record_batch(Key key, Value val) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};
    rb.add_raw_kv(to_json_iobuf(std::move(key)), to_json_iobuf(std::move(val)));
    return std::move(rb).build();
}

struct consume_to_store {
    explicit consume_to_store(sharded_store& s, seq_writer& seq)
      : _store{s}
      , _sequencer(seq) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        if (!b.header().attrs.is_control()) {
            b = co_await storage::internal::decompress_batch(std::move(b));
            auto base_offset = b.base_offset();
            co_await model::for_each_record(
              b, [this, base_offset](model::record& rec) {
                  auto offset = base_offset + model::offset(rec.offset_delta());
                  return (*this)(std::move(rec), offset);
              });
        }
        co_return ss::stop_iteration::no;
    }

    ss::future<> operator()(model::record record, model::offset offset) {
        auto key = record.release_key();
        auto key_type_str = from_json_iobuf<topic_key_type_handler<>>(
          key.share(0, key.size_bytes()));

        auto key_type = from_string_view<topic_key_type>(key_type_str);
        if (!key_type.has_value()) {
            vlog(plog.error, "Ignoring keytype: {}", key_type_str);
            co_await _sequencer.advance_offset(offset);
            co_return;
        }

        switch (*key_type) {
        case topic_key_type::noop:
            break;
        case topic_key_type::schema: {
            std::optional<unparsed_schema_value> val;
            if (!record.value().empty()) {
                val.emplace(from_json_iobuf<unparsed_schema_value_handler<>>(
                  record.release_value()));
            }
            co_await apply(
              offset,
              from_json_iobuf<schema_key_handler<>>(std::move(key)),
              std::move(val));
            break;
        }
        case topic_key_type::config: {
            std::optional<config_value> val;
            if (!record.value().empty()) {
                auto value = record.release_value();
                val.emplace(
                  from_json_iobuf<config_value_handler<>>(std::move(value)));
            }
            co_await apply(
              offset,
              from_json_iobuf<config_key_handler<>>(std::move(key)),
              val);
            break;
        }
        case topic_key_type::mode: {
            std::optional<mode_value> val;
            if (!record.value().empty()) {
                auto value = record.release_value();
                val.emplace(
                  from_json_iobuf<mode_value_handler<>>(std::move(value)));
            }
            co_await apply(
              offset, from_json_iobuf<mode_key_handler<>>(std::move(key)), val);
            break;
        }
        case topic_key_type::delete_subject: {
            std::optional<delete_subject_value> val;
            if (!record.value().empty()) {
                val.emplace(from_json_iobuf<delete_subject_value_handler<>>(
                  record.release_value()));
            }

            co_await apply(
              offset,
              from_json_iobuf<delete_subject_key_handler<>>(std::move(key)),
              std::move(val));
            break;
        }
        }

        co_await _sequencer.advance_offset(offset);
    }

    template<typename Tag>
    ss::future<> apply(
      model::offset offset,
      schema_key key,
      std::optional<schema_value<Tag>> val) {
        if (key.magic != 0 && key.magic != 1) {
            throw exception(
              error_code::topic_parse_error,
              fmt::format("Unexpected magic: {}", key));
        }

        // Out-of-place events happen when two writers collide.  First
        // writer wins: disregard subsequent events whose seq field
        // doesn't match their actually offset.  Check is only applied
        // for messages with values, not tombstones.
        //
        // Check seq if it was provided, otherwise assume 3rdparty
        // compatibility, which can't collide.
        if (val && key.seq.has_value() && offset != key.seq) {
            vlog(
              plog.debug,
              "Ignoring out of order {} (at offset {})",
              key,
              offset);
            co_return;
        }

        try {
            vlog(
              plog.debug,
              "Applying: {} tombstone={} (at offset {})",
              key,
              !val.has_value(),
              offset);
            if (!val) {
                try {
                    co_await _store.delete_subject_version(
                      key.sub, key.version, force::yes);
                } catch (exception& e) {
                    // This is allowed to throw not_found errors.  When we
                    // tombstone all the records referring to a particular
                    // version, we will see more than one get applied, and
                    // after the first one, the rest will not find it.
                    if (
                      e.code() == error_code::subject_not_found
                      || e.code() == error_code::subject_version_not_found) {
                        vlog(
                          plog.debug,
                          "Ignoring tombstone at offset={}, subject or version "
                          "already removed ({})",
                          offset,
                          key);
                    } else {
                        throw;
                    }
                }
            } else {
                co_await _store.upsert(
                  seq_marker{
                    .seq = key.seq,
                    .node = key.node,
                    .version = val->version,
                    .key_type = seq_marker_key_type::schema},
                  std::move(val->schema),
                  val->id,
                  val->version,
                  val->deleted);
            }
        } catch (const exception& e) {
            vlog(plog.debug, "Error replaying: {}: {}", key, e.what());
        }
    }

    ss::future<> apply(
      model::offset offset, config_key key, std::optional<config_value> val) {
        // Drop out-of-sequence messages
        //
        // Check seq if it was provided, otherwise assume 3rdparty
        // compatibility, which can't collide.
        if (val && key.seq.has_value() && offset != key.seq) {
            vlog(
              plog.debug,
              "Ignoring out of order {} (at offset {})",
              key,
              offset);
            co_return;
        }

        if (key.magic != 0) {
            throw exception(
              error_code::topic_parse_error,
              fmt::format("Unexpected magic: {}", key));
        }
        try {
            vlog(plog.debug, "Applying: {}", key);
            if (key.sub.has_value()) {
                if (!val.has_value()) {
                    co_await _store.clear_compatibility(
                      seq_marker{
                        .seq = key.seq,
                        .node = key.node,
                        .version{invalid_schema_version}, // Not applicable
                        .key_type = seq_marker_key_type::config},
                      *key.sub);
                } else {
                    co_await _store.set_compatibility(
                      seq_marker{
                        .seq = key.seq,
                        .node = key.node,
                        .version{invalid_schema_version}, // Not applicable
                        .key_type = seq_marker_key_type::config},
                      *key.sub,
                      val->compat);
                }
            } else if (val.has_value()) {
                co_await _store.set_compatibility(val->compat);
            } else {
                vlog(
                  plog.warn,
                  "Tried to apply config with neither subject nor value");
            }
        } catch (const exception& e) {
            vlog(plog.debug, "Error replaying: {}: {}", key, e);
        }
    }

    ss::future<>
    apply(model::offset offset, mode_key key, std::optional<mode_value> val) {
        // Drop out-of-sequence messages
        //
        // Check seq if it was provided, otherwise assume 3rdparty
        // compatibility, which can't collide.
        if (val && key.seq.has_value() && offset != key.seq) {
            vlog(
              plog.debug,
              "Ignoring out of order {} (at offset {})",
              key,
              offset);
            co_return;
        }

        if (key.magic != 0) {
            throw exception(
              error_code::topic_parse_error,
              fmt::format("Unexpected magic: {}", key));
        }
        try {
            vlog(plog.debug, "Applying: {}", key);
            if (key.sub.has_value()) {
                if (!val.has_value()) {
                    co_await _store.clear_mode(
                      seq_marker{
                        .seq = key.seq,
                        .node = key.node,
                        .version{invalid_schema_version}, // Not applicable
                        .key_type = seq_marker_key_type::mode},
                      *key.sub,
                      force::yes);
                } else {
                    co_await _store.set_mode(
                      seq_marker{
                        .seq = key.seq,
                        .node = key.node,
                        .version{invalid_schema_version}, // Not applicable
                        .key_type = seq_marker_key_type::mode},
                      *key.sub,
                      val->mode,
                      force::yes);
                }
            } else if (val.has_value()) {
                co_await _store.set_mode(val->mode, force::yes);
            } else {
                vlog(
                  plog.warn,
                  "Tried to apply mode with neither subject nor value");
            }
        } catch (const exception& e) {
            vlog(plog.debug, "Error replaying: {}: {}", key, e);
        }
    }

    ss::future<> apply(
      model::offset offset,
      delete_subject_key key,
      std::optional<delete_subject_value> val) {
        // Out-of-place events happen when two writers collide.  First
        // writer wins: disregard subsequent events whose seq field
        // doesn't match their actually offset.
        //
        // Check seq if it was provided, otherwise assume 3rdparty
        // compatibility, which can't collide.
        if (val && key.seq.has_value() && offset != key.seq) {
            vlog(
              plog.debug,
              "Ignoring out of order {} (at offset {})",
              key,
              offset);
            co_return;
        }

        if (!val.has_value()) {
            // Tombstones for a delete_subject (soft deletion) aren't
            // meaningful, and only exist to release space in the topic. The
            // actual removal of subjects/versions happens on hard delete, i.e.
            // the tombstone for the schema/version itself, not the tombstone
            // for the soft deletion.
            vlog(plog.debug, "Ignoring delete_subject tombstone at {}", offset);
            co_return;
        }

        if (key.magic != 0) {
            throw exception(
              error_code::topic_parse_error,
              fmt::format("Unexpected magic: {}", key));
        }
        try {
            vlog(plog.debug, "Applying: {}", key);
            co_await _store.delete_subject(
              seq_marker{
                .seq = key.seq,
                .node = key.node,
                .version{invalid_schema_version}, // Not applicable
                .key_type = seq_marker_key_type::delete_subject},
              key.sub,
              permanent_delete::no);
        } catch (const exception& e) {
            vlog(plog.debug, "Error replaying: {}: {}", key, e);
        }
    }

    void end_of_stream() {}
    sharded_store& _store;
    seq_writer& _sequencer;
};

} // namespace pandaproxy::schema_registry
