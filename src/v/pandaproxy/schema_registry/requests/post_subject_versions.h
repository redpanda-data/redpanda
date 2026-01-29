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

#include "base/seastarx.h"
#include "json/types.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/schema_registry/rjson.h"
#include "ssx/sformat.h"
#include "strings/string_switch.h"

#include <seastar/core/sstring.hh>

#include <utility>

namespace pandaproxy::schema_registry {

struct post_subject_versions_request {
    subject_schema schema;
};

template<typename Encoding = ::json::UTF8<>>
class post_subject_versions_request_handler
  : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        record,
        schema,
        id,
        version,
        metadata,
        metadata_properties,
        metadata_property,
        ruleset,
        schema_type,
        references,
        reference,
        reference_name,
        reference_subject,
        reference_version,
    };
    state _state = state::empty;

    struct mutable_schema {
        context_subject sub{invalid_subject};
        schema_definition::raw_string def;
        schema_type type{schema_type::avro};
        schema_definition::references refs;
        std::optional<schema_metadata> metadata;
    };
    ss::sstring metadata_property_key;
    mutable_schema _schema;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    struct rjson_parse_result {
        subject_schema def;
        std::optional<schema_id> id;
        std::optional<schema_version> version;
    };
    rjson_parse_result result;

    explicit post_subject_versions_request_handler(context_subject sub)
      : json::base_handler<Encoding>{json::serialization_format::none}
      , _schema{std::move(sub)} {}

    bool Key(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::record: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("schema", state::schema)
                                     .match("id", state::id)
                                     .match("version", state::version)
                                     .match("metadata", state::metadata)
                                     .match("ruleSet", state::ruleset)
                                     .match("schemaType", state::schema_type)
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
        case state::metadata: {
            std::optional<state> s{
              string_switch<std::optional<state>>(sv)
                .match("properties", state::metadata_properties)
                .default_match(std::nullopt)};
            if (s.has_value()) {
                _state = *s;
            }
            return s.has_value();
        }
        case state::metadata_property: {
            metadata_property_key = ss::sstring{sv};
            return true;
        }
        case state::empty:
        case state::schema:
        case state::id:
        case state::version:
        case state::ruleset:
        case state::schema_type:
        case state::references:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
        case state::metadata_properties:
            return false;
        }
        return false;
    }

    bool Null() {
        switch (_state) {
        case state::metadata:
        case state::ruleset:
            _state = state::record;
            return true;
        case state::metadata_properties:
            _state = state::metadata;
            return true;
        case state::empty:
        case state::record:
        case state::schema:
        case state::id:
        case state::version:
        case state::schema_type:
        case state::references:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
        case state::metadata_property:
            return false;
        }
        return false;
    }

    bool Bool(bool b) {
        switch (_state) {
        case state::metadata_property: {
            _schema.metadata->properties->insert_or_assign(
              std::move(metadata_property_key), ssx::sformat("{}", b));
            return true;
        }
        case state::empty:
        case state::record:
        case state::schema:
        case state::id:
        case state::version:
        case state::schema_type:
        case state::references:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
        case state::metadata:
        case state::metadata_properties:
        case state::ruleset:
            return false;
        }
        return false;
    }

    bool Int(int i) { return set_integer_value(i); }

    bool Uint(unsigned i) {
        if (i > std::numeric_limits<int>::max()) {
            return false;
        }
        return set_integer_value(static_cast<int>(i));
    }

    bool Double(double d) {
        switch (_state) {
        case state::metadata_property: {
            _schema.metadata->properties->insert_or_assign(
              std::move(metadata_property_key), ssx::sformat("{}", d));
            return true;
        }
        case state::empty:
        case state::record:
        case state::schema:
        case state::id:
        case state::version:
        case state::schema_type:
        case state::references:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
        case state::metadata:
        case state::ruleset:
        case state::metadata_properties:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::schema: {
            iobuf buf;
            buf.append(sv.data(), sv.size());
            _schema.def = schema_definition::raw_string{std::move(buf)};
            _state = state::record;
            return true;
        }
        case state::schema_type: {
            auto type = from_string_view<schema_type>(sv);
            if (type.has_value()) {
                _schema.type = *type;
                _state = state::record;
            }
            return type.has_value();
        }
        case state::reference_name: {
            _schema.refs.back().name = ss::sstring{sv};
            _state = state::reference;
            return true;
        }
        case state::reference_subject: {
            _schema.refs.back().sub = context_subject::from_string(sv);
            _state = state::reference;
            return true;
        }
        case state::metadata_property: {
            _schema.metadata->properties->insert_or_assign(
              std::move(metadata_property_key), ss::sstring{sv});
            return true;
        }
        case state::empty:
        case state::record:
        case state::id:
        case state::version:
        case state::metadata:
        case state::ruleset:
        case state::references:
        case state::reference:
        case state::reference_version:
        case state::metadata_properties:
            return false;
        }
        return false;
    }

    bool StartObject() {
        switch (_state) {
        case state::empty: {
            _state = state::record;
            return true;
        }
        case state::references: {
            _schema.refs.emplace_back();
            _state = state::reference;
            return true;
        }
        case state::metadata: {
            _schema.metadata.emplace();
            return true;
        }
        case state::metadata_properties: {
            _schema.metadata->properties.emplace();
            _state = state::metadata_property;
            return true;
        }
        case state::record:
        case state::schema:
        case state::id:
        case state::version:
        case state::ruleset:
        case state::schema_type:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
        case state::metadata_property:
            return false;
        }
        return false;
    }

    bool EndObject(::json::SizeType) {
        switch (_state) {
        case state::record: {
            _state = state::empty;
            result.def = {
              std::move(_schema.sub),
              {std::move(_schema.def),
               _schema.type,
               std::move(_schema.refs),
               std::move(_schema.metadata)}};
            return true;
        }
        case state::reference: {
            _state = state::references;
            const auto& reference{_schema.refs.back()};
            return !reference.name.empty() && reference.sub != invalid_subject
                   && reference.version != invalid_schema_version;
        }
        case state::metadata: {
            _state = state::record;
            return true;
        }
        case state::metadata_properties:
        case state::metadata_property: {
            _state = state::metadata;
            return true;
        }
        case state::empty:
        case state::schema:
        case state::id:
        case state::version:
        case state::ruleset:
        case state::schema_type:
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
        return std::exchange(_state, state::record) == state::references;
    }

private:
    bool set_integer_value(int32_t i) {
        switch (_state) {
        case state::id: {
            result.id = schema_id{i};
            _state = state::record;
            return true;
        }
        case state::version: {
            result.version = schema_version{i};
            _state = state::record;
            return true;
        }
        case state::reference_version: {
            _schema.refs.back().version = schema_version{i};
            _state = state::reference;
            return true;
        }
        case state::metadata_property: {
            _schema.metadata->properties->insert_or_assign(
              std::move(metadata_property_key), ssx::sformat("{}", i));
            return true;
        }
        case state::empty:
        case state::record:
        case state::schema:
        case state::metadata:
        case state::ruleset:
        case state::schema_type:
        case state::references:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::metadata_properties:
            return false;
        }
        return false;
    }
};

struct post_subject_response {
    subject_schema schema;
    schema_id id;
    schema_version version;
};

template<typename Buffer>
void rjson_serialize(
  ::json::iobuf_writer<Buffer>& w,
  const schema_registry::post_subject_response& res) {
    w.StartObject();
    w.Key("subject");
    w.String(res.schema.sub().to_string());
    w.Key("version");
    ::json::rjson_serialize(w, res.version);
    w.Key("id");
    ::json::rjson_serialize(w, res.id);
    w.Key("schemaType");
    ::json::rjson_serialize(w, to_string_view(res.schema.type()));
    if (!res.schema.def().refs().empty()) {
        w.Key("references");
        ::json::rjson_serialize(w, res.schema.def().refs());
    }
    ::json::rjson_serialize(w, res.schema.def().meta());
    w.Key("schema");
    ::json::rjson_serialize(w, res.schema.def().raw());
    w.EndObject();
}

struct post_subject_versions_response {
    schema_definition schema;
    schema_id id;
    schema_version version;
};

template<typename Buffer>
void rjson_serialize(
  ::json::iobuf_writer<Buffer>& w,
  const schema_registry::post_subject_versions_response& res) {
    w.StartObject();
    w.Key("id");
    ::json::rjson_serialize(w, res.id);
    w.Key("version");
    ::json::rjson_serialize(w, res.version);
    w.Key("schemaType");
    ::json::rjson_serialize(w, to_string_view(res.schema.type()));
    if (!res.schema.refs().empty()) {
        w.Key("references");
        ::json::rjson_serialize(w, res.schema.refs());
    }
    ::json::rjson_serialize(w, res.schema.meta());
    w.Key("schema");
    ::json::rjson_serialize(w, res.schema.raw());
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
