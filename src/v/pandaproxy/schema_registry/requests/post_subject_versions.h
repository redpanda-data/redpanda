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

#include "json/types.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/util.h"
#include "seastarx.h"
#include "utils/string_switch.h"

#include <seastar/core/sstring.hh>

namespace pandaproxy::schema_registry {

struct post_subject_versions_request {
    canonical_schema schema;
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
        schema_type,
        references,
        reference,
        reference_name,
        reference_subject,
        reference_version,
    };
    state _state = state::empty;

    struct mutable_schema {
        subject sub{invalid_subject};
        unparsed_schema_definition::raw_string def;
        schema_type type{schema_type::avro};
        unparsed_schema::references refs;
    };
    mutable_schema _schema;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    struct rjson_parse_result {
        unparsed_schema def;
        std::optional<schema_id> id;
        std::optional<schema_version> version;
    };
    rjson_parse_result result;

    explicit post_subject_versions_request_handler(subject sub)
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
        case state::empty:
        case state::schema:
        case state::id:
        case state::version:
        case state::schema_type:
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
        case state::empty:
        case state::record:
        case state::schema:
        case state::schema_type:
        case state::references:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
            return false;
        }
        return false;
    }

    bool String(const Ch* str, ::json::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::schema: {
            _schema.def = unparsed_schema_definition::raw_string{
              ss::sstring{sv}};
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
            _schema.refs.back().sub = subject{ss::sstring{sv}};
            _state = state::reference;
            return true;
        }
        case state::empty:
        case state::record:
        case state::id:
        case state::version:
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
            _state = state::record;
            return true;
        }
        case state::references: {
            _schema.refs.emplace_back();
            _state = state::reference;
            return true;
        }
        case state::record:
        case state::schema:
        case state::id:
        case state::version:
        case state::schema_type:
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
        case state::record: {
            _state = state::empty;
            result.def = {
              std::move(_schema.sub),
              {std::move(_schema.def), _schema.type},
              std::move(_schema.refs)};
            return true;
        }
        case state::reference: {
            _state = state::references;
            const auto& reference{_schema.refs.back()};
            return !reference.name.empty() && reference.sub != invalid_subject
                   && reference.version != invalid_schema_version;
        }
        case state::empty:
        case state::schema:
        case state::id:
        case state::version:
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
};

struct post_subject_versions_response {
    schema_id id;
};

inline void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w,
  const schema_registry::post_subject_versions_response& res) {
    w.StartObject();
    w.Key("id");
    ::json::rjson_serialize(w, res.id);
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
