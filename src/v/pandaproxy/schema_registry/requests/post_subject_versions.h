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

#include "outcome.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/util.h"
#include "seastarx.h"
#include "utils/string_switch.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <atomic>
#include <cstdint>

namespace pandaproxy::schema_registry {

struct post_subject_versions_request {
    struct schema_reference {
        ss::sstring name;
        subject sub{invalid_subject};
        schema_version version{invalid_schema_version};
    };

    struct body {
        schema_definition schema{invalid_schema_definition};
        schema_type type{schema_type::avro};
        std::vector<schema_reference> references;
    };

    subject sub;
    body payload;
};

template<typename Encoding = rapidjson::UTF8<>>
class post_subject_versions_request_handler
  : public json::base_handler<Encoding> {
    enum class state {
        empty = 0,
        record,
        schema,
        schema_type,
        references,
        reference,
        reference_name,
        reference_subject,
        reference_version,
    };
    state _state = state::empty;

public:
    using Ch = typename json::base_handler<Encoding>::Ch;
    using rjson_parse_result = post_subject_versions_request::body;
    rjson_parse_result result;

    post_subject_versions_request_handler()
      : json::base_handler<Encoding>{json::serialization_format::none} {}

    bool Key(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::record: {
            std::optional<state> s{string_switch<std::optional<state>>(sv)
                                     .match("schema", state::schema)
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
        case state::reference_version: {
            result.references.back().version = schema_version{i};
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

    bool String(const Ch* str, rapidjson::SizeType len, bool) {
        auto sv = std::string_view{str, len};
        switch (_state) {
        case state::schema: {
            auto def = make_schema_definition<Encoding>(sv);
            if (!def) {
                return false;
            }
            result.schema = std::move(def).value();
            _state = state::record;
            return true;
        }
        case state::schema_type: {
            auto type = from_string_view<schema_type>(sv);
            if (type.has_value()) {
                result.type = *type;
                _state = state::record;
            }
            return type.has_value();
        }
        case state::reference_name: {
            result.references.back().name = ss::sstring{sv};
            _state = state::reference;
            return true;
        }
        case state::reference_subject: {
            result.references.back().sub = subject{ss::sstring{sv}};
            _state = state::reference;
            return true;
        }
        case state::empty:
        case state::record:
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
            result.references.emplace_back();
            _state = state::reference;
            return true;
        }
        case state::record:
        case state::schema:
        case state::schema_type:
        case state::reference:
        case state::reference_name:
        case state::reference_subject:
        case state::reference_version:
            return false;
        }
        return false;
    }

    bool EndObject(rapidjson::SizeType) {
        switch (_state) {
        case state::record: {
            _state = state::empty;
            return !result.schema().empty();
        }
        case state::reference: {
            _state = state::references;
            const auto& reference{result.references.back()};
            return !reference.name.empty() && reference.sub != invalid_subject
                   && reference.version != invalid_schema_version;
        }
        case state::empty:
        case state::schema:
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

    bool EndArray(rapidjson::SizeType) {
        return std::exchange(_state, state::record) == state::references;
    }
};

struct post_subject_versions_response {
    schema_id id;
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const schema_registry::post_subject_versions_response& res) {
    w.StartObject();
    w.Key("id");
    ::json::rjson_serialize(w, res.id);
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
