/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/json.h"

#include "json/document.h"
#include "json/ostreamwrapper.h"
#include "json/schema.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>

#include <absl/container/inlined_vector.h>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rapidjson/error/en.h>

#include <numeric>
#include <string_view>
namespace pandaproxy::schema_registry {

struct json_schema_definition::impl {
    ss::sstring to_json() const {
        json::StringBuffer buf;
        json::Writer<json::StringBuffer> wrt(buf);
        doc.Accept(wrt);
        return {buf.GetString(), buf.GetLength()};
    }

    explicit impl(json::Document doc, std::string_view name)
      : doc{std::move(doc)}
      , name{name} {}

    json::Document doc;
    ss::sstring name;
};

bool operator==(
  const json_schema_definition& lhs, const json_schema_definition& rhs) {
    return lhs.raw() == rhs.raw();
}

std::ostream& operator<<(std::ostream& os, const json_schema_definition& def) {
    fmt::print(
      os,
      "type: {}, definition: {}",
      to_string_view(def.type()),
      def().to_json());
    return os;
}

canonical_schema_definition::raw_string json_schema_definition::raw() const {
    return canonical_schema_definition::raw_string{_impl->to_json()};
}

ss::sstring json_schema_definition::name() const { return {_impl->name}; };

namespace {

// from https://json-schema.org/draft-04/schema, this is used to meta-validate a
// jsonschema
constexpr std::string_view json_draft_4_metaschema = R"json(
{
    "id": "http://json-schema.org/draft-04/schema#",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Core schema meta-schema",
    "definitions": {
        "schemaArray": {
            "type": "array",
            "minItems": 1,
            "items": { "$ref": "#" }
        },
        "positiveInteger": {
            "type": "integer",
            "minimum": 0
        },
        "positiveIntegerDefault0": {
            "allOf": [ { "$ref": "#/definitions/positiveInteger" }, { "default": 0 } ]
        },
        "simpleTypes": {
            "enum": [ "array", "boolean", "integer", "null", "number", "object", "string" ]
        },
        "stringArray": {
            "type": "array",
            "items": { "type": "string" },
            "minItems": 1,
            "uniqueItems": true
        }
    },
    "type": "object",
    "properties": {
        "id": {
            "type": "string"
        },
        "$schema": {
            "type": "string",
            "enum": ["http://json-schema.org/draft-04/schema#"]
        },
        "title": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "default": {},
        "multipleOf": {
            "type": "number",
            "minimum": 0,
            "exclusiveMinimum": true
        },
        "maximum": {
            "type": "number"
        },
        "exclusiveMaximum": {
            "type": "boolean",
            "default": false
        },
        "minimum": {
            "type": "number"
        },
        "exclusiveMinimum": {
            "type": "boolean",
            "default": false
        },
        "maxLength": { "$ref": "#/definitions/positiveInteger" },
        "minLength": { "$ref": "#/definitions/positiveIntegerDefault0" },
        "pattern": {
            "type": "string",
            "format": "regex"
        },
        "additionalItems": {
            "anyOf": [
                { "type": "boolean" },
                { "$ref": "#" }
            ],
            "default": {}
        },
        "items": {
            "anyOf": [
                { "$ref": "#" },
                { "$ref": "#/definitions/schemaArray" }
            ],
            "default": {}
        },
        "maxItems": { "$ref": "#/definitions/positiveInteger" },
        "minItems": { "$ref": "#/definitions/positiveIntegerDefault0" },
        "uniqueItems": {
            "type": "boolean",
            "default": false
        },
        "maxProperties": { "$ref": "#/definitions/positiveInteger" },
        "minProperties": { "$ref": "#/definitions/positiveIntegerDefault0" },
        "required": { "$ref": "#/definitions/stringArray" },
        "additionalProperties": {
            "anyOf": [
                { "type": "boolean" },
                { "$ref": "#" }
            ],
            "default": {}
        },
        "definitions": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "properties": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "patternProperties": {
            "type": "object",
            "additionalProperties": { "$ref": "#" },
            "default": {}
        },
        "dependencies": {
            "type": "object",
            "additionalProperties": {
                "anyOf": [
                    { "$ref": "#" },
                    { "$ref": "#/definitions/stringArray" }
                ]
            }
        },
        "enum": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": true
        },
        "type": {
            "anyOf": [
                { "$ref": "#/definitions/simpleTypes" },
                {
                    "type": "array",
                    "items": { "$ref": "#/definitions/simpleTypes" },
                    "minItems": 1,
                    "uniqueItems": true
                }
            ]
        },
        "format": { "type": "string" },
        "allOf": { "$ref": "#/definitions/schemaArray" },
        "anyOf": { "$ref": "#/definitions/schemaArray" },
        "oneOf": { "$ref": "#/definitions/schemaArray" },
        "not": { "$ref": "#" }
    },
    "dependencies": {
        "exclusiveMaximum": [ "maximum" ],
        "exclusiveMinimum": [ "minimum" ]
    },
    "default": {}
}
)json";

result<json::Document> parse_json(std::string_view v) {
    // validation pre-step: compile metaschema for json draft
    static const auto metaschema_doc = [] {
        auto metaschema_json = json::Document{};
        metaschema_json.Parse(
          json_draft_4_metaschema.data(), json_draft_4_metaschema.size());
        vassert(
          !metaschema_json.HasParseError(), "Malformed metaschema document");

        return json::SchemaDocument{metaschema_json};
    }();

    // validation of schema: validate it against metaschema
    // first construct a reader that validates the schema against the metaschema
    // while parsing it
    auto schema_stream = rapidjson::MemoryStream{v.data(), v.size()};
    auto validating_reader
      = json::SchemaValidatingReader<rapidjson::MemoryStream>{
        schema_stream, metaschema_doc};

    // then parse schema to json
    auto schema_json = json::Document{};
    schema_json.Populate(validating_reader);

    if (auto parse_res = validating_reader.GetParseResult();
        parse_res.IsError()) {
        // schema_json is either not a json document
        // or it's not a valid json according to metaschema

        // Check the validation result
        if (!validating_reader.IsValid()) {
            // not a valid schema draft4 according to metaschema. retrieve some
            // info and return error
            auto error_loc_metaschema = json::StringBuffer{};
            auto error_loc_schema = json::StringBuffer{};
            validating_reader.GetInvalidSchemaPointer().StringifyUriFragment(
              error_loc_metaschema);
            validating_reader.GetInvalidDocumentPointer().StringifyUriFragment(
              error_loc_schema);
            auto invalid_keyword = validating_reader.GetInvalidSchemaKeyword();

            return error_info{
              error_code::schema_invalid,
              fmt::format(
                "Invalid json schema: '{}', invalid metaschema: '{}', invalid "
                "keyword: '{}'",
                std::string_view{
                  error_loc_schema.GetString(), error_loc_schema.GetLength()},
                std::string_view{
                  error_loc_metaschema.GetString(),
                  error_loc_metaschema.GetLength()},
                invalid_keyword)};
        } else {
            // not a valid json document, return error
            return error_info{
              error_code::schema_invalid,
              fmt::format(
                "Malformed json schema: {} at offset {}",
                rapidjson::GetParseError_En(parse_res.Code()),
                parse_res.Offset())};
        }
    }

    // schema_json is a valid json and a syntactically valid json schema draft4.
    // TODO AB cross validate "$ref" fields, this is not done automatically

    return {std::move(schema_json)};
}

/// is_superset section

// a schema O is a superset of another schema N if every schema that is valid
// for N is also valid for O. precondition: older and newer are both valid
// schemas
bool is_superset(json::Value const& older, json::Value const& newer);

// close the implementation in a namespace to keep it contained
namespace is_superset_impl {

// helper struct to format json::Value
struct pj {
    json::Value const& v;
    friend std::ostream& operator<<(std::ostream& os, pj const& p) {
        auto osw = json::OStreamWrapper{os};
        auto writer = json::Writer<json::OStreamWrapper>{osw};
        p.v.Accept(writer);
        return os;
    }
};

enum class json_type : uint8_t {
    string = 0,
    integer = 1,
    number = 2,
    object = 3,
    array = 4,
    boolean = 5,
    null = 6
};
// enough inlined space to hold all the values of json_type
using json_type_list = absl::InlinedVector<json_type, 7>;

constexpr std::string_view to_string_view(json_type t) {
    switch (t) {
    case json_type::string:
        return "string";
    case json_type::integer:
        return "integer";
    case json_type::number:
        return "number";
    case json_type::object:
        return "object";
    case json_type::array:
        return "array";
    case json_type::boolean:
        return "boolean";
    case json_type::null:
        return "null";
    }
}

constexpr std::optional<json_type> from_string_view(std::string_view v) {
    return string_switch<std::optional<json_type>>(v)
      .match(to_string_view(json_type::string), json_type::string)
      .match(to_string_view(json_type::integer), json_type::integer)
      .match(to_string_view(json_type::number), json_type::number)
      .match(to_string_view(json_type::object), json_type::object)
      .match(to_string_view(json_type::array), json_type::array)
      .match(to_string_view(json_type::boolean), json_type::boolean)
      .match(to_string_view(json_type::null), json_type::null)
      .default_match(std::nullopt);
}

constexpr auto parse_json_type(json::Value const& v) {
    std::string_view sv{v.GetString(), v.GetStringLength()};
    auto type = from_string_view(sv);
    if (!type) {
        throw as_exception(error_info{
          error_code::schema_invalid,
          fmt::format("Invalid JSON Schema type: '{}'", sv)});
    }
    return *type;
}

// parse None | schema_type | array[schema_type] into a set of types.
// the return type is implemented as a inlined_vector<json_type> with sorted set
// semantics
json_type_list normalized_type(json::Value const& v) {
    auto type_it = v.FindMember("type");
    auto ret = json_type_list{};
    if (type_it == v.MemberEnd()) {
        // omit keyword is like accepting all the types
        ret = {
          json_type::string,
          json_type::integer,
          json_type::number,
          json_type::object,
          json_type::array,
          json_type::boolean,
          json_type::null};
    } else if (type_it->value.IsArray()) {
        // schema ensures that all the values are unique
        for (auto& v : type_it->value.GetArray()) {
            ret.push_back(parse_json_type(v));
        }
    } else {
        ret.push_back(parse_json_type(type_it->value));
    }

    // to support set difference operations, sort the elements
    std::ranges::sort(ret);
    return ret;
}

bool is_string_superset(
  [[maybe_unused]] json::Value const& older,
  [[maybe_unused]] json::Value const& newer) {
    throw as_exception(invalid_schema(fmt::format(
      "{} not implemented. input: older: '{}', newer: '{}'",
      __FUNCTION__,
      pj{older},
      pj{newer})));
}
bool is_numeric_superset(
  [[maybe_unused]] json::Value const& older,
  [[maybe_unused]] json::Value const& newer) {
    throw as_exception(invalid_schema(fmt::format(
      "{} not implemented. input: older: '{}', newer: '{}'",
      __FUNCTION__,
      pj{older},
      pj{newer})));
}
bool is_array_superset(
  [[maybe_unused]] json::Value const& older,
  [[maybe_unused]] json::Value const& newer) {
    throw as_exception(invalid_schema(fmt::format(
      "{} not implemented. input: older: '{}', newer: '{}'",
      __FUNCTION__,
      pj{older},
      pj{newer})));
}
bool is_object_superset(
  [[maybe_unused]] json::Value const& older,
  [[maybe_unused]] json::Value const& newer) {
    throw as_exception(invalid_schema(fmt::format(
      "{} not implemented. input: older: '{}', newer: '{}'",
      __FUNCTION__,
      pj{older},
      pj{newer})));
}

} // namespace is_superset_impl

using namespace is_superset_impl;

// a schema O is a superset of another schema N if every schema that is valid
// for N is also valid for O. precondition: older and newer are both valid
// schemas
bool is_superset(json::Value const& older, json::Value const& newer) {
    // extract { "type" : ... }
    auto older_types = normalized_type(older);
    auto newer_types = normalized_type(newer);

    // looking for types that are new in `newer`. done as newer_types
    // \ older_types
    auto newer_minus_older = json_type_list{};
    std::ranges::set_difference(
      newer_types, older_types, std::back_inserter(newer_minus_older));
    if (
      !newer_minus_older.empty()
      && !(
        newer_minus_older == json_type_list{json_type::integer}
        && std::ranges::count(older_types, json_type::number) != 0)) {
        // newer_types_not_in_older accepts integer, and we can accept an
        // evolution from number -> integer. everything else is makes `newer`
        // less strict than older
        return false;
    }

    // newer accepts less (or equal) types. for each type, try to find a less
    // strict check
    for (auto t : newer_types) {
        // TODO this will perform a depth first search, but it might be better
        // to do a breadth first search to find a counterexample
        switch (t) {
        case json_type::string:
            if (!is_string_superset(older, newer)) {
                return false;
            }
            break;
        case json_type::integer:
            [[fallthrough]];
        case json_type::number:
            if (!is_numeric_superset(older, newer)) {
                return false;
            }
            break;
        case json_type::object:
            if (!is_object_superset(older, newer)) {
                return false;
            }
            break;
        case json_type::array:
            if (!is_array_superset(older, newer)) {
                return false;
            }
            break;
        case json_type::boolean:
            // no check needed for boolean;
            break;
        case json_type::null:
            // no check needed for null;
            break;
        }
    }

    for (auto not_yet_handled_keyword : {
           "id",
           "$schema",
           "title",
           "description",
           "default",
           "multipleOf",
           "maximum",
           "exclusiveMaximum",
           "minimum",
           "exclusiveMinimum",
           "maxLength",
           "minLength",
           "pattern",
           "additionalItems",
           "items",
           "maxItems",
           "minItems",
           "uniqueItems",
           "maxProperties",
           "minProperties",
           "required",
           "additionalProperties",
           "definitions",
           "properties",
           "patternProperties",
           "dependencies",
           "enum",
           "format",
           "allOf",
           "anyOf",
           "oneOf",
           "not",
         }) {
        if (
          newer.HasMember(not_yet_handled_keyword)
          || older.HasMember(not_yet_handled_keyword)) {
            // these keyword are not yet handled, their presence might change
            // the result of this function
            throw as_exception(invalid_schema(fmt::format(
              "{} not fully implemented yet. unsupported keyword: {}, input: "
              "older: '{}', newer: '{}'",
              __FUNCTION__,
              not_yet_handled_keyword,
              pj{older},
              pj{newer})));
        }
    }

    // no rule in newer is less strict than older, older is superset of newer
    return true;
}

} // namespace

ss::future<json_schema_definition>
make_json_schema_definition(sharded_store&, canonical_schema schema) {
    auto doc = parse_json(schema.def().raw()()).value(); // throws on error
    std::string_view name = schema.sub()();
    auto refs = std::move(schema).def().refs();
    co_return json_schema_definition{
      ss::make_shared<json_schema_definition::impl>(std::move(doc), name),
      std::move(refs)};
}

ss::future<canonical_schema>
make_canonical_json_schema(sharded_store&, unparsed_schema def) {
    // TODO BP: More validation and normalisation
    parse_json(def.def().raw()()).value(); // throws on error
    co_return canonical_schema{
      def.sub(), canonical_schema_definition{def.def().raw(), def.type()}};
}

bool check_compatible(
  const json_schema_definition& reader, const json_schema_definition& writer) {
    // reader is a superset of writer iff every schema that is valid for writer
    // is also valid for reader
    return is_superset(reader().doc, writer().doc);
}

} // namespace pandaproxy::schema_registry
