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
#include <seastar/util/variant_utils.hh>

#include <absl/container/inlined_vector.h>
#include <boost/math/special_functions/relative_difference.hpp>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rapidjson/error/en.h>
#include <re2/re2.h>

#include <ranges>
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
    // TODO validate that "pattern" and "patternProperties" are valid regex
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

json::Value const& get_true_schema() {
    // A `true` schema is one that validates every possible input, it's literal
    // json value is `{}` Then `"additionalProperty": true` is equivalent to
    // `"additionalProperties": {}` it's used during mainly in
    // is_object_superset(older, newer) and in is_superset to short circuit
    // validation
    static auto true_schema = json::Value{rapidjson::kObjectType};
    return true_schema;
}

json::Value const& get_false_schema() {
    // A `false` schema is one that doesn't validate any input, it's literal
    // json value is `{"not": {}}`
    // `"additionalProperty": false` is equivalent to `"additionalProperties":
    // {"not": {}}` it's used during mainly in is_object_superset(older, newer)
    // and in is_superset to short circuit validation
    static auto false_schema = [] {
        auto tmp = json::Document{};
        tmp.Parse(R"({"not": {}})");
        vassert(!tmp.HasParseError(), "Malformed `false` json schema");
        return tmp;
    }();
    return false_schema;
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

// helper to retrieve the object value for a key, or an empty object if the key
// is not present
json::Value::ConstObject
get_object_or_empty(json::Value const& v, std::string_view key) {
    auto it = v.FindMember(
      json::Value{key.data(), rapidjson::SizeType(key.size())});
    if (it != v.MemberEnd()) {
        return it->value.GetObject();
    }

    static const auto empty_obj = json::Value{rapidjson::kObjectType};
    return empty_obj.GetObject();
}

// helper to retrieve the array value for a key, or an empty array if the key
// is not present
json::Value::ConstArray
get_array_or_empty(json::Value const& v, std::string_view key) {
    auto it = v.FindMember(
      json::Value{key.data(), rapidjson::SizeType(key.size())});
    if (it != v.MemberEnd()) {
        return it->value.GetArray();
    }

    static const auto empty_array = json::Value{rapidjson::kArrayType};
    return empty_array.GetArray();
}

// extract the Values pointed from older[prop_name] and newer[prop_name].
// returns a tuple of 3 value. the first is an optional<bool> that if has value
// can be used to short circuit later value checks. the other two values are
// pointers to the values.
// short circuit can happen if:
// 1. older has no value, then newer can either have it or not, but the result
// is always compatible
// 2. older has a value and newer does not have. then the result is always not
// compatible if no short circuit can happen, then the pointers are valid and
// can be dereferenced.
std::tuple<std::optional<bool>, json::Value const*, json::Value const*>
extract_property_and_gate_check(
  json::Value const& older,
  json::Value const& newer,
  std::string_view prop_name) {
    auto older_it = older.FindMember(
      json::Value{prop_name.data(), rapidjson::SizeType(prop_name.size())});
    auto newer_it = newer.FindMember(
      json::Value{prop_name.data(), rapidjson::SizeType(prop_name.size())});
    if (older_it == older.MemberEnd()) {
        // nothing in older, max freedom for newer (can be nothing too)
        return {true, nullptr, nullptr};
    }
    // older has value

    if (newer_it == newer.MemberEnd()) {
        // newer has no value, but older has it so they are not compatible
        return {false, nullptr, nullptr};
    }
    // both are value, need further checks

    return {std::nullopt, &older_it->value, &newer_it->value};
}

// helper for numeric property that fits into a double:
//  older  |  newer  | is_superset
// ------- | ------- | -----------
// nothing | nothing |    yes
// nothing |   __    |    yes
//  value  | nothing |    no
//  value  |  value  | is_same or predicate
template<typename VPred>
requires std::is_invocable_r_v<bool, VPred, double, double>
bool is_numeric_property_value_superset(
  json::Value const& older,
  json::Value const& newer,
  std::string_view prop_name,
  VPred&& value_predicate) {
    auto [maybe_is_compatible, older_val_p, newer_val_p]
      = extract_property_and_gate_check(older, newer, prop_name);
    if (maybe_is_compatible.has_value()) {
        return maybe_is_compatible.value();
    }

    // Gate on values that can't be represented with doubles.
    // rapidjson can serialize a uint64_t even thought it's not a widely
    // supported type, so deserializing that would trigger this. note also that
    // 0.1 is a valid json literal, but does not have an exact double
    // representation. this cannot be caught with this, and it would require
    // some sort of decimal type
    if (!older_val_p->IsLosslessDouble() || !newer_val_p->IsLosslessDouble()) {
        // both have value but they can't be decoded as T
        throw as_exception(invalid_schema(fmt::format(
          R"({}-{} not implemented for types [{},{}]. input: older: '{}', newer: '{}')",
          __FUNCTION__,
          prop_name,
          older_val_p->GetType(),
          newer_val_p->GetType(),
          pj{older},
          pj{newer})));
    }

    auto older_value = older_val_p->GetDouble();
    auto newer_value = newer_val_p->GetDouble();
    return older_value == newer_value
           || std::invoke(
             std::forward<VPred>(value_predicate), older_value, newer_value);
}

enum class additional_field_for { object, array };

bool is_additional_superset(
  json::Value const& older,
  json::Value const& newer,
  additional_field_for field_type) {
    // "additional___" can be either true (if omitted it's true), false
    // or a schema. The check is performed with this table.
    // older ap | newer ap | compatible
    // -------- | -------- | ----------
    //   true   |   ____   |    yes
    //   false  |   ____   | newer==false
    //  schema  |  schema  |  recurse
    //  schema  |   true   |  recurse with {}
    //  schema  |   false  |  recurse with {"not":{}}

    auto field_name = [&] {
        switch (field_type) {
        case additional_field_for::object:
            return "additionalProperties";
        case additional_field_for::array:
            return "additionalItems";
        }
    }();
    // helper to parse additional__
    auto get_additional_props =
      [&](json::Value const& v) -> std::variant<bool, json::Value const*> {
        auto it = v.FindMember(field_name);
        if (it == v.MemberEnd()) {
            return true;
        }
        if (it->value.IsBool()) {
            return it->value.GetBool();
        }
        return &it->value;
    };

    // poor man's case matching. this is an optimization in case both
    // additionalProperties are boolean
    return std::visit(
      ss::make_visitor(
        [](bool older, bool newer) {
            if (older == newer) {
                // same value is compatible
                return true;
            }
            // older=true  -> newer=false - compatible
            // older=false -> newer=true  - not compatible
            return older;
        },
        [](bool older, json::Value const* newer) {
            if (older) {
                // true is compatible with any schema
                return true;
            }
            // likely false, but need to check
            return is_superset(get_false_schema(), *newer);
        },
        [](json::Value const* older, bool newer) {
            if (!newer) {
                // any schema is compatible with false
                return true;
            }
            // convert newer to {} and check against that
            return is_superset(*older, get_true_schema());
        },
        [](json::Value const* older, json::Value const* newer) {
            // check subschemas for compatibility
            return is_superset(*older, *newer);
        }),
      get_additional_props(older),
      get_additional_props(newer));
}

bool is_string_superset(json::Value const& older, json::Value const& newer) {
    // note: "format" is not part of the checks
    if (!is_numeric_property_value_superset(
          older, newer, "minLength", std::less_equal<>{})) {
        // older is less strict
        return false;
    }
    if (!is_numeric_property_value_superset(
          older, newer, "maxLength", std::greater_equal<>{})) {
        // older is less strict
        return false;
    }

    auto [maybe_gate_value, older_val_p, newer_val_p]
      = extract_property_and_gate_check(older, newer, "pattern");
    if (maybe_gate_value.has_value()) {
        return maybe_gate_value.value();
    }

    // both have "pattern". check if they are the same, the only
    // possible_value_accepted
    auto older_pattern = std::string_view{
      older_val_p->GetString(), older_val_p->GetStringLength()};
    auto newer_pattern = std::string_view{
      newer_val_p->GetString(), newer_val_p->GetStringLength()};
    return older_pattern == newer_pattern;
}

bool is_numeric_superset(json::Value const& older, json::Value const& newer) {
    // preconditions:
    // newer["type"]=="number" implies older["type"]=="number"
    // older["type"]=="integer" implies newer["type"]=="integer"
    // if older["type"]=="number", then newer can be either "number" or
    // "integer"

    // note: in draft4, "exclusiveMinimum"/"exclusiveMaximum" are bool
    // indicating if "minimum"/"maximum" form an inclusive (default) or
    // exclusive range. in later drafts this was reworked and are now numeric
    // values so that "minimum" is always the inclusive limit and
    // "exclusiveMinimum" is always the exclusive range. in this check we
    // require for them to be the same datatype

    if (!is_numeric_property_value_superset(
          older, newer, "minimum", std::less_equal<>{})) {
        // older["minimum"] is not superset of newer["minimum"] because newer is
        // less strict
        return false;
    }
    if (!is_numeric_property_value_superset(
          older, newer, "maximum", std::greater_equal<>{})) {
        // older["maximum"] is not superset of newer["maximum"] because newer
        // is less strict
        return false;
    }

    if (!is_numeric_property_value_superset(
          older, newer, "multipleOf", [](double older, double newer) {
              // check that the reminder of newer/older is close enough to 0, as
              // in some multiples of epsilon.
              // TODO: this is an approximate check, if a bigdecimal
              // representation it would be possible to perform an exact
              // reminder(newer, older)==0 check
              return boost::math::epsilon_difference(
                       std::remainder(newer, older), 0.)
                     <= 10.;
          })) {
        return false;
    }

    // exclusiveMinimum/exclusiveMaximum checks are mostly the same logic,
    // implemented in this helper
    auto exclusive_limit_check = [](
                                   json::Value const& older,
                                   json::Value const& newer,
                                   std::string_view prop_name) {
        auto [maybe_gate_value, older_it, newer_it]
          = extract_property_and_gate_check(older, newer, prop_name);
        if (maybe_gate_value.has_value()) {
            return maybe_gate_value.value();
        }
        // need to perform checks on actual values
        if (older_it->IsBool() && newer_it->IsBool()) {
            // both have value and can be decoded as bool
            if (older_it->GetBool() == true && newer_it->GetBool() == false) {
                // newer represent a larger range
                return false;
            } else {
                // either equal value or newer represent a smaller range
                return true;
            }
        }

        if (older_it->IsDouble() && newer_it->IsDouble()) {
            // TODO extend this for double
            throw as_exception(invalid_schema(fmt::format(
              R"({}-{} not implemented for types other than "boolean". input: older: '{}', newer: '{}')",
              __FUNCTION__,
              prop_name,
              pj{older},
              pj{newer})));
        } else {
            // types changes are always not compatible (one is boolean and the
            // other is double)
            return false;
        }
    };

    if (!exclusive_limit_check(older, newer, "exclusiveMinimum")) {
        return false;
    }

    if (!exclusive_limit_check(older, newer, "exclusiveMaximum")) {
        return false;
    }

    return true;
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

bool is_object_properties_superset(
  json::Value const& older, json::Value const& newer) {
    // check that every property in newer["properties"]
    // if it appears in older["properties"],
    //    then it has to be compatible with the schema
    // or if for every match with a pattern in older["patternProperties"],
    //    then it has to be compatible with the schema,
    // or
    //    it has to be compatible with older["additionalProperties"]

    auto newer_properties = get_object_or_empty(newer, "properties");
    if (newer_properties.ObjectEmpty()) {
        // no "properties" in newer, all good
        return true;
    }

    // older["properties"] is a map of <prop, schema>
    auto older_properties = get_object_or_empty(older, "properties");
    // older["patternProperties"] is a map of <pattern, schema>
    auto older_pattern_properties = get_object_or_empty(
      older, "patternProperties");
    // older["additionalProperties"] is a schema
    auto get_older_additional_properties = [&]() -> json::Value const& {
        auto older_it = older.FindMember("additionalProperties");
        if (older_it == older.MemberEnd()) {
            // default is `true`
            return get_true_schema();
        }

        if (older_it->value.IsBool()) {
            return older_it->value.GetBool() ? get_true_schema()
                                             : get_false_schema();
        }

        return older_it->value;
    };

    // scan every prop in newer["properties"]
    for (auto const& [prop, schema] : newer_properties) {
        // it is either an evolution of a schema in older["properties"]
        if (auto older_it = older_properties.FindMember(prop);
            older_it != older_properties.MemberEnd()) {
            // prop exists in both
            if (!is_superset(older_it->value, schema)) {
                // not compatible
                return false;
            }
            // check next property
            continue;
        }

        // or it should be checked against every schema in
        // older["patternProperties"] that matches
        auto pattern_match_found = false;
        for (auto pname
             = std::string_view{prop.GetString(), prop.GetStringLength()};
             auto const& [propPattern, schemaPattern] :
             older_pattern_properties) {
            // TODO this rebuilds the regex each time, could be cached
            auto regex = re2::RE2(std::string_view{
              propPattern.GetString(), propPattern.GetStringLength()});
            if (re2::RE2::PartialMatch(pname, regex)) {
                pattern_match_found = true;
                if (!is_superset(schemaPattern, schema)) {
                    // not compatible
                    return false;
                }
            }
        }

        // or it should check against older["additionalProperties"], if no match
        // in patternProperties was found
        if (
          !pattern_match_found
          && !is_superset(get_older_additional_properties(), schema)) {
            // not compatible
            return false;
        }
    }

    return true;
}

bool is_object_pattern_properties_superset(
  json::Value const& older, json::Value const& newer) {
    // check that every pattern property in newer["patternProperties"]
    // appears in older["patternProperties"] and is compatible with the schema

    // "patternProperties" is a map of <pattern, schema>
    auto newer_pattern_properties = get_object_or_empty(
      newer, "patternProperties");
    auto older_pattern_properties = get_object_or_empty(
      older, "patternProperties");

    // TODO O(n^2) lookup
    for (auto const& [pattern, schema] : newer_pattern_properties) {
        // search for pattern in older_pattern_properties and check schemas
        auto older_pp_it = older_pattern_properties.FindMember(pattern);
        if (older_pp_it == older_pattern_properties.MemberEnd()) {
            // pattern not in older["patternProperties"], not compatible
            return false;
        }

        if (!is_superset(older_pp_it->value, schema)) {
            // not compatible
            return false;
        }
    }

    return true;
}

bool is_object_required_superset(
  json::Value const& older, json::Value const& newer) {
    // to pass the check, a required property from newer has to be present in
    // older, or if new it needs to be without a default value note that:
    // 1. we check only required properties that are in both newer["properties"]
    // and older["properties"]
    // 2. there is no explicit check that older has an open content model
    //    there might be a property name outside of (1) that could be rejected
    //    by older, if older["additionalProperties"] is false

    auto older_req = get_array_or_empty(older, "required");
    auto newer_req = get_array_or_empty(newer, "required");
    auto older_props = get_object_or_empty(older, "properties");
    auto newer_props = get_object_or_empty(newer, "properties");

    // TODO O(n^2) lookup that can be a set_intersection
    auto newer_props_in_older = older_props
                                | std::views::transform([&](auto& n_v) {
                                      return newer_props.FindMember(n_v.name);
                                  })
                                | std::views::filter(
                                  [end = newer_props.end()](auto it) {
                                      return it != end;
                                  });
    // intersections of older["properties"] and newer["properties"]
    for (auto prop_it : newer_props_in_older) {
        auto& [name, newer_schema] = *prop_it;

        auto older_is_required = std::ranges::find(older_req, name)
                                 != older_req.end();
        auto newer_is_required = std::ranges::find(newer_req, name)
                                 != newer_req.end();
        if (older_is_required && !newer_is_required) {
            // required property not present in newer, not compatible
            return false;
        }

        if (!older_is_required && newer_is_required) {
            if (newer_schema.HasMember("default")) {
                // newer required property with a default makes newer
                // incompatible
                return false;
            }
        }
    }

    return true;
}

bool is_object_superset(json::Value const& older, json::Value const& newer) {
    if (!is_numeric_property_value_superset(
          older, newer, "minProperties", std::less_equal<>{})) {
        // newer requires less properties to be set
        return false;
    }
    if (!is_numeric_property_value_superset(
          older, newer, "maxProperties", std::greater_equal<>{})) {
        // newer requires more properties to be set
        return false;
    }
    if (!is_additional_superset(older, newer, additional_field_for::object)) {
        // additional properties are not compatible
        return false;
    }
    if (!is_object_properties_superset(older, newer)) {
        // "properties" in newer might not be compatible with
        // older["properties"] (incompatible evolution) or
        // older["patternProperties"] (it is not compatible with the pattern
        // that matches the new name) or older["additionalProperties"] (older
        // has partial open model that does not allow some new properties in
        // newer)
        return false;
    }
    if (!is_object_pattern_properties_superset(older, newer)) {
        // pattern properties checks are not compatible
        return false;
    }
    if (!is_object_required_superset(older, newer)) {
        // required properties are not compatible
        return false;
    }

    return true;
}

bool is_enum_superset(json::Value const& older, json::Value const& newer) {
    auto older_it = older.FindMember("enum");
    auto newer_it = newer.FindMember("enum");
    auto older_is_enum = older_it != older.MemberEnd();
    auto newer_is_enum = newer_it != newer.MemberEnd();

    if (!older_is_enum && !newer_is_enum) {
        // both are not an "enum" schema, compatible
        return true;
    }

    if (!(older_is_enum && newer_is_enum)) {
        // only one is an "enum" schema, not compatible
        return false;
    }

    // both "enum"
    // check that all "enum" values of newer are present in older.
    auto older_set = older_it->value.GetArray();
    auto newer_set = newer_it->value.GetArray();

    if (newer_set.Size() > older_set.Size()) {
        // quick check:
        // newer has some value not in older
        return false;
    }

    // TODO: current implementation is O(n^2), but could be O(n) with normalized
    // input json
    for (auto& v : newer_set) {
        // NOTE: values equality is performed with an O(n^2) search, this can
        // also be improved with normalization of the input
        if (older_set.end() == std::ranges::find(older_set, v)) {
            // newer has an element not in older
            return false;
        }
    }

    return true;
}

bool is_not_combinator_superset(
  json::Value const& older, json::Value const& newer) {
    auto older_it = older.FindMember("not");
    auto newer_it = newer.FindMember("not");
    auto older_has_not = older_it != older.MemberEnd();
    auto newer_has_not = newer_it != newer.MemberEnd();

    if (older_has_not != newer_has_not) {
        // only one has a "not" schema, not compatible
        return false;
    }

    if (older_has_not && newer_has_not) {
        // for not combinator, we want to check if the "not" newer subschema is
        // less strict than the older subschema, because this means that newer
        // validated less data than older
        return is_superset(newer_it->value, older_it->value);
    }

    // both do not have a "not" key, compatible
    return true;
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

    if (!is_enum_superset(older, newer)) {
        return false;
    }

    if (!is_not_combinator_superset(older, newer)) {
        return false;
    }

    for (auto not_yet_handled_keyword : {
           "$schema",
           "additionalItems",
           "items",
           "maxItems",
           "minItems",
           "uniqueItems",
           "definitions",
           "dependencies",
           "allOf",
           "anyOf",
           "oneOf",
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
