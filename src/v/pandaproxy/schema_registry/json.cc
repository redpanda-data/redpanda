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

#include "json/chunked_buffer.h"
#include "json/chunked_input_stream.h"
#include "json/document.h"
#include "json/ostreamwrapper.h"
#include "json/schema.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/absl_sstring_hash.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/flat_hash_set.h>
#include <absl/container/inlined_vector.h>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/max_cardinality_matching.hpp>
#include <boost/math/special_functions/ulp.hpp>
#include <boost/outcome/std_result.hpp>
#include <boost/outcome/success_failure.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <jsoncons/basic_json.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/evaluation_options.hpp>
#include <jsoncons_ext/jsonschema/json_schema_factory.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <rapidjson/error/en.h>
#include <re2/re2.h>

#include <exception>
#include <ranges>
#include <string_view>

namespace pandaproxy::schema_registry {

struct json_schema_definition::impl {
    iobuf to_json() const {
        json::chunked_buffer buf;
        json::Writer<json::chunked_buffer> wrt(buf);
        doc.Accept(wrt);
        return std::move(buf).as_iobuf();
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

ss::future<> check_references(sharded_store& store, canonical_schema schema) {
    for (const auto& ref : schema.def().refs()) {
        co_await store.is_subject_version_deleted(ref.sub, ref.version)
          .handle_exception([](auto) { return is_deleted::yes; })
          .then([&](is_deleted d) {
              if (d) {
                  throw as_exception(
                    no_reference_found_for(schema, ref.sub, ref.version));
              }
          });
    }
}

// this is the list of supported dialects
enum class json_schema_dialect {
    draft4,
    draft6,
    draft7,
    draft201909,
    draft202012,
};

constexpr std::string_view
to_uri(json_schema_dialect draft, bool strip = false) {
    using enum json_schema_dialect;
    auto dialect_str = [&]() -> std::string_view {
        switch (draft) {
        case draft4:
            return "http://json-schema.org/draft-04/schema#";
        case draft6:
            return "http://json-schema.org/draft-06/schema#";
        case draft7:
            return "http://json-schema.org/draft-07/schema#";
        case draft201909:
            return "https://json-schema.org/draft/2019-09/schema#";
        case draft202012:
            return "https://json-schema.org/draft/2020-12/schema#";
        }
    }();

    if (strip) {
        // strip final # from uri
        dialect_str.remove_suffix(1);
    }

    return dialect_str;
}

constexpr std::optional<json_schema_dialect> from_uri(std::string_view uri) {
    using enum json_schema_dialect;
    return string_switch<std::optional<json_schema_dialect>>{uri}
      .match_all(to_uri(draft4), to_uri(draft4, true), draft4)
      .match_all(to_uri(draft6), to_uri(draft6, true), draft6)
      .match_all(to_uri(draft7), to_uri(draft7, true), draft7)
      .match_all(to_uri(draft201909), to_uri(draft201909, true), draft201909)
      .match_all(to_uri(draft202012), to_uri(draft202012, true), draft202012)
      .default_match(std::nullopt);
}

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

template<json_schema_dialect Dialect>
jsoncons::jsonschema::json_schema<jsoncons::json> const& get_metaschema() {
    static auto const meteschema_doc = [] {
        auto metaschema = [] {
            switch (Dialect) {
            case json_schema_dialect::draft4:
                return jsoncons::jsonschema::draft4::schema_draft4<
                  jsoncons::json>::get_schema();
            case json_schema_dialect::draft6:
                return jsoncons::jsonschema::draft6::schema_draft6<
                  jsoncons::json>::get_schema();
            case json_schema_dialect::draft7:
                return jsoncons::jsonschema::draft7::schema_draft7<
                  jsoncons::json>::get_schema();
            case json_schema_dialect::draft201909:
                return jsoncons::jsonschema::draft201909::schema_draft201909<
                  jsoncons::json>::get_schema();
            case json_schema_dialect::draft202012:
                return jsoncons::jsonschema::draft202012::schema_draft202012<
                  jsoncons::json>::get_schema();
            }
        }();

        // Throws if the metaschema can't be parsed (which should never happen
        // and if it does, it would be detected by unit tests)
        return jsoncons::jsonschema::make_json_schema(metaschema);
    }();

    return meteschema_doc;
}

result<void> validate_json_schema(
  json_schema_dialect dialect, const jsoncons::json& schema) {
    // validation pre-step: get metaschema for json draft
    auto const& metaschema_doc = [=]() -> const auto& {
        using enum json_schema_dialect;
        switch (dialect) {
        case draft4:
            return get_metaschema<draft4>();
        case draft6:
            return get_metaschema<draft6>();
        case draft7:
            return get_metaschema<draft7>();
        case draft201909:
            return get_metaschema<draft201909>();
        case draft202012:
            return get_metaschema<draft202012>();
        }
    }();

    // validation of schema: validate it against metaschema
    try {
        // Throws when the schema is invalid with details about the failure
        metaschema_doc.validate(schema);
    } catch (const std::exception& e) {
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Invalid json schema: '{}'. Error: '{}'",
            schema.to_string(),
            e.what())};
    }

    // schema is a syntactically valid json schema, where $schema == Dialect.
    // TODO AB cross validate "$ref" fields, this is not done automatically
    // TODO validate that "pattern" and "patternProperties" are valid regex
    return outcome::success();
}

result<void> try_validate_json_schema(const jsoncons::json& schema) {
    using enum json_schema_dialect;

    // no explicit $schema: try to validate from newest to oldest draft
    auto first_error = std::optional<error_info>{};
    for (auto d : {draft202012, draft201909, draft7, draft6, draft4}) {
        auto res = validate_json_schema(d, schema);
        if (res.has_value()) {
            return outcome::success();
        }
        // failed to validated with dialect d. save error for reporting
        if (!first_error.has_value()) {
            first_error = res.error();
        }
    }

    // A json without a "$schema" member is likely meant to use the latest
    // dialect, so the first failure message is likely more insightful. Also,
    // except for draft4, the other schemas are mostly compatible, only adding
    // rules.
    return first_error.value();
}

result<json::Document> parse_json(iobuf buf) {
    // parse string in json document, check it's a valid json
    auto schema_stream = json::chunked_input_stream{
      buf.share(0, buf.size_bytes())};
    auto schema = json::Document{};
    if (schema.ParseStream(schema_stream).HasParseError()) {
        // not a valid json document, return error
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Malformed json schema: {} at offset {}",
            rapidjson::GetParseError_En(schema.GetParseError()),
            schema.GetErrorOffset())};
    }

    // get the dialect, try to directly validate it against the appropriate
    // metaschema
    auto dialect = std::optional<json_schema_dialect>{};

    if (schema.IsObject()) {
        // "true/false" are valid schemas so here we need to check that the
        // schema is an actual object
        if (auto it = schema.FindMember("$schema"); it != schema.MemberEnd()) {
            if (it->value.IsString()) {
                dialect = from_uri(it->value.GetString());
            }

            if (it->value.IsString() == false || dialect == std::nullopt) {
                // if present, "$schema" have to be a string, and it has to be
                // one the implemented dialects. If not, return an error
                return error_info{
                  error_code::schema_invalid,
                  fmt::format(
                    "Unsupported json schema dialect: '{}'", pj{it->value})};
            }
        }
    }

    // We use jsoncons for validating the schema against the metaschema as
    // currently rapidjson doesn't support validating schemas newer than
    // draft 5.
    iobuf_istream is{std::move(buf)};
    auto jsoncons_schema = jsoncons::json::parse(is.istream());
    auto validation_res = dialect.has_value()
                            ? validate_json_schema(
                              dialect.value(), jsoncons_schema)
                            : try_validate_json_schema(jsoncons_schema);
    if (validation_res.has_error()) {
        return validation_res.as_failure();
    }

    return {std::move(schema)};
}

/// is_superset section

// a schema O is a superset of another schema N if every schema that is valid
// for N is also valid for O. precondition: older and newer are both valid
// schemas
bool is_superset(json::Value const& older, json::Value const& newer);

// close the implementation in a namespace to keep it contained
namespace is_superset_impl {

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

json::Value::ConstObject get_true_schema() {
    // A `true` schema is one that validates every possible input, it's literal
    // json value is `{}` Then `"additionalProperty": true` is equivalent to
    // `"additionalProperties": {}` it's used during mainly in
    // is_object_superset(older, newer) and in is_superset to short circuit
    // validation
    static auto const true_schema = json::Value{rapidjson::kObjectType};
    return true_schema.GetObject();
}

bool is_true_schema(json::Value const& v) {
    // check that v is either true or {}. used to break recursion with
    // is_superset NOTE that {"this_prop_is_not_real": 42} should be considered
    // a true schema, but this function does not recognize it.
    // TODO possible micro optimization: if &v == &get_true_schema(): return
    // true

    // support keyword true
    if (v.IsBool()) {
        return v.GetBool();
    }

    // support {}
    if (v.IsObject()) {
        return v.MemberCount() == 0;
    }

    return false;
}

json::Value::ConstObject get_false_schema() {
    // A `false` schema is one that doesn't validate any input, it's literal
    // json value is `{"not": {}}`
    // `"additionalProperty": false` is equivalent to `"additionalProperties":
    // {"not": {}}` it's used during mainly in is_object_superset(older, newer)
    // and in is_superset to short circuit validation
    static auto const false_schema = [] {
        auto tmp = json::Document{};
        tmp.Parse(R"({"not": {}})");
        vassert(!tmp.HasParseError(), "Malformed `false` json schema");
        return tmp;
    }();
    return false_schema.GetObject();
}

bool is_false_schema(json::Value const& v) {
    // check that v is either false or {"not": {}}. used to break recursion with
    // is_superset This will accept also {˝this_prop_is_not_real": 42, "not":
    // {}} as a false schema.
    // TODO possible micro optimization: if &v == &get_false_schema(): return
    // true

    // support keyword false
    if (v.IsBool()) {
        return !v.GetBool();
    }
    // support {"not": {}}
    if (v.IsObject()) {
        auto it = v.FindMember("not");
        if (it != v.MemberEnd()) {
            return is_true_schema(it->value);
        }
    }

    return false;
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

// helper to convert a boolean to a schema
json::Value::ConstObject get_schema(json::Value const& v) {
    if (v.IsObject()) {
        return v.GetObject();
    }

    if (v.IsBool()) {
        // in >= draft6 "true/false" is a valid schema and means
        // {}/{"not":{}}
        return v.GetBool() ? get_true_schema() : get_false_schema();
    }
    throw as_exception(error_info{
      error_code::schema_invalid,
      fmt::format(
        "Invalid JSON Schema, should be object or boolean: '{}'", pj{v})});
}

// helper to retrieve the object value for a key, or an empty object if the key
// is not present
json::Value::ConstObject
get_object_or_empty(json::Value const& v, std::string_view key) {
    auto it = v.FindMember(
      json::Value{key.data(), rapidjson::SizeType(key.size())});
    if (it != v.MemberEnd()) {
        return get_schema(it->value);
    }

    return get_true_schema();
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

// helper for numeric property that fits into a double.
// if the property has a default value, it can be passed as last parameter.
// This is necessary to make {"type": "string", "minLength": 0} equivalent to
// {"type": "string"}.
// if no default value is passed, the result is given by this table
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
  VPred&& value_predicate,
  std::optional<double> default_value = std::nullopt) {
    // get value or default_value
    auto get_value = [&](json::Value const& v) -> std::optional<double> {
        auto it = v.FindMember(
          json::Value{prop_name.data(), rapidjson::SizeType(prop_name.size())});

        if (it == v.MemberEnd()) {
            return default_value;
        }

        // Gate on values that can't be represented with doubles.
        // rapidjson can serialize a uint64_t even thought it's not a widely
        // supported type, so deserializing that would trigger this. note also
        // that 0.1 is a valid json literal, but does not have an exact double
        // representation. this cannot be caught with this, and it would require
        // some sort of decimal type
        if (!it->value.IsLosslessDouble()) {
            throw as_exception(invalid_schema(fmt::format(
              R"(is_numeric_property_value_superset-{} not implemented for type {}. input: older: '{}', newer: '{}')",
              prop_name,
              it->value.GetType(),
              pj{older},
              pj{newer})));
        }

        return it->value.GetDouble();
    };

    auto older_value = get_value(older);
    auto newer_value = get_value(newer);
    if (older_value == newer_value) {
        // either both not set or with the same value
        return true;
    }

    if (older_value.has_value() && newer_value.has_value()) {
        return std::invoke(
          std::forward<VPred>(value_predicate), *older_value, *newer_value);
    }

    // (relevant only if default_value is not used)
    // only one is set. if older is not set then newer has a value that is more
    // restrictive, so older is a superset of newer
    return !older_value.has_value();
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
          older, newer, "minLength", std::less_equal<>{}, 0)) {
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
              // check that the reminder of newer/older is close enough to 0.
              // close enough is defined as being close to the Unit in the Last
              // Place of the bigger between the two.
              // TODO: this is an approximate check, if a bigdecimal
              // representation it would be possible to perform an exact
              // reminder(newer, older)==0 check
              constexpr auto max_ulp_error = 3;
              return std::abs(std::remainder(newer, older))
                     <= (max_ulp_error * boost::math::ulp(newer));
          })) {
        return false;
    }

    // exclusiveMinimum/exclusiveMaximum checks are mostly the same logic,
    // implemented in this helper
    auto exclusive_limit_check = [](
                                   json::Value const& older,
                                   json::Value const& newer,
                                   std::string_view prop_name,
                                   std::invocable<double, double> auto pred) {
        auto get_value = [=](json::Value const& v)
          -> std::variant<std::monostate, bool, double> {
            auto it = v.FindMember(json::Value{
              prop_name.data(), rapidjson::SizeType(prop_name.size())});
            if (it == v.MemberEnd()) {
                return std::monostate{};
            }
            if (it->value.IsBool()) {
                return it->value.GetBool();
            }
            if (it->value.IsLosslessDouble()) {
                return it->value.GetDouble();
            }
            // v could not be decodes as a double, likely a malformed json
            throw as_exception(invalid_schema(fmt::format(
              R"(is_numeric_superset-{} not implemented for types other than "boolean" and "number". input: '{}')",
              prop_name,
              pj{v})));
        };

        return std::visit(
          ss::make_visitor(
            [](bool older, bool newer) {
                // compatible if no change or if older was not "exclusive"
                return older == newer || older == false;
            },
            [](bool older, std::monostate) {
                // monostate defaults to false, compatible if older is false
                return older == false;
            },
            [&](double older, double newer) {
                // delegate to pred
                return std::invoke(pred, older, newer);
            },
            [](double, std::monostate) {
                // newer is less strict than older
                return false;
            },
            [](std::monostate, auto) {
                // older has no rules, compatible with everything
                return true;
            },
            [&](auto, auto) -> bool {
                throw as_exception(invalid_schema(fmt::format(
                  R"(is_numeric_superset-{} not implemented for mixed types: older: '{}', newer: '{}')",
                  prop_name,
                  pj{older},
                  pj{newer})));
            }),
          get_value(older),
          get_value(newer));
    };

    if (!exclusive_limit_check(
          older, newer, "exclusiveMinimum", std::less_equal<>{})) {
        return false;
    }

    if (!exclusive_limit_check(
          older, newer, "exclusiveMaximum", std::greater_equal<>{})) {
        return false;
    }

    return true;
}

bool is_array_superset(json::Value const& older, json::Value const& newer) {
    // "type": "array" is used to model an array or a tuple.
    // for array, "items" is a schema that validates all the elements.
    // for tuple in Draft4, "items" is an array of schemas to validate the
    // tuple, and "additionalItems" a schema to validate extra elements.
    // TODO in later drafts, tuple validation has "prefixItems" as array of
    // schemas, "items" is for validation of extra elements, "additionalItems"
    // is not used.
    // This superset function has a common section for tuples and array, and
    // then is split based on array/tuple.

    // size checks are common to both types
    if (!is_numeric_property_value_superset(
          older, newer, "minItems", std::less_equal<>{}, 0)) {
        return false;
    }

    if (!is_numeric_property_value_superset(
          older, newer, "maxItems", std::greater_equal<>{})) {
        return false;
    }

    // uniqueItems makes sense mostly for arrays, but it's also allowed for
    // tuples, so the validation is done here
    auto get_unique_items = [](json::Value const& v) {
        auto it = v.FindMember("uniqueItems");
        if (it == v.MemberEnd()) {
            // default value
            return false;
        }
        return it->value.GetBool();
    };

    auto older_value = get_unique_items(older);
    auto newer_value = get_unique_items(newer);
    // the only failure mode is if we removed the "uniqueItems" requirement
    // older ui | newer ui | compatible
    // -------- | -------- | ---------
    //  false   |   ___    |   yes
    //  true    |   true   |   yes
    //  true    |   false  |   no

    if (older_value == true && newer_value == false) {
        // removed unique items requirement
        return false;
    }

    // check if the input is an array schema or a tuple schema
    auto is_array = [](json::Value const& v) -> bool {
        // TODO "prefixItems" is not in Draft4, it's from later drafts. if it's
        // present, it's a tuple schema
        auto items_it = v.FindMember("items");
        // default for items is `{}` so it's not a tuple schema
        // v is a tuple schema if "items" is an array of schemas
        auto is_tuple = items_it != v.MemberEnd() && items_it->value.IsArray();
        return !is_tuple;
    };

    auto older_is_array = is_array(older);
    auto newer_is_array = is_array(newer);

    if (older_is_array != newer_is_array) {
        // one is a tuple and the other is not. not compatible
        return false;
    }
    // both are tuples or both are arrays

    if (older_is_array) {
        // both are array, only "items" is relevant and it's a schema
        // TODO after draft 4 "items" can be also a boolean so this needs to
        // account for that note that "additionalItems" can be defined, but it's
        // not used by validation because every element is validated against
        // "items"
        return is_superset(
          get_object_or_empty(older, "items"),
          get_object_or_empty(newer, "items"));
    }

    // both are tuple schemas, validation is similar to object. one side
    // effect is that the "items" key is present.

    // first check is for "additionalItems" compatibility, it's cheaper than the
    // rest
    if (!is_additional_superset(older, newer, additional_field_for::array)) {
        return false;
    }

    auto older_tuple_schema = older["items"].GetArray();
    auto newer_tuple_schema = newer["items"].GetArray();
    // find the first pair of schemas that do not match
    auto [older_it, newer_it] = std::ranges::mismatch(
      older_tuple_schema, newer_tuple_schema, is_superset);

    if (
      older_it != older_tuple_schema.end()
      && newer_it != newer_tuple_schema.end()) {
        // if both iterators are not end iterators, they are pointing to a
        // pair of elements where is_superset(*older_it, *newer_it)==false,
        // not compatible
        return false;
    }

    // no mismatching elements. two possible cases
    // 1. older_tuple_schema.Size() >= newer_tuple_schema.Size() ->
    // compatible (implies newer_it==end())
    // 2. older_tuple_schema.Size() <  newer_tuple_schema.Size() -> check
    // excess elements with older["additionalItems"]

    auto older_additional_schema = get_object_or_empty(
      older, "additionalItems");

    // check that all excess schemas are compatible with
    // older["additionalItems"]
    return std::all_of(
      newer_it, newer_tuple_schema.end(), [&](json::Value const& n) {
          return is_superset(older_additional_schema, n);
      });
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
    auto older_additional_properties = get_object_or_empty(
      older, "additionalProperties");
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
          && !is_superset(older_additional_properties, schema)) {
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
          older, newer, "minProperties", std::less_equal<>{}, 0)) {
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

enum class p_combinator { oneOf, allOf, anyOf };
json::Value to_keyword(p_combinator c) {
    switch (c) {
    case p_combinator::oneOf:
        return json::Value{"oneOf"};
    case p_combinator::allOf:
        return json::Value{"allOf"};
    case p_combinator::anyOf:
        return json::Value{"anyOf"};
    }
}

bool is_positive_combinator_superset(
  json::Value const& older, json::Value const& newer) {
    auto get_combinator = [](json::Value const& v) {
        auto res = std::optional<p_combinator>{};
        for (auto c :
             {p_combinator::oneOf, p_combinator::allOf, p_combinator::anyOf}) {
            if (v.HasMember(to_keyword(c))) {
                if (res.has_value()) {
                    // ensure that only one combinator is present in the schema.
                    // json schema allows more than one of {"oneOf", "anyOf",
                    // "allOf"} to appear, but it's not currently supported for
                    // is_superset
                    throw as_exception(invalid_schema(
                      fmt::format("{} has more than one combinator", pj{v})));
                }
                res = c;
            }
        }
        return res;
    };

    auto maybe_older_comb = get_combinator(older);
    auto maybe_newer_comb = get_combinator(newer);
    if (!maybe_older_comb.has_value()) {
        // older has not a combinator, maximum freedom for newer. compatible
        return true;
    }
    // older has a combinator

    if (!maybe_newer_comb.has_value()) {
        // older has a combinator but newer does not. not compatible
        return false;
    }
    // newer has a combinator

    auto older_comb = maybe_older_comb.value();
    auto newer_comb = maybe_newer_comb.value();
    auto older_schemas
      = older.FindMember(to_keyword(older_comb))->value.GetArray();
    auto newer_schemas
      = newer.FindMember(to_keyword(newer_comb))->value.GetArray();

    if (older_comb != p_combinator::anyOf && older_comb != newer_comb) {
        // different combinators, and older is not "anyOf". there might some
        // compatible combinations:

        if (older_schemas.Size() == 1 && newer_schemas.Size() == 1) {
            // both combinators have only one subschema, so the actual
            // combinator does not matter. compare subschemas directly
            return is_superset(*older_schemas.Begin(), *newer_schemas.Begin());
        }

        // either older or newer - or both - has more than one subschema

        if (older_schemas.Size() == 1 && newer_comb == p_combinator::allOf) {
            // older has only one subschema, newer is "allOf" so it can be
            // compatible if any one of the subschemas matches older
            return std::ranges::any_of(
              newer_schemas, [&](json::Value const& s) {
                  return is_superset(*older_schemas.Begin(), s);
              });
        }

        if (older_comb == p_combinator::oneOf && newer_schemas.Size() == 1) {
            // older has multiple schemas but only one can be valid. it's
            // compatible if the only subschema in newer is compatible with one
            // in older
            return std::ranges::any_of(
              older_schemas, [&](json::Value const& s) {
                  return is_superset(s, *newer_schemas.Begin());
              });
        }

        // different combinators, not a special case. not compatible
        return false;
    }

    // same combinator for older and newer, or older is "anyOf"

    // size differences between older_schemas and newer_schemas have different
    // meaning based on combinator.
    // TODO a denormalized schema could fail this check while being compatible
    if (older_schemas.Size() > newer_schemas.Size()) {
        if (older_comb == p_combinator::allOf) {
            // older has more restrictions than newer, not compatible
            return false;
        }
    } else if (older_schemas.Size() < newer_schemas.Size()) {
        if (
          newer_comb == p_combinator::anyOf
          || newer_comb == p_combinator::oneOf) {
            // newer has more degrees of freedom than older, not compatible
            return false;
        }
    }

    // sizes are compatible, now we need to check that every schema from
    // the smaller schema array has a unique compatible schema.
    // To do so, we construct a bipartite graphs of the schemas with a vertex
    // for each schema, and an edge for each pair (o ∈ older_schemas, n ∈
    // newer_schemas), if is_superset(o, n). Then we compute the
    // maximum_cardinality_matching and the result is compatible if all the
    // schemas from the smaller schema_array are connected in this match. NOTE:
    // older_schemas will have index [0, older_schemas.size()), newer_schemas
    // will have index [older_schemas.size(),
    // older_schemas.size()+newer_schemas.size()).
    // TODO during this phase, all the subschemas from the smaller schema array
    // need to have at least an edge, so we can early exit if is_superset if
    // false for all the possible edges
    using graph_t
      = boost::adjacency_list<boost::vecS, boost::vecS, boost::undirectedS>;
    auto superset_graph = graph_t{older_schemas.Size() + newer_schemas.Size()};
    for (auto o = 0u; o < older_schemas.Size(); ++o) {
        for (auto n = 0u; n < newer_schemas.Size(); ++n) {
            if (is_superset(older_schemas[o], newer_schemas[n])) {
                // translate n for the graph
                auto n_index = n + older_schemas.Size();
                add_edge(o, n_index, superset_graph);
            }
        }
    }

    // find if for each sub_schema there is a distinct compatible sub_schema
    auto mate_res = std::vector<graph_t::vertex_descriptor>(
      superset_graph.vertex_set().size());
    boost::edmonds_maximum_cardinality_matching(
      superset_graph, mate_res.data());

    if (
      matching_size(superset_graph, mate_res.data())
      != std::min(older_schemas.Size(), newer_schemas.Size())) {
        // one of  sub schema was left out, meaning that it either had no valid
        // is_superset() relation with the other schema array, or that the
        // algorithm couldn't find a unique compatible pattern.
        return false;
    }

    return true;
}

} // namespace is_superset_impl

using namespace is_superset_impl;

// a schema O is a superset of another schema N if every schema that is valid
// for N is also valid for O. precondition: older and newer are both valid
// schemas
bool is_superset(
  json::Value const& older_schema, json::Value const& newer_schema) {
    // break recursion if parameters are atoms:
    if (is_true_schema(older_schema) || is_false_schema(newer_schema)) {
        // either older is the superset of every possible schema, or newer is
        // the subset of every possible schema
        return true;
    }

    auto older = get_schema(older_schema);
    auto newer = get_schema(newer_schema);

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

    if (!is_positive_combinator_superset(older, newer)) {
        return false;
    }

    for (auto not_yet_handled_keyword : {
           "definitions",
           "dependencies",
           // draft 6 unhandled keywords:
           "$ref",
           // draft 2019-09 unhandled keywords:
           "dependentRequired",
           "dependentSchemas",
           // draft 2020-12 unhandled keywords:
           "prefixItems",
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

// this function assumes that the inputs are valid schemas, and if they have a
// $schema member it will have a string value
bool check_compatible_dialects(
  json::Value const& older, json::Value const& newer) {
    auto get_dialect =
      [](json::Value const& v) -> std::optional<json_schema_dialect> {
        if (!v.IsObject()) {
            // support true/false schemas
            return std::nullopt;
        }
        auto it = v.FindMember("$schema");
        if (it == v.MemberEnd()) {
            return std::nullopt;
        }
        return from_uri(it->value.GetString());
    };

    auto older_dialect = get_dialect(older);
    auto newer_dialect = get_dialect(newer);
    if (!older_dialect.has_value() && !newer_dialect.has_value()) {
        // no $schema, compatible
        return true;
    }
    // basic support: require that both use the same dialect.
    // TODO: schemas using different dialects could be conditionally compatible
    if (older_dialect != newer_dialect) {
        throw as_exception(invalid_schema(fmt::format(
          "not yet implemented compatibility check between different dialects: "
          "{}, {}",
          older_dialect ? to_uri(older_dialect.value()) : "[not specified]",
          newer_dialect ? to_uri(newer_dialect.value()) : "[not specified]")));
    }

    // same dialect, compatible
    return true;
}

void sort(json::Value& val) {
    switch (val.GetType()) {
    case rapidjson::Type::kFalseType:
    case rapidjson::Type::kNullType:
    case rapidjson::Type::kNumberType:
    case rapidjson::Type::kStringType:
    case rapidjson::Type::kTrueType:
        break;
    case rapidjson::Type::kArrayType: {
        for (auto& v : val.GetArray()) {
            sort(v);
        }
        break;
    }
    case rapidjson::Type::kObjectType: {
        auto v = val.GetObject();
        std::sort(v.begin(), v.end(), [](auto& lhs, auto& rhs) {
            return std::string_view{
                     lhs.name.GetString(), lhs.name.GetStringLength()}
                   < std::string_view{
                     rhs.name.GetString(), rhs.name.GetStringLength()};
        });
    }
    }
}

} // namespace

ss::future<json_schema_definition>
make_json_schema_definition(sharded_store&, canonical_schema schema) {
    auto doc
      = parse_json(schema.def().shared_raw()()).value(); // throws on error
    std::string_view name = schema.sub()();
    auto refs = std::move(schema).def().refs();
    co_return json_schema_definition{
      ss::make_shared<json_schema_definition::impl>(std::move(doc), name),
      std::move(refs)};
}

ss::future<canonical_schema> make_canonical_json_schema(
  sharded_store& store, unparsed_schema unparsed_schema, normalize norm) {
    auto [sub, unparsed] = std::move(unparsed_schema).destructure();
    auto [def, type, refs] = std::move(unparsed).destructure();

    auto doc = parse_json(std::move(def)).value(); // throws on error
    if (norm) {
        sort(doc);
        std::sort(refs.begin(), refs.end());
        refs.erase(std::unique(refs.begin(), refs.end()), refs.end());
    }
    json::chunked_buffer out;
    json::Writer<json::chunked_buffer> w{out};
    doc.Accept(w);

    canonical_schema schema{
      std::move(sub),
      canonical_schema_definition{
        canonical_schema_definition::raw_string{std::move(out).as_iobuf()},
        type,
        std::move(refs)}};

    // Ensure all references exist
    co_await check_references(store, schema.share());

    co_return schema;
}

compatibility_result check_compatible(
  const json_schema_definition& reader,
  const json_schema_definition& writer,
  verbose is_verbose [[maybe_unused]]) {
    auto is_compatible = [&]() {
        // schemas might be using incompatible dialects
        if (!check_compatible_dialects(reader().doc, writer().doc)) {
            return false;
        }
        // reader is a superset of writer iff every schema that is valid for
        // writer is also valid for reader
        return is_superset(reader().doc, writer().doc);
    }();

    // TODO(gellert.nagy): start using the is_verbose flag in a follow up PR
    return compatibility_result{.is_compat = is_compatible};
}

} // namespace pandaproxy::schema_registry
