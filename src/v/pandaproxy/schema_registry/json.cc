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
#include "json/pointer.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/compatibility.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/flat_hash_map.h>
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
#include <filesystem>
#include <ranges>
#include <string_view>

namespace pandaproxy::schema_registry {

namespace {

using json_compatibility_result = raw_compatibility_result;

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

// type to contain the canonical uri for an id, in the form host/path
// useful to limit the degree of freedom of the uri (https vs http, ports, user
// info, etc)
using json_id_uri = named_type<ss::sstring, struct json_id_uri_tag>;

// mapping of $id to jsonpointer to the parent object
// it contains at least the root $id for doc. If the root $id is not
// present, then the default value "" is used
using id_to_schema_pointer = absl::
  flat_hash_map<json_id_uri, std::pair<json::Pointer, json_schema_dialect>>;

json_id_uri to_json_id_uri(const jsoncons::uri& uri) {
    // ensure that only scheme, host and path are used
    return json_id_uri{
      jsoncons::uri{uri.scheme(), "", uri.host(), "", uri.path(), "", ""}
        .string()};
}

struct document_context {
    json::Document doc;
    json_schema_dialect dialect;
    id_to_schema_pointer bundled_schemas;
};

// Passed into is_superset_* methods where the path and the generated verbose
// incompatibilities don't matter, only whether they are compatible or not
static const std::filesystem::path ignored_path = "";

// helper struct to hold a json::Document or a json::Value::ConstObject, used in
// ref_resolution to either hold an existing json::Value from the root object or
// a synthesized one, when a $ref has siblings
struct json_const_object {
    std::variant<json::Value::ConstObject, std::unique_ptr<json::Document>>
      storage;
    json_const_object(const json::Value::ConstObject& co) noexcept
      : storage(co) {}
    json_const_object(std::unique_ptr<json::Document>&& d) noexcept
      : storage(std::move(d)) {}

    operator const json::Value&() const {
        return std::visit(
          ss::make_visitor(
            [](const json::Value::ConstObject& co) -> const json::Value& {
                return co;
            },
            [](const std::unique_ptr<json::Document>& d) -> const json::Value& {
                return d->GetObject();
            }),
          storage);
    }

    const json::Value& operator*() const {
        return static_cast<const json::Value&>(*this);
    }
    const json::Value* operator->() const {
        return &static_cast<const json::Value&>(*this);
    }
};

} // namespace

struct json_schema_definition::impl {
    iobuf to_json() const {
        json::chunked_buffer buf;
        json::Writer<json::chunked_buffer> wrt(buf);
        ctx.doc.Accept(wrt);
        return std::move(buf).as_iobuf();
    }

    impl(
      document_context ctx,
      std::string_view name,
      canonical_schema_definition::references refs)
      : ctx{std::move(ctx)}
      , name{name}
      , refs(std::move(refs)) {}

    document_context ctx;
    ss::sstring name;
    canonical_schema_definition::references refs;
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

const canonical_schema_definition::references&
json_schema_definition::refs() const {
    return _impl->refs;
}

ss::sstring json_schema_definition::name() const { return {_impl->name}; };

std::optional<ss::sstring> json_schema_definition::title() const {
    if (!_impl->ctx.doc.IsObject()) {
        return std::nullopt;
    }
    auto it = _impl->ctx.doc.FindMember("title");
    if (it == _impl->ctx.doc.MemberEnd()) {
        return std::nullopt;
    }

    return ss::sstring{it->value.GetString(), it->value.GetStringLength()};
}
namespace {

std::string_view as_string_view(const json::Value& v) {
    return {v.GetString(), v.GetStringLength()};
}

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

// helper struct to format json::Value
struct pj {
    const json::Value& v;
    friend std::ostream& operator<<(std::ostream& os, const pj& p) {
        auto osw = json::OStreamWrapper{os};
        auto writer = json::Writer<json::OStreamWrapper>{osw};
        p.v.Accept(writer);
        return os;
    }
};

struct pjp {
    const json::Pointer& p;
    friend std::ostream& operator<<(std::ostream& os, const pjp& p) {
        auto osw = json::OStreamWrapper{os};
        p.p.Stringify(osw);
        return os;
    }
};

class schema_context {
public:
    explicit schema_context(const json_schema_definition::impl& schema)
      : _schema{schema} {}

    json_schema_dialect dialect() const { return _schema.ctx.dialect; }
    const json::Value& doc() const { return _schema.ctx.doc; }

    const id_to_schema_pointer::mapped_type*
    find_bundled(const json_id_uri id) const {
        auto it = _schema.ctx.bundled_schemas.find(id);
        if (it == _schema.ctx.bundled_schemas.end()) {
            return nullptr;
        }
        return &(it->second);
    }

    int remaining_ref_units() const { return _ref_units; }
    int consume_ref_units() { return --_ref_units; }

private:
    const json_schema_definition::impl& _schema;
    static constexpr int max_recursion_depth{5};
    int _ref_units{max_recursion_depth};
};

struct context {
    schema_context older;
    schema_context newer;
};

template<json_schema_dialect Dialect>
const jsoncons::jsonschema::json_schema<jsoncons::ojson>& get_metaschema() {
    static const auto meteschema_doc = [] {
        auto metaschema = [] {
            switch (Dialect) {
            case json_schema_dialect::draft4:
                return jsoncons::jsonschema::draft4::schema_draft4<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft6:
                return jsoncons::jsonschema::draft6::schema_draft6<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft7:
                return jsoncons::jsonschema::draft7::schema_draft7<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft201909:
                return jsoncons::jsonschema::draft201909::schema_draft201909<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft202012:
                return jsoncons::jsonschema::draft202012::schema_draft202012<
                  jsoncons::ojson>::get_schema();
            }
        }();

        // Throws if the metaschema can't be parsed (which should never happen
        // and if it does, it would be detected by unit tests)
        return jsoncons::jsonschema::make_json_schema(metaschema);
    }();

    return meteschema_doc;
}

result<json_schema_dialect> validate_json_schema(
  json_schema_dialect dialect, const jsoncons::ojson& schema) {
    // validation pre-step: get metaschema for json draft
    const auto& metaschema_doc = [=]() -> const auto& {
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
    return dialect;
}

result<json_schema_dialect>
try_validate_json_schema(const jsoncons::ojson& schema) {
    using enum json_schema_dialect;

    // no explicit $schema: try to validate from newest to oldest draft
    auto first_error = std::optional<error_info>{};
    for (auto d : {draft202012, draft201909, draft7, draft6, draft4}) {
        auto res = validate_json_schema(d, schema);
        if (res.has_value()) {
            return res;
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

// forward declaration
result<id_to_schema_pointer> collect_bundled_schema_and_fix_refs(
  jsoncons::ojson& doc, json_schema_dialect dialect);

result<document_context> parse_json(iobuf buf) {
    // parse string in json document, check it's a valid json
    iobuf_istream is{buf.share(0, buf.size_bytes())};

    auto decoder = jsoncons::json_decoder<jsoncons::ojson>{};
    auto reader = jsoncons::basic_json_reader(is.istream(), decoder);
    auto ec = std::error_code{};
    reader.read(ec);
    if (ec || !decoder.is_valid()) {
        // not a valid json document, return error
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Malformed json schema: {} at line {} column {}",
            ec ? ec.message() : "Invalid document",
            reader.line(),
            reader.column())};
    }
    auto schema = decoder.get_result();

    // get the dialect, try to directly validate it against the appropriate
    // metaschema
    auto maybe_dialect = std::optional<json_schema_dialect>{};

    if (schema.is_object()) {
        // "true/false" are valid schemas so here we need to check that the
        // schema is an actual object
        if (auto it = schema.find("$schema");
            it != schema.object_range().end()) {
            if (it->value().is_string()) {
                maybe_dialect = from_uri(it->value().as_string_view());
            }

            if (
              it->value().is_string() == false || !maybe_dialect.has_value()) {
                // if present, "$schema" have to be a string, and it has to be
                // one the implemented dialects. If not, return an error
                return error_info{
                  error_code::schema_invalid,
                  fmt::format(
                    "Unsupported json schema dialect: '{}'",
                    jsoncons::print(it->value()))};
            }
        }
    }

    // We use jsoncons for validating the schema against the metaschema as
    // currently rapidjson doesn't support validating schemas newer than
    // draft 5.
    auto validation_res = maybe_dialect.has_value()
                            ? validate_json_schema(
                                maybe_dialect.value(), schema)
                            : try_validate_json_schema(schema);
    if (validation_res.has_error()) {
        return validation_res.as_failure();
    }
    auto dialect = validation_res.assume_value();

    // this function will resolve al local ref against their respective baseuri.
    auto bundled_schemas_map = collect_bundled_schema_and_fix_refs(
      schema, dialect);
    if (bundled_schemas_map.has_error()) {
        return bundled_schemas_map.as_failure();
    }

    // to use rapidjson we need to serialized schema again
    // We take a copy of the jsoncons schema here because it has the fixed-up
    // references that we want to use for compatibility checks
    auto iobuf_os = iobuf_ostream{};
    schema.dump(iobuf_os.ostream());

    auto schema_stream = json::chunked_input_stream{std::move(iobuf_os).buf()};
    auto rapidjson_schema = json::Document{};
    if (rapidjson_schema.ParseStream(schema_stream).HasParseError()) {
        // not a valid json document, return error
        // this is unlikely to happen, since we already parsed this stream with
        // jsoncons, but the possibility of a bug exists
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Malformed json schema: {} at offset {}",
            rapidjson::GetParseError_En(rapidjson_schema.GetParseError()),
            rapidjson_schema.GetErrorOffset())};
    }

    return {
      std::move(rapidjson_schema),
      dialect,
      std::move(bundled_schemas_map).assume_value()};
}

/// is_superset section

// a schema O is a superset of another schema N if every schema that is valid
// for N is also valid for O. precondition: older and newer are both valid
// schemas
json_compatibility_result is_superset(
  context ctx,
  const json::Value& older,
  const json::Value& newer,
  std::filesystem::path p);

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

constexpr auto parse_json_type(const json::Value& v) {
    auto sv = as_string_view(v);
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
    static const auto true_schema = json::Value{rapidjson::kObjectType};
    return true_schema.GetObject();
}

bool is_true_schema(const json::Value& v) {
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
    static const auto false_schema = [] {
        auto tmp = json::Document{};
        tmp.Parse(R"({"not": {}})");
        vassert(!tmp.HasParseError(), "Malformed `false` json schema");
        return tmp;
    }();
    return false_schema.GetObject();
}

bool is_false_schema(const json::Value& v) {
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
json_type_list normalized_type(const json::Value& v) {
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

// builder for merged references. this will spilt out a json that is validated
// by this schema.
// {
//   "oneOf": [
//     {
//       "$comment": "the referenced schema, if the base schema did not contain
//       siblings to $ref"
//     },
//     {
//       "type": "object",
//       "properties": {
//         "allOf": {
//           "type": "array",
//           "minItems": 2,
//           "items": true,
//           "$comment": "items are 1) the original schema without the $ref and
//           2) the referenced schema. if the referenced schema contains a $ref,
//           further referenced schemas are flattened"
//         }
//       }
//     }
//   ]
// }
// the caller is responsible to keep the root object alive, since the result may
// reference strings in it
json_const_object
merge_references(std::span<json::Value::ConstObject> references_objects) {
    if (references_objects.empty()) {
        throw std::logic_error("merged_references called on empty sequence");
    }

    const auto ref_key = json::Value{"$ref"};

    auto empty_refless = [&](const json::Value::ConstObject& obj) {
        // empty_refless if empty once $ref is removed
        return obj.ObjectEmpty()
               || (obj.MemberCount() == 1 && obj.FindMember(ref_key) != obj.MemberEnd());
    };

    // list of objects once we remove the $ref-only schemas
    auto non_empty_references = references_objects
                                | std::views::filter([&](const auto& obj) {
                                      return !empty_refless(obj);
                                  });

    auto non_empty_size = std::ranges::distance(non_empty_references);

    if (non_empty_size == 0) {
        // a degenerate case: all the objects are empty, once removed the
        // references.
        return get_true_schema();
    }

    if (non_empty_size == 1) {
        // only one object is not empty, return it directly
        // this is the case for objects with a single $ref without siblings
        return non_empty_references.front();
    }

    // at least 2 non-empty objects, build the allOf array

    auto to_document_without_ref = [&](const json::Value::ConstObject& obj) {
        // copy obj to a new document without the $ref key
        auto doc = json::Document{rapidjson::kObjectType};
        auto& alloc = doc.GetAllocator();
        for (auto& [k, v] : obj) {
            if (k != ref_key) {
                // copy the key-value pair, try to reference the strings from
                // the original document if possible
                doc.AddMember(
                  json::Value(k, alloc, false),
                  json::Value(v, alloc, false),
                  alloc);
            }
        }
        return doc;
    };

    // build the result document with the allof array
    auto res = std::make_unique<json::Document>();
    res->Parse(R"({"allOf": []})");
    vassert(!res->HasParseError(), "malformed merged references template");

    // append everything to the allof array
    auto& res_alloc = res->GetAllocator();
    auto& allof_array = res->FindMember("allOf")->value;
    allof_array.Reserve(non_empty_size, res_alloc);
    for (auto d : non_empty_references
                    | std::views::transform(to_document_without_ref)) {
        allof_array.PushBack(json::Value(d, res_alloc), res_alloc);
    }

    return res;
}

// helper to parse a json pointer with rapidjson. throws if there is an error
// parsing it
json::Pointer to_json_pointer(std::string_view sv) {
    auto candidate = json::Pointer{sv.data(), sv.size()};
    if (auto ec = candidate.GetParseErrorCode();
        ec != rapidjson::kPointerParseErrorNone) {
        throw as_exception(error_info{
          error_code::schema_invalid,
          fmt::format(
            "invalid fragment '{}' error {} at {}",
            sv,
            ec,
            candidate.GetParseErrorOffset())});
    }

    return candidate;
}

// helper to resolve a pointer in a json object. throws if the object can't be
// retrieved
const json::Value&
resolve_pointer(const json::Pointer& p, const json::Value& root) {
    auto unresolved_token = size_t{0};
    auto* value = p.Get(root, &unresolved_token);
    if (value == nullptr) {
        throw as_exception(error_info{
          error_code::schema_invalid,
          fmt::format(
            "object not found for pointer '{}' unresolved token at index "
            "{}",
            pjp{p},
            unresolved_token)});
    }

    return *value;
}

// iteratively resolve a reference, following the $ref field until the end or
// the max_allowed_depth is reached. throws if the max depth is reached or if
// the reference can't be resolved
json_const_object
resolve_reference(schema_context& ctx, const json::Value& candidate) {
    auto ref_it = candidate.FindMember("$ref");
    if (ref_it == candidate.MemberEnd()) { // not a reference, no-op
        return candidate.GetObject();
    }

    auto get_uri_fragment = [](std::string uri_s) {
        // split into host and fragment
        auto uri = jsoncons::uri{uri_s};
        return std::pair{to_json_id_uri(uri), to_json_pointer(uri.fragment())};
    };

    auto [id_uri, fragment_p] = get_uri_fragment(ref_it->value.GetString());

    // store the reference chains here, to merge them later, start with base
    auto references_objects = absl::InlinedVector<json::Value::ConstObject, 4>{
      candidate.GetObject()};

    // resolve the reference:
    while (ctx.consume_ref_units() > 0) {
        // try to find the bundled schema, get a pointer to it
        auto* lookup_p = ctx.find_bundled(id_uri);
        if (lookup_p == nullptr) {
            // TODO use a better error code
            throw as_exception(error_info{
              error_code::schema_invalid,
              fmt::format("schema pointer not found for uri '{}'", id_uri)});
        }
        const auto& [schema_pointer, dialect] = *lookup_p;

        // step 1: get the schema object
        const auto& schema = resolve_pointer(schema_pointer, ctx.doc());
        // step 2: get the referenced object inside the schema
        const auto& referenced_obj = resolve_pointer(fragment_p, schema);
        // step 2.5: store referenced_obj for merging later
        references_objects.push_back(referenced_obj.GetObject());

        // step 3: check if the referenced object has a $ref field, and if so
        // resolve it
        if (auto next_ref_it = referenced_obj.FindMember("$ref");
            next_ref_it != referenced_obj.MemberEnd()) {
            std::tie(id_uri, fragment_p) = get_uri_fragment(
              next_ref_it->value.GetString());
        } else {
            // if this is the final target, return it.

            // require that we are using the same dialect as the root object.
            // this requirement could be relaxed but it requires to keep track
            // of the dialect for each json::Value
            if (dialect != ctx.dialect()) {
                throw as_exception(error_info{
                  error_code::schema_invalid,
                  fmt::format("schema dialect mismatch for uri '{}'", id_uri)});
            }

            return merge_references(references_objects);
        }
    }
    throw std::runtime_error(fmt::format(
      "max traversals reached for uri {} '{}'", id_uri, pjp{fragment_p}));
}

// helper to convert a boolean to a schema, and to traverse $refs
json_const_object get_schema(schema_context& ctx, const json::Value& v) {
    if (v.IsObject()) {
        return resolve_reference(ctx, v.GetObject());
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
get_object_or_empty(const json::Value& v, std::string_view key) {
    auto it = v.FindMember(
      json::Value{key.data(), rapidjson::SizeType(key.size())});
    if (it == v.MemberEnd()) {
        return get_true_schema();
    }

    if (it->value.IsObject()) {
        return it->value.GetObject();
    }

    if (it->value.IsBool()) {
        // in >= draft6 "true/false" is a valid schema and means
        // {}/{"not":{}}
        return it->value.GetBool() ? get_true_schema() : get_false_schema();
    }
    throw as_exception(error_info{
      error_code::schema_invalid,
      fmt::format(
        "Invalid JSON Schema, should be object or boolean: '{}'", pj{v})});
}

// helper to retrieve the array value for a key, or an empty array if the key
// is not present
json::Value::ConstArray
get_array_or_empty(const json::Value& v, std::string_view key) {
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
std::tuple<std::optional<bool>, const json::Value*, const json::Value*>
extract_property_and_gate_check(
  const json::Value& older,
  const json::Value& newer,
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
json_compatibility_result is_numeric_property_value_superset(
  const json::Value& older,
  const json::Value& newer,
  std::string_view prop_name,
  VPred&& value_predicate,
  json_incompatibility changed_err,
  json_incompatibility added_err,
  std::optional<double> default_value = std::nullopt) {
    // get value or default_value
    auto get_value = [&](const json::Value& v) -> std::optional<double> {
        auto it = v.FindMember(
          json::Value{prop_name.data(), rapidjson::SizeType(prop_name.size())});

        if (it == v.MemberEnd()) {
            return std::nullopt;
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

    if (older_value.has_value() && newer_value.has_value()) {
        if (!std::invoke(
              std::forward<VPred>(value_predicate),
              *older_value,
              *newer_value)) {
            return json_compatibility_result::of<json_incompatibility>(
              changed_err);
        }
    } else if (older_value.has_value()) {
        if (!default_value.has_value() || *older_value != *default_value) {
            // Non-default value was removed
            return json_compatibility_result::of<json_incompatibility>(
              added_err);
        }
    }

    // Value only in newer or neither
    return json_compatibility_result{};
}

enum class additional_field_for { object, array };

json_compatibility_result is_additional_superset(
  const context& ctx,
  const json::Value& older,
  const json::Value& newer,
  additional_field_for field_type,
  std::filesystem::path p) {
    // "additional___" can be either true (if omitted it's true), false
    // or a schema. The check is performed with this table.
    // older ap | newer ap | compatible
    // -------- | -------- | ----------
    //   true   |   ____   |    yes
    //   false  |   ____   | newer==false
    //  schema  |  schema  |  recurse
    //  schema  |   true   |  recurse with {}
    //  schema  |   false  |  recurse with {"not":{}}

    // in draft 2020, if checking "type": "array", "items" is used to represent
    // additional tuples items instead of an "additionalItems"
    auto get_field_name = [&](json_schema_dialect d) {
        switch (field_type) {
        case additional_field_for::object:
            return "additionalProperties";
        case additional_field_for::array:
            using enum json_schema_dialect;
            switch (d) {
            case draft4:
            case draft6:
            case draft7:
            case draft201909:
                return "additionalItems";
            case draft202012:
                return "items";
            }
        }
    };
    auto [additional_path, narrowed_errt, removed_errt] = [&] {
        switch (field_type) {
        case additional_field_for::object:
            return std::make_tuple(
              p / "additionalProperties",
              json_incompatibility_type::additional_properties_narrowed,
              json_incompatibility_type::additional_properties_removed);
        case additional_field_for::array:
            // Even for draft 202012 "additionalItems" is used in the path not
            // "items"
            return std::make_tuple(
              p / "additionalItems",
              json_incompatibility_type::additional_items_narrowed,
              json_incompatibility_type::additional_items_removed);
        }
    }();
    // helper to parse additional__
    auto get_additional_props = [&](json_schema_dialect d, const json::Value& v)
      -> std::variant<bool, const json::Value*> {
        auto it = v.FindMember(get_field_name(d));
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
        [&additional_path, removed_errt](bool older, bool newer) {
            if (older || !newer) {
                return json_compatibility_result{};
            }
            // older=false -> newer=true  - not compatible
            return json_compatibility_result::of<json_incompatibility>(
              std::move(additional_path), removed_errt);
        },
        [&ctx, &additional_path, removed_errt](
          bool older, const json::Value* newer) {
            if (older) {
                // true is compatible with any schema
                return json_compatibility_result{};
            }
            // likely false, but need to check
            if (is_superset(ctx, get_false_schema(), *newer, ignored_path)
                  .has_error()) {
                return json_compatibility_result::of<json_incompatibility>(
                  std::move(additional_path), removed_errt);
            }
            return json_compatibility_result{};
        },
        [&ctx, &additional_path, narrowed_errt](
          const json::Value* older, bool newer) {
            if (!newer) {
                // any schema is compatible with false
                return json_compatibility_result{};
            }
            // convert newer to {} and check against that
            if (is_superset(ctx, *older, get_true_schema(), ignored_path)
                  .has_error()) {
                return json_compatibility_result::of<json_incompatibility>(
                  std::move(additional_path), narrowed_errt);
            }
            return json_compatibility_result{};
        },
        [&ctx,
         &additional_path](const json::Value* older, const json::Value* newer) {
            // check subschemas for compatibility
            return is_superset(ctx, *older, *newer, std::move(additional_path));
        }),
      get_additional_props(ctx.older.dialect(), older),
      get_additional_props(ctx.newer.dialect(), newer));
}

json_compatibility_result is_string_superset(
  const json::Value& older, const json::Value& newer, std::filesystem::path p) {
    json_compatibility_result res;

    // note: "format" is not part of the checks

    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "minLength",
      std::less_equal<>{},
      {p / "minLength", json_incompatibility_type::min_length_increased},
      {p / "minLength", json_incompatibility_type::min_length_added},
      0));

    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "maxLength",
      std::greater_equal<>{},
      {p / "maxLength", json_incompatibility_type::max_length_decreased},
      {p / "maxLength", json_incompatibility_type::max_length_added}));

    auto [maybe_gate_value, older_val_p, newer_val_p]
      = extract_property_and_gate_check(older, newer, "pattern");
    if (maybe_gate_value.has_value()) {
        if (!maybe_gate_value.value()) {
            res.emplace<json_incompatibility>(
              p / "pattern", json_incompatibility_type::pattern_added);
        }
        return res;
    }

    // both have "pattern". check if they are the same, the only
    // possible_value_accepted
    if (as_string_view(*older_val_p) != as_string_view(*newer_val_p)) {
        res.emplace<json_incompatibility>(
          p / "pattern", json_incompatibility_type::pattern_changed);
    }
    return res;
}

json_compatibility_result is_numeric_superset(
  const json::Value& older, const json::Value& newer, std::filesystem::path p) {
    json_compatibility_result res;

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

    // older["minimum"] is not superset of newer["minimum"] because newer is
    // less strict
    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "minimum",
      std::less_equal<>{},
      {p / "minimum", json_incompatibility_type::minimum_increased},
      {p / "minimum", json_incompatibility_type::minimum_added}));

    // older["maximum"] is not superset of newer["maximum"] because newer is
    // less strict
    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "maximum",
      std::greater_equal<>{},
      {p / "maximum", json_incompatibility_type::maximum_decreased},
      {p / "maximum", json_incompatibility_type::maximum_added}));

    // TODO: return multiple_of_expanded instead of multiple_of_changed if older
    // is a multiple of newer
    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "multipleOf",
      [](double older, double newer) {
          // check that the reminder of newer/older is close enough to 0.
          // close enough is defined as being close to the Unit in the Last
          // Place of the bigger between the two.
          // TODO: this is an approximate check, if a bigdecimal
          // representation it would be possible to perform an exact
          // reminder(newer, older)==0 check
          constexpr auto max_ulp_error = 3;
          return std::abs(std::remainder(newer, older))
                 <= (max_ulp_error * boost::math::ulp(newer));
      },
      {p / "multipleOf", json_incompatibility_type::multiple_of_changed},
      {p / "multipleOf", json_incompatibility_type::multiple_of_added}));

    // exclusiveMinimum/exclusiveMaximum checks are mostly the same logic,
    // implemented in this helper
    auto exclusive_limit_check =
      [](
        const json::Value& older,
        const json::Value& newer,
        std::string_view prop_name,
        std::invocable<double, double> auto pred,
        json_compatibility_result changed_err,
        json_compatibility_result added_err) -> json_compatibility_result {
        auto get_value = [=](const json::Value& v)
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
            [&](bool older, bool newer) {
                // compatible if no change or if older was not "exclusive"
                if (older != newer && older != false) {
                    return changed_err;
                }
                return json_compatibility_result{};
            },
            [&](bool older, std::monostate) {
                // monostate defaults to false, compatible if older is false
                if (older != false) {
                    return added_err;
                }
                return json_compatibility_result{};
            },
            [&](double older, double newer) {
                // delegate to pred
                if (!std::invoke(pred, older, newer)) {
                    return changed_err;
                }
                return json_compatibility_result{};
            },
            [&](double, std::monostate) {
                // newer is less strict than older
                return added_err;
            },
            [](std::monostate, auto) {
                // older has no rules, compatible with everything
                return json_compatibility_result{};
            },
            [&](auto, auto) -> json_compatibility_result {
                throw as_exception(invalid_schema(fmt::format(
                  R"(is_numeric_superset-{} not implemented for mixed types: older: '{}', newer: '{}')",
                  prop_name,
                  pj{older},
                  pj{newer})));
            }),
          get_value(older),
          get_value(newer));
    };

    auto p_exlusive_minimum = p / "exclusiveMinimum";
    res.merge(exclusive_limit_check(
      older,
      newer,
      "exclusiveMinimum",
      std::less_equal<>{},
      json_compatibility_result::of<json_incompatibility>(
        p_exlusive_minimum,
        json_incompatibility_type::exclusive_minimum_increased),
      json_compatibility_result::of<json_incompatibility>(
        p_exlusive_minimum,
        json_incompatibility_type::exclusive_minimum_added)));

    auto p_exlusive_maximum = p / "exclusiveMaximum";
    res.merge(exclusive_limit_check(
      older,
      newer,
      "exclusiveMaximum",
      std::greater_equal<>{},
      json_compatibility_result::of<json_incompatibility>(
        p_exlusive_maximum,
        json_incompatibility_type::exclusive_maximum_decreased),
      json_compatibility_result::of<json_incompatibility>(
        p_exlusive_maximum,
        json_incompatibility_type::exclusive_maximum_added)));

    return res;
}

json_compatibility_result is_array_superset(
  const context& ctx,
  const json::Value& older,
  const json::Value& newer,
  std::filesystem::path p) {
    json_compatibility_result res;

    // "type": "array" is used to model an array or a tuple.
    // for array, "items" is a schema that validates all the elements.
    // for tuple in Draft4, "items" is an array of schemas to validate the
    // tuple, and "additionalItems" a schema to validate extra elements.
    // from draft 2020, tuple validation has "prefixItems" as array of
    // schemas, "items" is for validation of extra elements, "additionalItems"
    // is not used.
    // This superset function has a common section for tuples and array, and
    // then is split based on array/tuple.

    // size checks are common to both types
    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "minItems",
      std::less_equal<>{},
      {p / "minItems", json_incompatibility_type::min_items_increased},
      {p / "minItems", json_incompatibility_type::min_items_added},
      0));

    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "maxItems",
      std::greater_equal<>{},
      {p / "maxItems", json_incompatibility_type::max_items_decreased},
      {p / "maxItems", json_incompatibility_type::max_items_added}));

    // uniqueItems makes sense mostly for arrays, but it's also allowed for
    // tuples, so the validation is done here
    auto get_unique_items = [](const json::Value& v) {
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
        res.emplace<json_incompatibility>(
          p / "uniqueItems", json_incompatibility_type::unique_items_added);
    }

    // in draft 2020, "prefixItems" is used to represent tuples instead of an
    // overloaded "items"
    constexpr static auto get_tuple_items_kw = [](json_schema_dialect d) {
        using enum json_schema_dialect;
        switch (d) {
        case draft4:
        case draft6:
        case draft7:
        case draft201909:
            return "items";
        case draft202012:
            return "prefixItems";
        }
    };
    constexpr static auto get_additional_items_kw = [](json_schema_dialect d) {
        using enum json_schema_dialect;
        switch (d) {
        case draft4:
        case draft6:
        case draft7:
        case draft201909:
            return "additionalItems";
        case draft202012:
            return "items";
        }
    };

    // check if the input is an array schema or a tuple schema
    auto is_tuple = [](json_schema_dialect d, const json::Value& v) -> bool {
        auto tuple_items_it = v.FindMember(get_tuple_items_kw(d));
        // default for items is `{}` so it's not a tuple schema
        // v is a tuple schema if "items" is an array of schemas
        return tuple_items_it != v.MemberEnd()
               && tuple_items_it->value.IsArray();
    };

    auto older_is_tuple = is_tuple(ctx.older.dialect(), older);
    auto newer_is_tuple = is_tuple(ctx.newer.dialect(), newer);

    if (older_is_tuple != newer_is_tuple) {
        // one is a tuple and the other is not. not compatible
        res.emplace<json_incompatibility>(
          p / "items", json_incompatibility_type::unknown);
        return res;
    }
    // both are tuples or both are arrays

    if (!older_is_tuple) {
        // both are array, only "items" is relevant and it's a schema
        // note that "additionalItems" can be defined, but it's
        // not used by validation because every element is validated against
        // "items"
        res.merge(is_superset(
          ctx,
          get_object_or_empty(older, "items"),
          get_object_or_empty(newer, "items"),
          p / "items"));
        return res;
    }

    // both are tuple schemas, validation is similar to object. one side
    // effect is that the "items" key is present.

    // first check is for "additionalItems" compatibility, it's cheaper than the
    // rest

    res.merge(is_additional_superset(
      ctx, older, newer, additional_field_for::array, p));
    if (res.has_error()) {
        return res;
    }

    auto older_tuple_schema
      = older[get_tuple_items_kw(ctx.older.dialect())].GetArray();
    auto newer_tuple_schema
      = newer[get_tuple_items_kw(ctx.newer.dialect())].GetArray();
    auto older_it = older_tuple_schema.begin();
    auto newer_it = newer_tuple_schema.begin();
    int index = 0;
    for (; older_it != older_tuple_schema.end()
           && newer_it != newer_tuple_schema.end();
         ++older_it, ++newer_it, ++index) {
        res.merge(is_superset(
          ctx, *older_it, *newer_it, p / "items" / std::to_string(index)));
        if (res.has_error()) {
            return res;
        }
    }

    // no mismatching elements, and they don't have the same size.
    // To be compatible, excess elements needs to be compatible with the other
    // "additionalItems" schema
    auto older_additional_schema = get_object_or_empty(
      older, get_additional_items_kw(ctx.older.dialect()));
    auto newer_additional_schema = get_object_or_empty(
      newer, get_additional_items_kw(ctx.newer.dialect()));

    // newer_has_more: true if newer has excess elements, false if older has
    // excess elements
    auto newer_has_more = newer_it != newer_tuple_schema.end();
    auto excess_begin = newer_has_more ? newer_it : older_it;
    auto excess_end = newer_has_more ? newer_tuple_schema.end()
                                     : older_tuple_schema.end();
    auto errt = newer_has_more
                  ? json_incompatibility_type::
                      item_removed_not_covered_by_partially_open_content_model
                  : json_incompatibility_type::
                      item_added_not_covered_by_partially_open_content_model;

    std::for_each(excess_begin, excess_end, [&](const json::Value& e) {
        auto item_p = p / "items" / std::to_string(index);
        auto sup_res = newer_has_more
                         ? is_superset(ctx, older_additional_schema, e, item_p)
                         : is_superset(ctx, e, newer_additional_schema, item_p);

        if (sup_res.has_error()) {
            res.merge(std::move(sup_res));
            res.emplace<json_incompatibility>(std::move(item_p), errt);
        }

        ++index;
    });

    return res;
}

json_compatibility_result is_object_properties_superset(
  const context& ctx,
  const json::Value& older,
  const json::Value& newer,
  std::filesystem::path p) {
    json_compatibility_result res;
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
        return res;
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
    for (const auto& [prop, schema] : newer_properties) {
        auto prop_path = [&p, &prop] {
            return p / "properties" / prop.GetString();
        };

        // it is either an evolution of a schema in older["properties"]
        if (auto older_it = older_properties.FindMember(prop);
            older_it != older_properties.MemberEnd()) {
            // prop exists in both
            res.merge(is_superset(ctx, older_it->value, schema, prop_path()));
            // check next property
            continue;
        }

        // or it should be checked against every schema in
        // older["patternProperties"] that matches
        auto pattern_match_found = false;
        for (auto pname = as_string_view(prop);
             const auto& [propPattern, schemaPattern] :
             older_pattern_properties) {
            // TODO this rebuilds the regex each time, could be cached
            auto regex = re2::RE2(as_string_view(propPattern));
            if (re2::RE2::PartialMatch(pname, regex)) {
                pattern_match_found = true;

                auto prop_res = is_superset(
                  ctx, schemaPattern, schema, prop_path());

                if (prop_res.has_error()) {
                    res.merge(std::move(prop_res));
                    res.emplace<json_incompatibility>(
                      prop_path(),
                      json_incompatibility_type::
                        property_removed_not_covered_by_partially_open_content_model);
                    break;
                }
            }
        }
        if (pattern_match_found) {
            // check next property
            continue;
        }

        // or it should check against older["additionalProperties"], if no match
        // in patternProperties was found
        if (is_false_schema(older_additional_properties)) {
            res.emplace<json_incompatibility>(
              prop_path(),
              json_incompatibility_type::
                property_removed_from_closed_content_model);
            continue;
        }

        auto add_prop_res = is_superset(
          ctx, older_additional_properties, schema, prop_path());

        if (add_prop_res.has_error()) {
            res.merge(std::move(add_prop_res));
            res.emplace<json_incompatibility>(
              prop_path(),
              json_incompatibility_type::
                property_removed_not_covered_by_partially_open_content_model);
            continue;
        }

        // Additional properties matched. We can move on to the next property
        // without any errors added to res.
    }

    return res;
}

json_compatibility_result is_object_required_superset(
  const json::Value& older, const json::Value& newer, std::filesystem::path p) {
    json_compatibility_result res;
    // to pass the check, a required property from newer has to be present in
    // older, or if new it needs to have a default value.
    // note that:
    // 1. we check only required properties that are in both newer["properties"]
    // and older["properties"]
    // 2. there is no explicit check that older has an open content model
    //    there might be a property name outside of (1) that could be rejected
    //    by update, if update["additionalProperties"] is false

    auto older_req = get_array_or_empty(older, "required");
    auto newer_req = get_array_or_empty(newer, "required");
    auto older_props = get_object_or_empty(older, "properties");
    auto newer_props = get_object_or_empty(newer, "properties");

    // TODO O(n^2) lookup that can be a set_intersection.
    auto older_req_in_both_properties
      = older_req | std::views::filter([&](const json::Value& o) {
            return newer_props.HasMember(o) && older_props.HasMember(o);
        });

    // for each element:
    // in older.required? | in newer.required? | result
    //       yes          |        yes         |  yes
    //       yes          |         no         |  if it has "default" in older
    //       no           |        yes         |  yes
    std::ranges::for_each(
      older_req_in_both_properties, [&](const json::Value& o) {
          if (
            std::ranges::find(newer_req, o) == newer_req.End()
            && !older_props.FindMember(o)->value.HasMember("default")) {
              res.emplace<json_incompatibility>(
                p / "required" / as_string_view(o),
                json_incompatibility_type::required_attribute_added);
          }
      });
    return res;
}

json_compatibility_result is_object_dependencies_superset(
  const context& ctx,
  const json::Value& older,
  const json::Value& newer,
  std::filesystem::path p) {
    json_compatibility_result res;
    // "dependencies", if present, is a dict of <property, string_array |
    // schema>. To be compatible, each key in older has to be in newer and the
    // values have to be of the same type and compatible.
    // older string array needs to be a subset of newer string array.
    // older schema needs to be compatible with newer schema

    auto older_p = get_object_or_empty(older, "dependencies");
    auto newer_p = get_object_or_empty(newer, "dependencies");

    // all dependencies in older need to carry over in newer, and need to be
    // compatible
    // TODO: n^2 search

    std::ranges::for_each(older_p, [&](const json::Value::Member& older_dep) {
        auto path_dep = p / "dependencies" / as_string_view(older_dep.name);
        const auto& o = older_dep.value;
        auto n_it = newer_p.FindMember(older_dep.name);

        if (o.IsObject()) {
            if (n_it == newer_p.MemberEnd()) {
                res.emplace<json_incompatibility>(
                  std::move(path_dep),
                  json_incompatibility_type::dependency_schema_added);
                return;
            }

            const auto& n = n_it->value;

            if (!n.IsObject()) {
                res.emplace<json_incompatibility>(
                  std::move(path_dep),
                  json_incompatibility_type::dependency_schema_added);
                return;
            }

            // schemas: o and n needs to be compatible
            res.merge(is_superset(ctx, o, n, std::move(path_dep)));
        } else if (o.IsArray()) {
            if (n_it == newer_p.MemberEnd()) {
                res.emplace<json_incompatibility>(
                  std::move(path_dep),
                  json_incompatibility_type::dependency_array_added);
                return;
            }

            const auto& n = n_it->value;

            if (!n.IsArray()) {
                res.emplace<json_incompatibility>(
                  std::move(path_dep),
                  json_incompatibility_type::dependency_array_added);
                return;
            }
            // string array: n needs to be a a superset of o
            // TODO: n^2 search
            bool n_superset_of_o = std::ranges::all_of(
              o.GetArray(), [n_array = n.GetArray()](const json::Value& p) {
                  return std::ranges::find(n_array, p) != n_array.End();
              });
            if (!n_superset_of_o) {
                bool o_superset_of_n = std::ranges::all_of(
                  n.GetArray(), [o_array = o.GetArray()](const json::Value& p) {
                      return std::ranges::find(o_array, p) != o_array.End();
                  });
                if (o_superset_of_n) {
                    res.emplace<json_incompatibility>(
                      std::move(path_dep),
                      json_incompatibility_type::dependency_array_extended);
                } else {
                    res.emplace<json_incompatibility>(
                      std::move(path_dep),
                      json_incompatibility_type::dependency_array_changed);
                }
            }
            return;
        } else {
            throw as_exception(invalid_schema(fmt::format(
              "dependencies can only be an array or an object for valid "
              "schemas but it was: {}",
              pj{o})));
        }
    });

    return res;
}

json_compatibility_result is_object_superset(
  const context& ctx,
  const json::Value& older,
  const json::Value& newer,
  std::filesystem::path p) {
    json_compatibility_result res;

    // newer requires less properties to be set
    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "minProperties",
      std::less_equal<>{},
      {p / "minProperties",
       json_incompatibility_type::min_properties_increased},
      {p / "minProperties", json_incompatibility_type::min_properties_added},
      0));

    // newer requires more properties to be set
    res.merge(is_numeric_property_value_superset(
      older,
      newer,
      "maxProperties",
      std::greater_equal<>{},
      {p / "maxProperties",
       json_incompatibility_type::max_properties_decreased},
      {p / "maxProperties", json_incompatibility_type::max_properties_added}));

    // Check if additional properties are compatible
    res.merge(is_additional_superset(
      ctx, older, newer, additional_field_for::object, p));

    // "properties" in newer might not be compatible with
    // older["properties"] (incompatible evolution) or
    // older["patternProperties"] (it is not compatible with the pattern
    // that matches the new name) or older["additionalProperties"] (older
    // has partial open model that does not allow some new properties in
    // newer)
    res.merge(is_object_properties_superset(ctx, older, newer, p));

    // note: to match the behavior of the legacy software,
    // ```
    // if(!is_object_pattern_properties_superset(ctx, older, newer))
    //   return false;
    // ```
    // is omitted

    // Check if required properties are compatible
    res.merge(is_object_required_superset(older, newer, p));

    // Check if dependencies are compatible
    res.merge(is_object_dependencies_superset(ctx, older, newer, std::move(p)));

    return res;
}

json_compatibility_result is_enum_superset(
  const json::Value& older, const json::Value& newer, std::filesystem::path p) {
    json_compatibility_result res;
    auto enum_p = p / "enum";

    auto older_it = older.FindMember("enum");
    auto newer_it = newer.FindMember("enum");
    auto older_is_enum = older_it != older.MemberEnd();
    auto newer_is_enum = newer_it != newer.MemberEnd();

    if (!older_is_enum && !newer_is_enum) {
        // both are not an "enum" schema, compatible
        return res;
    }

    if (!(older_is_enum && newer_is_enum)) {
        // only one is an "enum" schema, not compatible
        res.emplace<json_incompatibility>(
          std::move(enum_p), json_incompatibility_type::enum_array_changed);
        return res;
    }

    // both "enum"
    // check that all "enum" values of newer are present in older.
    auto older_set = older_it->value.GetArray();
    auto newer_set = newer_it->value.GetArray();

    if (newer_set.Size() > older_set.Size()) {
        // quick check:
        // newer has some value not in older
        res.emplace<json_incompatibility>(
          std::move(enum_p), json_incompatibility_type::enum_array_changed);
        return res;
    }

    // TODO: current implementation is O(n^2), but could be O(n) with normalized
    // input json
    for (auto& v : newer_set) {
        // NOTE: values equality is performed with an O(n^2) search, this can
        // also be improved with normalization of the input
        if (older_set.end() == std::ranges::find(older_set, v)) {
            // newer has an element not in older
            res.emplace<json_incompatibility>(
              std::move(enum_p), json_incompatibility_type::enum_array_changed);
            return res;
        }
    }

    return res;
}

json_compatibility_result is_not_combinator_superset(
  const context& ctx,
  const json::Value& older,
  const json::Value& newer,
  std::filesystem::path p) {
    json_compatibility_result res;

    auto older_it = older.FindMember("not");
    auto newer_it = newer.FindMember("not");
    auto older_has_not = older_it != older.MemberEnd();
    auto newer_has_not = newer_it != newer.MemberEnd();

    if (older_has_not != newer_has_not) {
        // only one has a "not" schema, not compatible
        res.emplace<json_incompatibility>(
          p, json_incompatibility_type::type_changed);
        return res;
    }

    if (older_has_not && newer_has_not) {
        // for not combinator, we want to check if the "not" newer subschema is
        // less strict than the older subschema, because this means that newer
        // validated less data than older
        auto is_not_superset = is_superset(
          {ctx.newer, ctx.older},
          newer_it->value,
          older_it->value,
          ignored_path);

        if (is_not_superset.has_error()) {
            res.emplace<json_incompatibility>(
              p / "not", json_incompatibility_type::not_type_extended);
        }
    }

    // both do not have a "not" key, compatible
    return res;
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

json_compatibility_result is_positive_combinator_superset(
  const context& ctx,
  const json::Value& older,
  const json::Value& newer,
  std::filesystem::path p) {
    json_compatibility_result res;

    auto get_combinator = [](const json::Value& v) {
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
        return res;
    }
    // older has a combinator

    if (!maybe_newer_comb.has_value()) {
        // older has a combinator but newer does not. not compatible
        res.emplace<json_incompatibility>(
          std::move(p), json_incompatibility_type::combined_type_changed);
        return res;
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
            auto is_combinator_superset = is_superset(
              ctx,
              *older_schemas.Begin(),
              *newer_schemas.Begin(),
              ignored_path);
            if (is_combinator_superset.has_error()) {
                res.emplace<json_incompatibility>(
                  std::move(p),
                  json_incompatibility_type::combined_type_subschemas_changed);
            }
            return res;
        }

        // either older or newer - or both - has more than one subschema

        if (older_schemas.Size() == 1 && newer_comb == p_combinator::allOf) {
            // older has only one subschema, newer is "allOf" so it can be
            // compatible if any one of the subschemas matches older
            auto any_superset = std::ranges::any_of(
              newer_schemas, [&](const json::Value& s) {
                  return !is_superset(
                            ctx, *older_schemas.Begin(), s, ignored_path)
                            .has_error();
              });
            if (!any_superset) {
                res.emplace<json_incompatibility>(
                  std::move(p),
                  json_incompatibility_type::combined_type_subschemas_changed);
            }
            return res;
        }

        if (older_comb == p_combinator::oneOf && newer_schemas.Size() == 1) {
            // older has multiple schemas but only one can be valid. it's
            // compatible if the only subschema in newer is compatible with one
            // in older
            auto any_superset = std::ranges::any_of(
              older_schemas, [&](const json::Value& s) {
                  return !is_superset(
                            ctx, s, *newer_schemas.Begin(), ignored_path)
                            .has_error();
              });
            if (!any_superset) {
                res.emplace<json_incompatibility>(
                  std::move(p),
                  json_incompatibility_type::combined_type_subschemas_changed);
            }
            return res;
        }

        // different combinators, not a special case. not compatible
        res.emplace<json_incompatibility>(
          std::move(p), json_incompatibility_type::combined_type_changed);
        return res;
    }

    // same combinator for older and newer, or older is "anyOf"

    // size differences between older_schemas and newer_schemas have different
    // meaning based on combinator.
    // TODO a denormalized schema could fail this check while being compatible
    if (older_schemas.Size() > newer_schemas.Size()) {
        if (older_comb == p_combinator::allOf) {
            // older has more restrictions than newer, not compatible
            res.emplace<json_incompatibility>(
              std::move(p), json_incompatibility_type::product_type_extended);
            return res;
        }
    } else if (older_schemas.Size() < newer_schemas.Size()) {
        if (
          newer_comb == p_combinator::anyOf
          || newer_comb == p_combinator::oneOf) {
            // newer has more degrees of freedom than older, not compatible
            res.emplace<json_incompatibility>(
              std::move(p), json_incompatibility_type::sum_type_narrowed);
            return res;
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
            if (!is_superset(
                   ctx, older_schemas[o], newer_schemas[n], ignored_path)
                   .has_error()) {
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
        res.emplace<json_incompatibility>(
          std::move(p),
          json_incompatibility_type::combined_type_subschemas_changed);
        return res;
    }

    return res;
}

} // namespace is_superset_impl

using namespace is_superset_impl;

// a schema O is a superset of another schema N if every schema that is valid
// for N is also valid for O. precondition: older and newer are both valid
// schemas
json_compatibility_result is_superset(
  context ctx,
  const json::Value& older_schema,
  const json::Value& newer_schema,
  std::filesystem::path p) {
    json_compatibility_result res;

    // break recursion if parameters are atoms:
    if (is_true_schema(older_schema) || is_false_schema(newer_schema)) {
        // either older is the superset of every possible schema, or newer is
        // the subset of every possible schema
        return res;
    }

    auto older = get_schema(ctx.older, older_schema);
    auto newer = get_schema(ctx.newer, newer_schema);

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

        auto older_minus_newer = json_type_list{};
        std::ranges::set_difference(
          older_types, newer_types, std::back_inserter(older_minus_newer));

        if (
          older_minus_newer.empty()
          || (older_minus_newer == json_type_list{json_type::integer} && newer_minus_older == json_type_list{json_type::number})) {
            // type_narrowed is reported when older has fewer elements or the
            // same but with integer in older instead of number in newer
            res.emplace<json_incompatibility>(
              std::move(p), json_incompatibility_type::type_narrowed);
        } else {
            // Otherwise report the more general type_changed
            res.emplace<json_incompatibility>(
              std::move(p), json_incompatibility_type::type_changed);
        }
        return res;
    }

    // newer accepts less (or equal) types. for each type, try to find a less
    // strict check
    for (auto t : newer_types) {
        // TODO this will perform a depth first search, but it might be better
        // to do a breadth first search to find a counterexample
        switch (t) {
        case json_type::string:
            res.merge(is_string_superset(older, newer, p));
            break;
        case json_type::integer:
            [[fallthrough]];
        case json_type::number:
            res.merge(is_numeric_superset(older, newer, p));
            break;
        case json_type::object:
            res.merge(is_object_superset(ctx, older, newer, p));
            break;
        case json_type::array:
            res.merge(is_array_superset(ctx, older, newer, p));
            break;
        case json_type::boolean:
            // no check needed for boolean;
            break;
        case json_type::null:
            // no check needed for null;
            break;
        }
    }

    res.merge(is_enum_superset(older, newer, p));
    res.merge(is_not_combinator_superset(ctx, older, newer, p));
    res.merge(is_positive_combinator_superset(ctx, older, newer, p));

    // no rule in newer is less strict than older, older is superset of newer
    return res;
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
            return as_string_view(lhs.name) < as_string_view(rhs.name);
        });
    }
    }
}

void collect_bundled_schemas_and_fix_refs(
  id_to_schema_pointer& bundled_schemas,
  jsoncons::uri base_uri,
  jsoncons::jsonpointer::json_pointer this_obj_ptr,
  jsoncons::ojson& this_obj,
  json_schema_dialect dialect) {
    // scan the json schema object for bundled schema.
    // A bundled schema is defined as a schema with `"$id" : a_base_uri`.
    // all relative refs under this bundled schema will be relative
    // to a_base_uri. if this_obj is a bundled schema, it might use a different
    // dialect than the root schema, and the current implementation requires
    // that the dialect is known. the jsonschema spec is not strict so we could
    // receive some valid but nonsensical edge cases. these are not supported
    // but do not generate errors here, since they are problematic only when a
    // accessed by a $ref. so we don't register them in the base_uris_map, an
    // will raise an error later if we try to access them.
    // keyword | $schema  | is bundled schema
    //   "$id" | missing  | if root is >=draft6
    //   "$id" | >draft4  |       yes
    //   "$id" |  draft4  |       no
    //   "id"  | missing  | if root is draft4
    //   "id"  | >draft4  |       no
    //   "id"  |  draft4  |       yes

    auto maybe_draft4_id_it = this_obj.find("id");
    auto maybe_id_it = this_obj.find("$id");
    if (
      maybe_id_it != this_obj.object_range().end()
      || maybe_draft4_id_it != this_obj.object_range().end()) {
        // we are visiting a bundled schema. the dialect has to be known and has
        // to match the keyword used.
        // try to extract the dialect from the $schema keyword, or use the
        // parent dialect. empty means that "$schema" is present but the dialect
        // is not known, and we should stop scanning this branch.
        auto maybe_new_dialect = [&]() -> std::optional<json_schema_dialect> {
            auto dialect_it = this_obj.find("$schema");
            if (dialect_it == this_obj.object_range().end()) {
                // If no $schema is declared in an embedded schema, it defaults
                // to using the dialect of the parent schema. from
                // https://json-schema.org/understanding-json-schema/structuring#bundling
                return dialect;
            }

            // we have a $schema keyword, use this dialect if we find out that
            // this_obj is a bundled schema
            return from_uri(dialect_it->value().as_string_view());
        }();

        if (maybe_new_dialect.has_value() == false) {
            // stop scanning this tree, we might be in a bundled schema but we
            // don't know the dialect.
            throw as_exception(invalid_schema(fmt::format(
              "bundled schema without a known dialect: '{}'",
              this_obj["$schema"].as_string_view())));
        }

        // we are in a bundled schema and we know the dialect to use, now we
        // know which keyword to use to get the base_uri
        auto id_it = [&] {
            switch (maybe_new_dialect.value()) {
            case json_schema_dialect::draft4:
                return maybe_draft4_id_it;
            case json_schema_dialect::draft6:
            case json_schema_dialect::draft7:
            case json_schema_dialect::draft201909:
            case json_schema_dialect::draft202012:
                return maybe_id_it;
            }
        }();

        if (id_it == this_obj.object_range().end()) {
            // stop scanning this branch, the keyword for base uri does not
            // agree with schema dialect.
            throw as_exception(invalid_schema(fmt::format(
              "bundled schema with mismatched dialect '{}' for id key",
              to_uri(maybe_new_dialect.value()))));
        }

        // run validation since we are not a guaranteed to be in proper schema
        if (auto validation = validate_json_schema(
              maybe_new_dialect.value(), this_obj);
            validation.has_error()) {
            // stop exploring this branch, the schema is invalid
            throw as_exception(invalid_schema(fmt::format(
              "bundled schema is invalid. {}",
              validation.assume_error().message())));
        }

        // base uri keyword agrees with the dialect, it's a validated schema, we
        // can register this as a bundled schema and continue scanning.
        // (run resolve because it could be relative to the parent schema).
        base_uri = jsoncons::uri{id_it->value().as_string()}.resolve(base_uri);
        dialect = maybe_new_dialect.value();
        bundled_schemas.insert_or_assign(
          to_json_id_uri(base_uri),
          std::pair{json::Pointer{this_obj_ptr.to_string()}, dialect});
    }

    if (auto ref_it = this_obj.find("$ref");
        ref_it != this_obj.object_range().end()) {
        // ensure refs are absolute uris
        ref_it->value() = jsoncons::uri{ref_it->value().as_string()}
                            .resolve(base_uri)
                            .string();
    }

    // lambda to recursively scan the object for more bundled schemas and $refs
    auto collect_and_fix = [&](const auto& key, auto& value) {
        collect_bundled_schemas_and_fix_refs(
          bundled_schemas, base_uri, this_obj_ptr / key, value, dialect);
    };

    // recursively scan the object for more bundled schemas and $refs
    for (auto& e : this_obj.object_range()) {
        const auto& key = e.key();
        auto& value = e.value();
        if (value.is_object()) {
            collect_and_fix(key, value);
        } else if (value.is_array()) {
            for (auto i = 0u; i < value.size(); ++i) {
                if (value[i].is_object()) {
                    collect_and_fix(i, value[i]);
                }
            }
        }
    }
}

result<id_to_schema_pointer> collect_bundled_schema_and_fix_refs(
  jsoncons::ojson& doc, json_schema_dialect dialect) {
    // entry point to collect all bundled schemas
    // fetch the root id, if it exists
    auto root_id = [&] {
        if (!doc.is_object()) {
            // might be the case for "true" or "false" schemas
            return json_id_uri{""};
        }

        auto id_it = doc.find(
          dialect == json_schema_dialect::draft4 ? "id" : "$id");
        if (id_it == doc.object_range().end()) {
            // no explicit id, use the empty string
            return json_id_uri{""};
        }

        // $id is set in the schema, use it as the root id
        return to_json_id_uri(jsoncons::uri{id_it->value().as_string()});
    }();

    // insert the root schema as a bundled schema
    auto bundled_schemas = id_to_schema_pointer{
      {root_id, std::pair{json::Pointer{}, dialect}}};

    if (doc.is_object()) {
        // note: current implementation is overly strict and reject any bundled
        // schema that is deemed invalid. this could be relaxed if the invalid
        // schema is not actually accessed by a $ref, but it requires to scan
        // the document in two passes.
        try {
            collect_bundled_schemas_and_fix_refs(
              bundled_schemas, jsoncons::uri{}, {}, doc, dialect);
        } catch (const exception& e) {
            return error_info(
              static_cast<error_code>(e.code().value()), e.message());
        }
    }

    return bundled_schemas;
}

} // namespace

ss::future<json_schema_definition>
make_json_schema_definition(sharded_store&, canonical_schema schema) {
    auto doc
      = parse_json(schema.def().shared_raw()()).value(); // throws on error
    std::string_view name = schema.sub()();
    auto refs = std::move(schema).def().refs();
    co_return json_schema_definition{
      ss::make_shared<json_schema_definition::impl>(
        std::move(doc), name, std::move(refs))};
}

ss::future<canonical_schema> make_canonical_json_schema(
  sharded_store& store, unparsed_schema unparsed_schema, normalize norm) {
    auto [sub, unparsed] = std::move(unparsed_schema).destructure();
    auto [def, type, refs] = std::move(unparsed).destructure();

    auto ctx = parse_json(std::move(def)).value(); // throws on error
    if (norm) {
        sort(ctx.doc);
        std::sort(refs.begin(), refs.end());
        refs.erase(std::unique(refs.begin(), refs.end()), refs.end());
    }
    json::chunked_buffer out;
    json::Writer<json::chunked_buffer> w{out};
    ctx.doc.Accept(w);

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
  verbose is_verbose) {
    auto raw_compat_result = [&]() {
        // reader is a superset of writer iff every schema that is valid for
        // writer is also valid for reader
        context ctx{.older{reader()}, .newer{writer()}};
        return is_superset(ctx, reader().ctx.doc, writer().ctx.doc, "#/");
    }();

    return std::move(raw_compat_result)(is_verbose);
}

} // namespace pandaproxy::schema_registry
