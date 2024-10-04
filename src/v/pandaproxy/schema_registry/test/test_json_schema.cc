// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/compatibility.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/compatibility_common.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/test/tools/context.hpp>
#include <fmt/core.h>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;
using incompat_t = pps::json_incompatibility_type;
using incompatibility = pps::json_incompatibility;

bool check_compatible(
  const pps::json_schema_definition& reader_schema,
  const pps::json_schema_definition& writer_schema) {
    auto res = pps::check_compatible(
      reader_schema, writer_schema, pps::verbose::yes);
    return res.is_compat;
}

pps::compatibility_result check_compatible_verbose(
  const pps::canonical_schema_definition& r,
  const pps::canonical_schema_definition& w) {
    pps::sharded_store s;
    return check_compatible(
      pps::make_json_schema_definition(
        s, {pps::subject("r"), {r.shared_raw(), pps::schema_type::json}})
        .get(),
      pps::make_json_schema_definition(
        s, {pps::subject("w"), {w.shared_raw(), pps::schema_type::json}})
        .get(),
      pps::verbose::yes);
}

struct store_fixture {
    store_fixture() {
        store.start(pps::is_mutable::yes, ss::default_smp_service_group())
          .get();
    }
    ~store_fixture() { store.stop().get(); }

    pps::sharded_store store;
};

struct error_test_case {
    ss::sstring def;
    pps::error_info err;
    friend std::ostream&
    operator<<(std::ostream& os, const error_test_case& e) {
        fmt::print(
          os,
          "def: {}, error_code: {}, error_message: {}",
          e.def,
          e.err.code(),
          e.err.message());
        return os;
    }
};
static const auto error_test_cases = std::to_array({
  // Test invalid JSON
  error_test_case{
    R"({])",
    pps::error_info{
      pps::error_code::schema_invalid,
      "Malformed json schema: Expected object member key at line 1 column 2"}},
  error_test_case{
    "",
    pps::error_info{
      pps::error_code::schema_invalid,
      "Malformed json schema: Invalid document at line 1 column 1"}},
  error_test_case{
    R"({"type": "thisisnotapropertype"})",
    pps::error_info{
      pps::error_code::schema_invalid,
      R"(Invalid json schema: '{"type":"thisisnotapropertype"}'. Error: ': Must be valid against all schemas, but found unmatched schemas')"}},
  error_test_case{
    R"({"$schema": "unsupported_dialect"})",
    pps::error_info{
      pps::error_code::schema_invalid,
      R"(Unsupported json schema dialect: '"unsupported_dialect"')"}},
  error_test_case{
    R"({"$schema": 42})",
    pps::error_info{
      pps::error_code::schema_invalid,
      "Unsupported json schema dialect: '42'"}},
  // exclusiveMinimum is a bool in draft 4 but it is a double in draft 6
  error_test_case{
    R"(
{
  "$schema": "http://json-schema.org/draft-06/schema#",
  "type": "number",
  "minimum": 0,
  "exclusiveMinimum": false
})",
    pps::error_info{
      pps::error_code::schema_invalid,
      R"(Invalid json schema: '{"$schema":"http://json-schema.org/draft-06/schema#","type":"number","minimum":0,"exclusiveMinimum":false}'. Error: '/exclusiveMinimum: Expected number, found boolean')"}},
  // properties, patternProperties, dependencies, can't be
  // refs (they need to contain key:values)
  error_test_case{
    R"(
{
  "type": "object",
  "properties": {
    "$ref": "#/$defs/props"
  }
}
)",
    pps::error_info{
      pps::error_code::schema_invalid,
      R"(Invalid json schema: '{"type":"object","properties":{"$ref":"#/$defs/props"}}'. Error: ': Must be valid against all schemas, but found unmatched schemas')"}},
  error_test_case{
    R"(
{
  "type": "object",
  "patternProperties": {
    "$ref": "#/$defs/patprops"
  }
}
)",
    pps::error_info{
      pps::error_code::schema_invalid,
      R"(Invalid json schema: '{"type":"object","patternProperties":{"$ref":"#/$defs/patprops"}}'. Error: ': Must be valid against all schemas, but found unmatched schemas')"}},
  error_test_case{
    R"(
{
  "type": "object",
  "dependencies": {
    "$ref": "#/$defs/deps"
  }
}
)",
    pps::error_info{
      pps::error_code::schema_invalid,
      R"(Invalid json schema: '{"type":"object","dependencies":{"$ref":"#/$defs/deps"}}'. Error: '/dependencies: Additional property '$ref' found but was invalid.')"}},
  // invalid bundled schemas
  error_test_case{
    R"(
{
  "$comment": "the root schema is valid but the bundled schema is not",
  "$defs": {
      "$comment": "dialect is draft-04, but id key is '$id'",
      "$id": "https://example.com/mismatch_id",
      "$schema": "http://json-schema.org/draft-04/schema#"
  }
}
)",
    pps::error_info{
      pps::error_code::schema_invalid,
      "bundled schema with mismatched dialect "
      "'http://json-schema.org/draft-04/schema#' for id key"}},
  error_test_case{
    R"(
{
  "$comment": "the root schema is valid but the bundled schema is not",
  "$defs": {
      "$comment": "dialect is unkown",
      "$id": "https://example.com/mismatch_id",
      "$schema": "http://json-schema.org/draft-3000/schema#"
  }
}
)",
    pps::error_info{
      pps::error_code::schema_invalid,
      "bundled schema without a known dialect: "
      "'http://json-schema.org/draft-3000/schema#'"}},
  error_test_case{
    R"(
{
  "$comment": "the root schema is valid but the bundled schema is not",
  "$defs": {
      "$comment": "schema is invalid",
      "$id": "https://example.com/mismatch_id",
      "type": "potato"
  }
}
)",
    pps::error_info{
      pps::error_code::schema_invalid,
      R"(bundled schema is invalid. Invalid json schema: '{"$comment":"schema is invalid","$id":"https://example.com/mismatch_id","type":"potato"}'. Error: '/type: Must be valid against at least one schema, but found no matching schemas')"}},
});
SEASTAR_THREAD_TEST_CASE(test_make_invalid_json_schema) {
    for (const auto& data : error_test_cases) {
        store_fixture f;
        BOOST_TEST_CONTEXT(data) {
            try {
                pps::make_canonical_json_schema(
                  f.store,
                  {pps::subject{"test"}, {data.def, pps::schema_type::json}})
                  .get();
                BOOST_CHECK_MESSAGE(
                  false, "terminated without an exception for invalid schema");
            } catch (const pps::exception& e) {
                BOOST_CHECK_EQUAL(e.code(), data.err.code());
                BOOST_WARN_MESSAGE(
                  e.message() == data.err.message(),
                  fmt::format(
                    "[{}] does not match expected [{}]",
                    e.message(),
                    data.err.message()));
            } catch (...) {
                BOOST_CHECK_MESSAGE(
                  false,
                  fmt::format(
                    "terminated with exception {}", std::current_exception()));
            }
        }
    }
}

static constexpr auto valid_test_cases = std::to_array<std::string_view>({
  // primitives
  R"({"type": "number"})",
  R"({"type": "integer"})",
  R"({"type": "object"})",
  R"({"type": "array"})",
  R"({"type": "boolean"})",
  R"({"type": "null"})",
  // atoms
  R"({})",
  R"({"the json schema is an open model": "it means this object is is equivalent to a empty one"})",
  // schemas
  R"(
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "number",
  "minimum": 0,
  "exclusiveMinimum": false
})",
  R"json(
{
  "$schema": "http://json-schema.org/draft-06/schema#",
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    },
    "b": true
  },
  "propertyNames": { "enum": ["a", "b"] }
}
  )json",
  R"json(
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    },
    "b": true
  },
  "propertyNames": { "enum": ["a", "b"] },
  "if": true,
  "then": true
}
  )json",
  R"json({"$schema": "http://json-schema.org/draft-07/schema"})json",
  R"json({"$schema": "http://json-schema.org/draft-06/schema"})json",
  R"json({"$schema": "http://json-schema.org/draft-04/schema"})json",
  R"json({"$schema": "https://json-schema.org/draft/2019-09/schema"})json",
  R"json({"$schema": "https://json-schema.org/draft/2020-12/schema"})json",
});
SEASTAR_THREAD_TEST_CASE(test_make_valid_json_schema) {
    for (const auto& data : valid_test_cases) {
        store_fixture f;
        BOOST_TEST_CONTEXT(data) {
            try {
                pps::make_json_schema_definition(
                  f.store,
                  pps::make_canonical_json_schema(
                    f.store,
                    {pps::subject{"test"}, {data, pps::schema_type::json}})
                    .get())
                  .get();
            } catch (...) {
                BOOST_CHECK_MESSAGE(
                  false,
                  fmt::format(
                    "terminated with exception {}", std::current_exception()));
            }
        }
    }
}

struct test_references_data {
    struct data {
        pps::unparsed_schema schema;
        pps::error_info result;
    };
    std::array<data, 2> _schemas;
};

const auto referenced = pps::unparsed_schema{
  pps::subject{"referenced"},
  pps::unparsed_schema_definition{
    R"({
  "description": "A base schema that defines a number",
  "type": "object",
  "properties": {
    "numberProperty": {
      "type": "number"
    }
  }
})",
    pps::schema_type::json,
    {}}};

const auto referencer = pps::unparsed_schema{
  pps::subject{"referencer"},
  pps::unparsed_schema_definition{
    R"({
  "description": "A schema that references the base schema",
  "type": "object",
  "properties": {
    "reference": {
      "$ref": "example.com/referenced.json"
    }
  }
})",
    pps::schema_type::json,
    {pps::schema_reference{
      .name = "example.com/referenced.json",
      .sub{referenced.sub()},
      .version = pps::schema_version{1}}}}};

const auto referencer_wrong_sub = pps::unparsed_schema{
  referencer.sub(),
  pps::unparsed_schema_definition{
    referencer.def().shared_raw(),
    referencer.def().type(),
    {pps::schema_reference{
      .name = "example.com/referenced.json",
      .sub{"wrong_sub"},
      .version = pps::schema_version{1}}}}};

const std::array test_reference_cases = {
  // Referece correct subject
  test_references_data{{{{referenced.share(), {}}, {referencer.share(), {}}}}},
  // Reference wrong subject
  test_references_data{
    {{{referenced.share(), {}},
      {referencer_wrong_sub.share(),
       {pps::error_code::schema_empty,
        R"(Invalid schema {subject=referencer,version=0,id=-1,schemaType=JSON,references=[{name='example.com/referenced.json', subject='wrong_sub', version=1}],metadata=null,ruleSet=null,schema={
  "description": "A schema that references the base schema",
  "type": "object",
  "properties": {
    "reference": {
      "$ref": "example.com/referenced.json"
    }
  }
}} with refs [{name='example.com/referenced.json', subject='wrong_sub', version=1}] of type JSON, details: No schema reference found for subject "wrong_sub" and version 1)"}}}}}};

SEASTAR_THREAD_TEST_CASE(test_json_schema_references) {
    for (const auto& test : test_reference_cases) {
        store_fixture f;
        pps::schema_id id{0};

        for (const auto& [schema, result] : test._schemas) {
            pps::schema_version ver{0};
            pps::canonical_schema canonical{};
            auto make_canonical = [&]() {
                canonical = f.store.make_canonical_schema(schema.share()).get();
            };

            if (result.code() == pps::error_code{}) {
                BOOST_CHECK_NO_THROW(make_canonical());
            } else {
                BOOST_CHECK_EXCEPTION(
                  make_canonical(),
                  pps::exception,
                  [ec{result.code()}](const pps::exception& ex) {
                      return ex.code() == ec;
                  });
            }
            f.store
              .upsert(
                pps::seq_marker{},
                canonical.share(),
                ++id,
                ++ver,
                pps::is_deleted::no)
              .get();
        }
    }
}

struct compatibility_test_case {
    std::string_view reader_schema;
    std::string_view writer_schema;
    std::vector<incompatibility> compat_result;
    bool expected_exception = false;
};

static const auto compatibility_test_cases = std::to_array<compatibility_test_case>({
  //***** not compatible section *****
  // atoms
  {
    .reader_schema = "false",
    .writer_schema = "true",
    .compat_result = {{"#/", incompat_t::type_changed}},
  },
  // not allowed promotion
  {
    .reader_schema = R"({"type": "integer"})",
    .writer_schema = R"({"type": "number"})",
    .compat_result = {{"#/", incompat_t::type_narrowed}},
  },
  {
    .reader_schema = R"({"type": ["integer", "string"]})",
    .writer_schema = R"({"type": ["number", "string"]})",
    .compat_result = {{"#/", incompat_t::type_narrowed}},
  },
  // not allowed new types
  {
    .reader_schema = R"({"type": "integer"})",
    .writer_schema = R"({})",
    .compat_result = {{"#/", incompat_t::type_narrowed}},
  },
  {
    .reader_schema = R"({"type": "boolean"})",
    .writer_schema = R"({"type": ["boolean", "null"]})",
    .compat_result = {{"#/", incompat_t::type_narrowed}},
  },
  // not allowed numeric evolutions
  {
    .reader_schema = R"({"type": "number", "minimum": 11.2})",
    .writer_schema = R"({"type": "number", "maximum": 30})",
    .compat_result = {{"#/minimum", incompat_t::minimum_added}},
  },
  {
    .reader_schema = R"({"type": "number", "minimum": 1.1, "maximum": 3.199})",
    .writer_schema = R"({"type": "integer", "minimum": 1.1, "maximum": 3.2})",
    .compat_result = {{"#/maximum", incompat_t::maximum_decreased}},
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10})",
    .writer_schema = R"({"type": "number", "multipleOf": 21})",
    .compat_result = {{"#/multipleOf", incompat_t::multiple_of_changed}},
  },
  {.reader_schema = R"({"type": "number", "multipleOf": 20})",
  .writer_schema = R"({"type": "number", "multipleOf": 10})",
  .compat_result = {
      // TODO: should be multiple_of_expanded
    {"#/multipleOf", incompat_t::multiple_of_changed}}, 
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10.1})",
    .writer_schema = R"({"type": "number", "multipleOf": 20.2001})",
    .compat_result = {{"#/multipleOf", incompat_t::multiple_of_changed}},
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 20})",
    .writer_schema = R"({"type": "number"})",
    .compat_result = {{"#/multipleOf", incompat_t::multiple_of_added}},
  },
  {
    .reader_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": true})",
    .writer_schema = R"({"type": "number", "maximum": 10})",
    // Note: this is incorrectly accepted as compatible by the reference impl
    .compat_result = {{"#/exclusiveMaximum", incompat_t::exclusive_maximum_added}},
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .writer_schema = R"({"type": "number"})",
    .compat_result = {{"#/exclusiveMaximum", incompat_t::exclusive_maximum_added}},
  },
  {
    .reader_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": true})",
    .writer_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": 10})",
    .expected_exception = true,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMinimum": 9})",
    .compat_result = {{ "#/exclusiveMinimum", incompat_t::exclusive_minimum_increased}},
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 9})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .compat_result = {{"#/exclusiveMaximum", incompat_t::exclusive_maximum_decreased}},
  },
  // string checks
  {
    .reader_schema = R"({"type": "string", "minLength": 2})",
    .writer_schema = R"({"type": "string", "minLength": 1})",
    .compat_result = {{"#/minLength", incompat_t::min_length_increased}},
  },
  // string + pattern check: reader regex is a superset of writer regex, but
  // the rules specify to reject this
  {
    .reader_schema = R"({"type": "string", "pattern": "^test +"})",
    .writer_schema = R"({"type": "string", "pattern": "^test  +"})",
    .compat_result = {{"#/pattern", incompat_t::pattern_changed}},
  },
  // string + pattern check: pattern removed
  {
    .reader_schema = R"({"type": "string", "pattern": "^test +"})",
    .writer_schema = R"({"type": "string"})",
    .compat_result = {{"#/pattern", incompat_t::pattern_added}},
  },
  // enum checks
  {
    .reader_schema = R"({"type": "integer", "enum": [1, 2, 4]})",
    .writer_schema = R"({"type": "integer", "enum": [4, 1, 3]})",
    // Note: this is reported as combined_type_subschemas_changed by the reference impl
    .compat_result = {{"#/enum",incompat_t::enum_array_changed}},
  },
  // objects checks: size increase is not allowed
  {
    .reader_schema = R"({"type": "object", "minProperties": 2, "maxProperties": 10})",
    .writer_schema = R"({"type": "object", "maxProperties": 11})",
    .compat_result = {
      {"#/minProperties", incompat_t::min_properties_added},
      {"#/maxProperties", incompat_t::max_properties_decreased},
    },
  },
  // objects checks: additional properties not compatible
  {
    .reader_schema = R"({"type": "object", "additionalProperties": false})",
    .writer_schema
    = R"({"type": "object", "additionalProperties": {"type": "string"}})",
    .compat_result = {{"#/additionalProperties", incompat_t::additional_properties_removed}},
  },
  {
    .reader_schema
    = R"({"type": "object", "additionalProperties": {"type": "string"}})",
    .writer_schema = R"({"type": "object" })",
    .compat_result = {{"#/additionalProperties", incompat_t::additional_properties_narrowed}},
  },
  // object checks: new properties not compatible for a closed reader
  {
    .reader_schema = R"({"type": "object", "additionalProperties": false})",
    .writer_schema
    = R"({"type": "object", "properties": {"a": {"type": "null"}}, "additionalProperties": false})",
    .compat_result = {{"#/properties/a", incompat_t::property_removed_from_closed_content_model}},
  },
  // object checks: existing properties need to be compatible
  {
    .reader_schema
    = R"({"type": "object", "properties": {"aaaa": {"type": "integer"}}})",
    .writer_schema
    = R"({"type": "object", "properties": {"aaaa": {"type": "string"}}})",
    .compat_result = {{"#/properties/aaaa", incompat_t::type_changed}},
  },
  // object checks: new properties need to be compatible
  {
    .reader_schema
    = R"({"type": "object", "patternProperties": {"^a": {"type": "integer"}}})",
    .writer_schema
    = R"({"type": "object", "properties": {"aaaa": {"type": "string"}}})",
    .compat_result = {
      {"#/properties/aaaa", incompat_t::type_changed},
      {"#/properties/aaaa", incompat_t::property_removed_not_covered_by_partially_open_content_model}
    },
  },
  // object checks: new properties need to be compatible with all old patternProperties
  {
    .reader_schema
    = R"({"type": "object", "patternProperties": {"^a": {"type": "integer"}, "^b": {"type": "integer"}}})",
    .writer_schema
    = R"({"type": "object", "properties": {"bbbb": {"type": "string"}}})",
    .compat_result = {
      {"#/properties/bbbb", incompat_t::type_changed},
      {"#/properties/bbbb", incompat_t::property_removed_not_covered_by_partially_open_content_model}
    },
  },
  // object checks: dependencies removed
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"], "b": ["a"]}
})",
    .writer_schema = R"({"type": "object"})",
    .compat_result = {
      {"#/dependencies/a", incompat_t::dependency_array_added},
      {"#/dependencies/b", incompat_t::dependency_array_added},
    },
  },
  // object checks: dependencies missing members
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"], "b": ["a"]}
})",
    .writer_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"]}
})",
    .compat_result = {{"#/dependencies/b", incompat_t::dependency_array_added}},
  },
  // object checks: dependencies missing value in string array
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b", "c"]}
})",
    .writer_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"]}
})",
    .compat_result = {{"#/dependencies/a", incompat_t::dependency_array_extended}},
  },
  // object checks: dependencies incompatible schemas
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": {"type": "integer"}}
})",
    .writer_schema = R"(
{
  "type": "object",
  "dependencies": {"a": {"type": "number"}}
})",
    .compat_result = {{"#/dependencies/a", incompat_t::type_narrowed}},
  },
  // object checks: dependency "a" changed from array to object
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"]}
})",
    .writer_schema = R"(
{
  "type": "object",
  "dependencies": {"a": {"type": "number"}}
})",
    .compat_result = {{"#/dependencies/a", incompat_t::dependency_array_added}},
  },
  // object checks: dependency "a" changed from object to array
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": {"type": "number"}}
})",
    .writer_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"]}
})",
    .compat_result = {{"#/dependencies/a", incompat_t::dependency_schema_added}},
  },
  // object checks: dependency "a" changed from array to object
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"]}
})",
    .writer_schema = R"(
{
  "type": "object",
  "dependencies": {"a": {"type": "number"}}
})",
    .compat_result = {{"#/dependencies/a", incompat_t::dependency_array_added}},
  },
  // object checks: dependency "a" changed from object to array
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": {"type": "number"}}
})",
    .writer_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"]}
})",
    .compat_result = {{"#/dependencies/a", incompat_t::dependency_schema_added}},
  },
  // array checks: size increase is not allowed
  {
    .reader_schema = R"({"type": "array", "minItems": 2, "maxItems": 10})",
    .writer_schema = R"({"type": "array", "maxItems": 11})",
    .compat_result = {
      {"#/minItems", incompat_t::min_items_added},
      {"#/maxItems", incompat_t::max_items_decreased},
    },
  },
  // array checks: uniqueItems must be compatible
  {
    .reader_schema = R"({"type": "array", "uniqueItems": true})",
    .writer_schema = R"({"type": "array"})",
    .compat_result = {{"#/uniqueItems", incompat_t::unique_items_added}},
  },
  // array checks: "items" = schema must be superset
  {
    .reader_schema = R"({"type": "array", "items": {"type": "boolean"}})",
    .writer_schema = R"({"type": "array", "items": {"type": "integer"}})",
    .compat_result = {{"#/items", incompat_t::type_changed}},
  },
  // array checks: "items": array schema should be compatible
  {
    .reader_schema = R"({"type": "array", "items": [{"type":"boolean"}]})",
    .writer_schema = R"({"type": "array", "items": [{"type":"integer"}]})",
    .compat_result = {{"#/items/0", incompat_t::type_changed}},
  },
  // array checks: "items": array schema additionalItems should be compatible
  {
    .reader_schema
    = R"({"type": "array", "additionalItems": {"type": "boolean"}, "items": [{"type":"boolean"}]})",
    .writer_schema
    = R"({"type": "array", "additionalItems": {"type": "integer"}, "items": [{"type":"boolean"}]})",
    .compat_result = {{"#/additionalItems", incompat_t::type_changed}},
  },
  // array checks: "items": array schema extra elements should be compatible
  {
    .reader_schema
    = R"({"type": "array", "additionalItems": {"type": "integer"}, "items": [{"type":"boolean"}]})",
    .writer_schema
    = R"({"type": "array", "additionalItems": false, "items": [{"type":"boolean"}, {"type": "number"}]})",
    .compat_result = {
      {"#/items/1", incompat_t::type_narrowed},
      {"#/items/1", incompat_t::item_removed_not_covered_by_partially_open_content_model},
    },
  },
  {
    .reader_schema
    = R"({"type": "array", "additionalItems": {"type": "number"}, "items": [{"type":"boolean"}, {"type": "integer"}]})",
    .writer_schema
    = R"({"type": "array", "additionalItems": {"type": "number"}, "items": [{"type":"boolean"}]})",
    .compat_result = {
      {"#/items/1", incompat_t::type_narrowed},
      {"#/items/1", incompat_t::item_added_not_covered_by_partially_open_content_model},
    },
  },
  // combinators: "not" is required on both schema
  {
    .reader_schema
    = R"({"not": {"type": ["string", "integer", "number", "object", "array", "null"]}})",
    .writer_schema = R"({"type": "boolean"})",
    .compat_result = {{"#/", incompat_t::type_changed}},
  },
  // combinators: "not" subschema has to get less strict
  {
    .reader_schema
    = R"({"type": ["integer", "number"], "not": {"type": "number"}})",
    .writer_schema
    = R"({"type": ["integer", "number"], "not": {"type": "integer"}})",
    // Note: this is reported as combined_type_changed at '#/' by the reference impl
    .compat_result = {{"#/not", incompat_t::not_type_extended}},
  },
  // positive combinators: multiple combs
  {
    .reader_schema = R"({"type": "integer", "oneOf": [true], "anyOf": [true]})",
    .writer_schema = R"({"type": "integer"})",
    .expected_exception = true,
  },
  // positive combinators: mismatch of type
  {
    .reader_schema = R"({"type": "integer", "anyOf": [true]})",
    .writer_schema = R"({"type": "integer"})",
    // Note: this is reported as type_changed by the reference impl
    .compat_result = {{"#/", incompat_t::combined_type_changed}},
  },
  // positive combinators: mismatch of size
  {
    .reader_schema = R"({"allOf": [true, false]})",
    .writer_schema = R"({"allOf": [true]})",
    .compat_result = {{"#/", incompat_t::product_type_extended}},
  },
  {
    .reader_schema = R"({"anyOf": [true]})",
    .writer_schema = R"({"anyOf": [true, false]})",
    .compat_result = {{"#/", incompat_t::sum_type_narrowed}},
  },
  {
    .reader_schema = R"({"oneOf": [true]})",
    .writer_schema = R"({"oneOf": [true, false]})",
    .compat_result = {{"#/", incompat_t::sum_type_narrowed}},
  },
  // positive combinators: subschema mismatch
  {
    // note: this fails because for writer, there isn't s distinct matching
    // schema in reader
    .reader_schema = R"({"oneOf": [{"type":"number"}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"oneOf": [{"type":"number", "multipleOf": 10}, {"type": "number", "multipleOf": 1.1}]})",
    // Note: this is reported as combined_type_changed by the reference impl
    .compat_result = {{"#/", incompat_t::combined_type_subschemas_changed}},
  },
  // object checks: removing a required property not ok if didn't have a default value
  {
    .reader_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {"type": "integer"}
  },
  "required": ["a"]
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {"type": "integer"}
  }
})",
    .compat_result = {{"#/required/a", incompat_t::required_attribute_added}},
  },
  // Both type-specific and combined checks fail
  {
    .reader_schema = R"(
{
  "type": ["number", "object"],
  "minimum": 20,
  "minProperties": 10,
  "oneOf": [
    { "multipleOf": 10 },
    { "multipleOf": 3 }
  ]
})",
    .writer_schema = R"(
{
  "type": ["number", "object"],
  "minimum": 10,
  "oneOf": [
    { "multipleOf": 5 },
    { "multipleOf": 3 }
  ]
})",
    // Note: the reference implementation only reports combined_type_subschemas_changed here
    .compat_result = {{"#/minimum", incompat_t::minimum_increased}, {"#/minProperties", incompat_t::min_properties_added}, {"#/", incompat_t::combined_type_subschemas_changed}},
  },

  //***** compatible section *****
  // atoms
  {
    .reader_schema = "true",
    .writer_schema = "false",
    .compat_result = {},
  },
  // same type
  {
    .reader_schema = R"({"type": "boolean"})",
    .writer_schema = R"({"type": "boolean"})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "null"})",
    .writer_schema = R"({"type": "null"})",
    .compat_result = {},
  },
  // restrict types
  {
    .reader_schema = R"({"type": ["null", "boolean"]})",
    .writer_schema = R"({"type": "null"})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": ["number", "boolean"], "minimum": 10.2})",
    .writer_schema = R"({"type": ["integer", "boolean"], "minimum": 11})",
    .compat_result = {},
  },
  // numeric checks
  {
    .reader_schema = R"({"type": "number"})",
    .writer_schema
    = R"({"type": "number", "minimum": 11, "exclusiveMinimum": true})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number"})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .compat_result = {},
  },
  {
    .reader_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": false})",
    .writer_schema = R"({"type": "number", "maximum": 10})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMinimum": 11})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 8})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number"})",
    .writer_schema = R"({"type": "number", "maximum": 11})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "minimum": 1.1, "maximum": 4})",
    .writer_schema = R"({"type": "number", "minimum": 1.1, "maximum": 3.2})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10})",
    .writer_schema = R"({"type": "number", "multipleOf": 20})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10.1})",
    .writer_schema = R"({"type": "number", "multipleOf": 20.2})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 1.1})",
    .writer_schema = R"({"type": "number", "multipleOf": 3.3})",
    .compat_result = {},
  },
  // string checks
  {
    .reader_schema = R"({"type": "string", "minLength": 0})",
    .writer_schema = R"({"type": "string"})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "string"})",
    .writer_schema = R"({"type": "string", "minLength": 1, "maxLength": 10})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "string", "minLength": 1})",
    .writer_schema = R"({"type": "string", "minLength": 2})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"type": "string", "pattern": "^test"})",
    .writer_schema = R"({"type": "string", "pattern": "^test"})",
    .compat_result = {},
  },
  // metadata is ignored
  {
    .reader_schema
    = R"({"title": "myschema", "description": "this is my schema", "default": true, "type": "boolean"})",
    .writer_schema
    = R"({"title": "MySchema", "description": "this schema is mine", "default": false, "type": "boolean"})",
    .compat_result = {},
  },
  // enum checks
  {
    .reader_schema = R"({"type": "integer", "enum": [1, 2, 4]})",
    .writer_schema = R"({"type": "integer", "enum": [4, 1]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"(
{
  "type": "object",
  "additionalProperties": {"type": "boolean"}
})",
    .writer_schema = R"(
{
  "type": "object",
  "additionalProperties": false
})",
    .compat_result = {},
  },
  // objects checks:
  // - size decrease
  // - properties change and new properties added
  // - patternProperties evolve
  // - additionalProperties evolve
  // - required list increase
  // - dependencies list increase
  {
    .reader_schema = R"(
{
  "type": "object",
  "minProperties": 2,
  "maxProperties": 10,
  "properties": {
    "aaaa": {"type": "number"}
  },
  "patternProperties": {
    "^b": {"type": "string"}
  },
  "additionalProperties": {"type": "boolean"},
  "required": ["aaaa"],
  "dependencies": {"a":["c", "b"], "b": {"type": "number"}}
})",
    .writer_schema = R"(
{
  "type": "object",
  "minProperties": 2,
  "maxProperties": 9,
  "properties": {
    "aaaa": {"type": "integer"},
    "bbbb": {"type": "string"},
    "cccc": {"type": "boolean"}
  },
  "patternProperties": {
    "^b": {"type": "string", "minLength":10}
  },
  "additionalProperties": false,
  "required": ["aaaa", "cccc"],
  "dependencies": {"a": ["b", "c", "d"], "b": {"type": "integer"}, "d": ["a"]}
})",
    .compat_result = {},
  },
  // object checks: a new required property is ok if it has a default value
  {
    .reader_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "integer"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "integer",
      "default": 10
    }
  },
  "required": [
    "a"
  ]
})",
    .compat_result = {},
  },
  // array checks: same
  {
    .reader_schema = R"({"type": "array"})",
    .writer_schema = R"({"type": "array"})",
    .compat_result = {},
  },
  // array checks: uniqueItems explicit value to false
  {
    .reader_schema = R"({"type": "array", "uniqueItems": false})",
    .writer_schema = R"({"type": "array"})",
    .compat_result = {},
  },
  // array checks:
  // - size decrease
  // - items change and new items added
  {
    .reader_schema = R"(
  {
    "type": "array",
    "minItems": 2,
    "maxItems": 10,
    "items": [
      {
        "type": "array",
        "items": {"type": "boolean"}
      }
    ],
    "additionalItems": {"type": "number"}
  })",
    .writer_schema = R"(
  {
    "type": "array",
    "minItems": 2,
    "maxItems": 9,
    "items": [
      {
        "type": "array",
        "items": {"type": "boolean"},
        "uniqueItems": true
      },
      {"type": "integer"}
    ],
    "additionalItems": {"type": "integer"}
  })",
    .compat_result = {},
  },
  // array checks: excess elements are absorbed by additionalItems
  {
    .reader_schema = R"(
{
  "type": "array",
  "items": [
    {
      "type": "boolean"
    }
  ],
  "additionalItems": {
    "type": "number"
  }
})",
    .writer_schema = R"(
{
  "type": "array",
  "items": [
    {
      "type": "boolean"
    },
    {
      "type": "integer"
    }
  ],
  "additionalItems": false
})",
    .compat_result = {},
  },
  {
    .reader_schema = R"(
{
  "type": "array",
  "items": [
    {
      "type": "boolean"
    },
    {
      "type": "number"
    }
  ],
  "additionalItems": {"type": "number"}
})",
    .writer_schema = R"(
{
  "type": "array",
  "items": [
    {
      "type": "boolean"
    }
  ],
  "additionalItems": {
    "type": "integer"
  }
})",
    .compat_result = {},
  },
  {
    .reader_schema = R"(
{
  "type": "array",
  "items": [
    {
      "type": "boolean"
    },
    {
      "type": "number"
    }
  ],
  "additionalItems": false
})",
    .writer_schema = R"(
{
  "type": "array",
  "items": [
    {
      "type": "boolean"
    }
  ],
  "additionalItems": false
})",
    .compat_result = {},
  },
  // array checks: prefixItems/items are compatible across drafts
  {
    .reader_schema = R"({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "array",
            "prefixItems": [
                {
                    "type": "integer"
                }
            ],
            "items": false
        })",

    .writer_schema = R"({
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "array",
            "items": [
                {
                    "type": "integer"
                }
            ],
            "additionalItems": false
        })",
    .compat_result = {},
  },
  {
    .reader_schema = R"({
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "array",
            "items": [
                {
                    "type": "integer"
                }
            ],
            "additionalItems": false
        })",
    .writer_schema = R"({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "array",
            "prefixItems": [
                {
                    "type": "integer"
                }
            ],
            "items": false
        })",

    .compat_result = {},
  },
  // combinators: "not"
  {
    .reader_schema
    = R"({"type": "integer", "not": {"type": "integer", "minimum": 10}})",
    .writer_schema
    = R"({"type": "integer", "not": {"type": "integer", "minimum": 5}})",
    .compat_result = {},
  },
  // positive combinators
  {
    .reader_schema
    = R"({"oneOf": [{"type":"number", "multipleOf": 3}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"oneOf": [{"type":"boolean"}, {"type": "number", "multipleOf": 9}]})",
    .compat_result = {},
  },
  // positive combinators special case: only writer has a combinator
  {
    .reader_schema = R"({"type": "object"})",
    .writer_schema = R"(
        {"type": "object",
            "oneOf": [
                {"type": "object", "properties": {"c": {"type": "string"}}},
                {"type": "object", "properties": {"d": {"type": "array"}}}
            ]
        })",
    .compat_result = {},
  },
  // positive combinators single schema cases: the actual combinator does not
  // matter
  {
    .reader_schema = R"({"anyOf": [{"type": "number"}]})",
    .writer_schema = R"({"oneOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"allOf": [{"type": "number"}]})",
    .writer_schema = R"({"oneOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}]})",
    .writer_schema = R"({"anyOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"allOf": [{"type": "number"}]})",
    .writer_schema = R"({"anyOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"anyOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  // positive combinators older is single schema, newer is allOf
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}, {"type": "string"}]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"anyOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}, {"type": "string"}]})",
    .compat_result = {},
  },
  // positive combinators older is oneOf, newer is single schema
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema = R"({"anyOf": [{"type": "integer"}]})",
    .compat_result = {},
  },
  // positive combinators special cases: anyOf in reader can be compared with
  // the other
  // combinators
  {
    // smoke test identical schemas
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .compat_result = {},
  },
  {
    // smoke test smaller reader is not compatible
    .reader_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .compat_result = {{"#/", incompat_t::sum_type_narrowed}},
  },
  {
    // smoke test bigger reader is  compatible
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .compat_result = {},
  },
  {
    // oneOf in writer can only be smaller or equal
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"oneOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .compat_result = {},
  },
  {
    // oneOf in writer can only be smaller or equal
    .reader_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema
    = R"({"oneOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .compat_result = {{"#/", incompat_t::sum_type_narrowed}},
  },
  {
    // oneOf in writer can only be smaller or equal
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema = R"({"oneOf": [{"type": "number"}, {"type": "string"}]})",
    .compat_result = {},
  },
  {
    // allOf in writer can be compatible with any cardinality
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"allOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .compat_result = {},
  },
  {
    // allOf in writer can be compatible with any cardinality
    .reader_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema
    = R"({"allOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .compat_result = {},
  },
  {
    // allOf in writer can be compatible with any cardinality
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema = R"({"allOf": [{"type": "number"}, {"type": "string"}]})",
    .compat_result = {},
  },
  // dialects
  {
    .reader_schema = R"({"$schema": "http://json-schema.org/draft-06/schema"})",
    .writer_schema
    = R"({"$schema": "http://json-schema.org/draft-06/schema#"})",
    .compat_result = {},
  },
  // different dialects
  {
    .reader_schema = R"({"$schema": "http://json-schema.org/draft-04/schema"})",
    .writer_schema
    = R"({"$schema": "http://json-schema.org/draft-06/schema#"})",
    .compat_result = {},
  },
  {
    .reader_schema = R"({"$schema": "http://json-schema.org/draft-04/schema"})",
    .writer_schema = "true",
    .compat_result = {},
  },
  // array checks: prefixItems/items are compatible across drafts, unspecified
  // schema
  {
    .reader_schema = R"({
            "type": "array",
            "prefixItems": [
                {
                    "type": "integer"
                }
            ],
            "items": false
        })",

    .writer_schema = R"({
            "type": "array",
            "items": [
                {
                    "type": "integer"
                }
            ],
            "additionalItems": false
        })",
    .compat_result = {},
  },
  {
    .reader_schema = R"({
            "type": "array",
            "items": [
                {
                    "type": "integer"
                }
            ],
            "additionalItems": false
        })",
    .writer_schema = R"({
            "type": "array",
            "prefixItems": [
                {
                    "type": "integer"
                }
            ],
            "items": false
        })",

    .compat_result = {},
  },
  // object: "patternProperties" is not involved in compat checks
  {
    .reader_schema = R"(
{
  "type": "object",
  "patternProperties": {
    "^a": {
      "type": "boolean"
    },
    "^b": {
      "type": "integer"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "patternProperties": {
    "^a": {
      "type": "null"
    },
    "^c": {
      "type": "string"
    }
  }
})",
    .compat_result = {},
  },
  /* internal refs checks section */
  {
// simple fragment ref
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  },
  "properties": {
    "a": {
      "$ref": "#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
  },
  {
// simple fragment ref incompatible
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  },
  "properties": {
    "a": {
      "$ref": "#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMaximum": 0
    }
  }
})",
    .compat_result = {{"#/properties/a/exclusiveMinimum", incompat_t::exclusive_minimum_added}},
  },
{
// simple full relative ref
    .reader_schema = R"(
{
  "type": "object",
  "$id": "https://example.com/schemas/positive_num",
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  },
  "properties": {
    "a": {
      "$ref": "/schemas/positive_num#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
  },
  {
// simple full relative ref incompatible
    .reader_schema = R"(
{
  "type": "object",
  "$id": "https://example.com/schemas/positive_num",
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  },
  "properties": {
    "a": {
      "$ref": "/schemas/positive_num#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMaximum": 0
    }
  }
})",
    .compat_result = {{"#/properties/a/exclusiveMinimum", incompat_t::exclusive_minimum_added}},
  },
  {
// simple partial relative ref
.reader_schema = R"(
{
  "type": "object",
  "$id": "https://example.com/schemas/positive_num",
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  },
  "properties": {
    "a": {
      "$ref": "positive_num#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
  },
  {
// simple partial relative ref incompatible
    .reader_schema = R"(
{
  "type": "object",
  "$id": "https://example.com/schemas/positive_num",
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  },
  "properties": {
    "a": {
      "$ref": "positive_num#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMaximum": 0
    }
  }
})",
    .compat_result = {{"#/properties/a/exclusiveMinimum", incompat_t::exclusive_minimum_added}},
  },
  {
// simple infinite recursive ref
    .reader_schema = R"(
{
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "children": {
      "type": "array",
      "items": { "$ref": "#" }
    }
  }
}
)",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "children": {
      "type": "array",
      "items": { "$ref": "#" }
    }
  }
})",
    .compat_result = {},
    .expected_exception = true,
  },
  {
// simple multiple recursive ref
    .reader_schema = R"(
{
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "a": { "$ref": "#/properties/name" },
    "b": { "$ref": "#/properties/name" },
    "c": { "$ref": "#/properties/name" },
    "d": { "$ref": "#/properties/name" },
    "e": { "$ref": "#/properties/name" },
    "f": { "$ref": "#/properties/name" },
    "g": { "$ref": "#/properties/name" },
    "h": { "$ref": "#/properties/name" },
    "i": { "$ref": "#/properties/name" },
    "l": { "$ref": "#/properties/name" },
    "m": { "$ref": "#/properties/name" },
    "n": { "$ref": "#/properties/name" },
    "o": { "$ref": "#/properties/name" },
    "p": { "$ref": "#/properties/name" },
    "q": { "$ref": "#/properties/name" },
    "r": { "$ref": "#/properties/name" },
    "s": { "$ref": "#/properties/name" },
    "t": { "$ref": "#/properties/name" },
    "u": { "$ref": "#/properties/name" },
    "v": { "$ref": "#/properties/name" },
    "z": { "$ref": "#/properties/name" },
    "j": { "$ref": "#/properties/name" },
    "k": { "$ref": "#/properties/name" },
    "w": { "$ref": "#/properties/name" },
    "y": { "$ref": "#/properties/name" }
  }
}
)",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "a": { "$ref": "#/properties/name" },
    "b": { "$ref": "#/properties/name" },
    "c": { "$ref": "#/properties/name" },
    "d": { "$ref": "#/properties/name" },
    "e": { "$ref": "#/properties/name" },
    "f": { "$ref": "#/properties/name" },
    "g": { "$ref": "#/properties/name" },
    "h": { "$ref": "#/properties/name" },
    "i": { "$ref": "#/properties/name" },
    "l": { "$ref": "#/properties/name" },
    "m": { "$ref": "#/properties/name" },
    "n": { "$ref": "#/properties/name" },
    "o": { "$ref": "#/properties/name" },
    "p": { "$ref": "#/properties/name" },
    "q": { "$ref": "#/properties/name" },
    "r": { "$ref": "#/properties/name" },
    "s": { "$ref": "#/properties/name" },
    "t": { "$ref": "#/properties/name" },
    "u": { "$ref": "#/properties/name" },
    "v": { "$ref": "#/properties/name" },
    "z": { "$ref": "#/properties/name" },
    "j": { "$ref": "#/properties/name" },
    "k": { "$ref": "#/properties/name" },
    "w": { "$ref": "#/properties/name" },
    "y": { "$ref": "#/properties/name" }
  }
}
)",
    .compat_result = {},
  },
{
// simple ref in bundled schema compatible
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "type": "number",
          "exclusiveMinimum": 0
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
  },
{
// simple ref in bundled schema incompatible
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "type": "number",
          "exclusiveMinimum": 0
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMaximum": 0
    }
  }
})",
    .compat_result = {{"#/properties/a/exclusiveMinimum", incompat_t::exclusive_minimum_added}},
  },
{
// double jump ref in bundled schema compatible
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "$ref": "#/$defs/pnum_impl"
        },
        "pnum_impl": {
          "type": "number",
          "exclusiveMinimum": 0
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
  },
  {
    // double jump ref in bundled schema incompatible
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "$ref": "#/$defs/pnum_impl"
        },
        "pnum_impl": {
          "type": "number",
          "exclusiveMinimum": 0
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMaximum": 0
    }
  }
})",
    .compat_result = {{"#/properties/a/exclusiveMinimum", incompat_t::exclusive_minimum_added}},
  },
  {
    // recursive double jump ref in bundled schema error
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "$ref": "#/$defs/pnum_impl"
        },
        "pnum_impl": {
          "$ref": "#/$defs/positive_num"
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
    .expected_exception = true,
  },
  {
    // recursive double jump ref with siblings in bundled schema error
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "$ref": "#/$defs/pnum_impl",
          "minimum": 0
        },
        "pnum_impl": {
          "$ref": "#/$defs/positive_num",
          "exclusiveMinimum": 0
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
    .expected_exception = true,
  },
  {
    // non existing bundled schema
    .reader_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
    .expected_exception = true,
  },
  {
    // non existing external ref
    .reader_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "$ref": "an.external.schema#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
    .expected_exception = true,
  },
  {
    // non existing local ref in bundled schema
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "$ref": "#/$defs/pnum_impl"
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num"
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  }
})",
    .compat_result = {},
    .expected_exception = true,
  },
  {
    // local ref with siblings
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    }
  },
  "properties": {
    "a": {
      "$ref": "#/$defs/positive_num",
      "type": "number",
      "multipleOf": 33
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "allOf": [
        {
          "type": "number",
          "exclusiveMinimum": 0
        },
        {
          "type": "number",
          "multipleOf": 33
        }
      ]
    }
  }
})",
    .compat_result = {},
  },
  {
    // recursive double jump ref with siblings in bundled schema
    .reader_schema = R"(
{
  "type": "object",
  "$defs": {
    "positive_num": {
      "$id": "https://example.com/schemas/external",
      "$defs": {
        "positive_num": {
          "$ref": "#/$defs/pnum_impl"
        },
        "pnum_impl": {
          "type": "number",
          "exclusiveMinimum": 0
        }
      }
    }
  },
  "properties": {
    "a": {
      "$ref": "https://example.com/schemas/external#/$defs/positive_num",
      "type": "number",
      "multipleOf": 33
    }
  }
})",
    .writer_schema = R"(
{
  "type": "object",
  "properties": {
    "a": {
      "allOf": [
        {
          "type": "number",
          "exclusiveMinimum": 0
        },
        {
          "type": "number",
          "multipleOf": 33
        }
      ]
    }
  }
})",
    .compat_result = {},
  },
  {
    // ref in arrays
    .reader_schema = R"(
{
  "$id": "https://example.com/schemas/base",
  "type": "array",
  "prefixItems": [
    {"$ref": "#/$defs/positive_num"},
    {"type": "string"}
  ],
  "items": {"$ref": "https://example.com/schemas/base#/$defs/negative_num", "mutipleOf": 10},
  "$defs": {
    "positive_num": {
      "type": "number",
      "exclusiveMinimum": 0
    },
    "negative_num": {
      "type": "number",
      "exclusiveMaximum": 0
    }
  }
})",
    .writer_schema = R"(
{
  "type": "array",
  "prefixItems": [
    {"type": "number", "exclusiveMinimum": 0},
    {"type": "string"}
  ],
  "items": {
    "allOf": [
      {"type": "number", "exclusiveMaximum": 0},
      {"multipleOf": 10}
    ]
  }
})",
  .compat_result = {},
  },
});
SEASTAR_THREAD_TEST_CASE(test_compatibility_check) {
    store_fixture f;
    auto make_json_schema = [&](std::string_view schema) {
        return pps::make_json_schema_definition(
                 f.store,
                 pps::make_canonical_json_schema(
                   f.store,
                   {pps::subject{"test"}, {schema, pps::schema_type::json}})
                   .get())
          .get();
    };
    for (const auto& data : compatibility_test_cases) {
        BOOST_TEST_CONTEXT(fmt::format(
          "reader: {}, writer: {}, is compatible: {}",
          data.reader_schema,
          data.writer_schema,
          data.compat_result.empty())) {
            try {
                // sanity check that each schema is compatible with itself
                BOOST_CHECK_MESSAGE(
                  ::check_compatible(
                    make_json_schema(data.reader_schema),
                    make_json_schema(data.reader_schema)),
                  fmt::format(
                    "reader '{}' should be compatible with itself",
                    data.reader_schema));
                BOOST_CHECK_MESSAGE(
                  ::check_compatible(
                    make_json_schema(data.writer_schema),
                    make_json_schema(data.writer_schema)),
                  fmt::format(
                    "writer '{}' should be compatible with itself",
                    data.writer_schema));

                // check compatibility (or not) reader->writer
                auto expected = [&data] {
                    pps::raw_compatibility_result res{};
                    for (auto inc : data.compat_result) {
                        res.emplace<incompatibility>(inc);
                    }
                    return std::move(res)(pps::verbose::yes);
                };

                BOOST_CHECK_EQUAL(
                  expected(),
                  pps::check_compatible(
                    make_json_schema(data.reader_schema),
                    make_json_schema(data.writer_schema),
                    pps::verbose::yes));
            } catch (...) {
                BOOST_CHECK_MESSAGE(
                  data.expected_exception,
                  fmt::format(
                    "terminated with exception {}", std::current_exception()));
                continue;
            }
            BOOST_CHECK_MESSAGE(!data.expected_exception, "no exception");
        }
    };
}

namespace {

const auto schema_old = pps::canonical_schema_definition({
  R"(
{
  "type": "object",
  "minProperties": 2,
  "maxProperties": 9,
  "properties": {
    "aaaa": {"type": "integer"},
    "bbbb": {"type": "string"},
    "cccc": {"type": "boolean"}
  },
  "patternProperties": {
    "^b": {"type": "string", "minLength":10}
  },
  "additionalProperties": false,
  "required": ["aaaa", "cccc"],
  "dependencies": {"a": ["b", "c", "d"], "b": {"type": "integer"}, "d": ["a"]}
})",
  pps::schema_type::json});

const auto schema_new = pps::canonical_schema_definition({
  R"(
{
  "type": "object",
  "minProperties": 3,
  "maxProperties": 10,
  "properties": {
    "aaaa": {"type": "number"}
  },
  "patternProperties": {
    "^b": {"type": "string"}
  },
  "additionalProperties": {"type": "boolean"},
  "required": ["aaaa"],
  "dependencies": {"a":["c", "b"], "b": {"type": "number"}}
})",
  pps::schema_type::json});

const absl::flat_hash_set<incompatibility> forward_expected{
  {"#/properties/aaaa", incompat_t::type_narrowed},
  // TODO: implement the check for
  // required_property_added_to_unopen_content_model. That is also expected
  // for this schema update.
  // {"#/properties/cccc",
  //  incompat_t::required_property_added_to_unopen_content_model},
  {"#/dependencies/a", incompat_t::dependency_array_extended},
  {"#/dependencies/d", incompat_t::dependency_array_added},
  {"#/dependencies/b", incompat_t::type_narrowed},
  {"#/additionalProperties", incompat_t::additional_properties_removed},
  {"#/maxProperties", incompat_t::max_properties_decreased},
};
const absl::flat_hash_set<incompatibility> backward_expected{
  {"#/minProperties", incompat_t::min_properties_increased},
};

const auto compat_data = std::to_array<compat_test_data<incompatibility>>({
  {
    schema_old.share(),
    schema_new.share(),
    forward_expected,
  },
  {
    schema_new.share(),
    schema_old.share(),
    backward_expected,
  },
});

std::string format_set(const absl::flat_hash_set<ss::sstring>& d) {
    return fmt::format("{}", fmt::join(d, "\n"));
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_json_compat_messages) {
    for (const auto& cd : compat_data) {
        auto compat = check_compatible_verbose(cd.reader, cd.writer);

        absl::flat_hash_set<ss::sstring> errs{
          compat.messages.begin(), compat.messages.end()};
        absl::flat_hash_set<ss::sstring> expected{
          cd.expected.messages.begin(), cd.expected.messages.end()};

        BOOST_CHECK(!compat.is_compat);
        BOOST_CHECK_EQUAL(errs.size(), expected.size());
        BOOST_REQUIRE_MESSAGE(
          errs == expected,
          fmt::format("{} != {}", format_set(errs), format_set(expected)));
    }
}

SEASTAR_THREAD_TEST_CASE(test_refs_fixing) {
    // test that look check that in the in-memory representation of a schema,
    // the refs are absolute
    using namespace jsoncons::literals;

    // input schema with a mix of local, relative and absolute refs
    auto input_schema = R"({
  // root schema has no id
  "properties": {
    // local ref
    "giga": {"$ref": "#/properties/mega"},
    // absolute ref to bundled schema
    "mega": {"$ref": "https://example.com/schemas/customer#/properties/local"}
  },
  "$defs": {
    "bundled": {
      // $id makes it a bundled schema
      "$id": "https://example.com/schemas/customer",
      "properties": {
        // relative local ref to this bundled schema
        "local": { "$ref": "#/$defs/name" },

        // absolute ref to this bundled schema
        "local_absolute": { "$ref": "https://example.com/schemas/customer#/$defs/name" },

        // relative ref to another bundled schema, with uri relative to this bundled schema host
        "another_schema": { "$ref": "/schemas/address" },

        // absolute ref to another bundled schema, with fragment
        "another_schemas_absolute": { "$ref": "https://example.com/schemas/address#/name" },

        // relative version of the above
        "another_schema_frag": { "$ref": "/schemas/address#/name" },

        // recursive ref
        "recursive": { "$ref": "#/properties/recursive" }
      },
      "$defs": {
        "bundled": {
          // this makes it a bundled schema (in a bundled schema)
          "$id" : "https://example.com/schemas/boat",
          "properties": {
            // local to this bundled schema
            "self": { "$ref": "#/$defs/name" }
          },
          "$defs": {
            "bundled": {
              // bundled schema with a relative id.
              // NOTE: this will be resolved against example.com,
              // but the value will not be rewritten in the in-memory schema
              "$id": "/schemas/nanites"
            }
          }
        }
      }
    }
  }
}
)"_json;

    store_fixture f;
    // get a schema definiton from the input schema, this will produce an
    // in-memory where refs are resolved
    auto json_schema_def
      = pps::make_json_schema_definition(
          f.store,
          pps::make_canonical_json_schema(
            f.store,
            {pps::subject{"test"},
             {fmt::format("{}", jsoncons::print(input_schema)),
              pps::schema_type::json}})
            .get())
          .get();

    // extract the in-memory schema to check te results
    auto inmemory_buffer = iobuf_parser{json_schema_def.raw()};
    auto processed_schema = jsoncons::json::parse(
      inmemory_buffer.read_string(inmemory_buffer.bytes_left()));

    // this is the expected in memory result
    auto expected_schema = R"({
  "properties": {
    "giga": {"$ref": "#/properties/mega"},
    "mega": {"$ref": "https://example.com/schemas/customer#/properties/local"}
  },
  "$defs": {
    "bundled": {
      "$id": "https://example.com/schemas/customer",
      "properties": {
        "local": { "$ref": "https://example.com/schemas/customer#/$defs/name" },
        "local_absolute": { "$ref": "https://example.com/schemas/customer#/$defs/name" },
        "another_schema": { "$ref": "https://example.com/schemas/address" },
        "another_schemas_absolute": { "$ref": "https://example.com/schemas/address#/name" },
        "another_schema_frag": { "$ref": "https://example.com/schemas/address#/name" },
        "recursive": { "$ref": "https://example.com/schemas/customer#/properties/recursive" }
      },
      "$defs": {
        "bundled": {
          "$id" : "https://example.com/schemas/boat",
          "properties": {
            "self": { "$ref": "https://example.com/schemas/boat#/$defs/name" }
          },
          "$defs": {
            "bundled": {
              // NOTE: $id is not rewritten, unlike $ref
              "$id": "/schemas/nanites"
            }
          }
        }
      }
    }
  }
}
)"_json;

    BOOST_TEST_CONTEXT(fmt::format(
      "input_schema:\n{}\n\nexpected_schema:\n{}\n\nprocessed_schema:\n{}\n\n",
      jsoncons::pretty_print(input_schema),
      jsoncons::pretty_print(expected_schema),
      jsoncons::pretty_print(processed_schema))) {
        // check that the processed schema is the same as the expected schema,
        // output the difference if not
        auto jpatch = jsoncons::jsonpatch::from_diff(
          expected_schema, processed_schema);
        BOOST_CHECK_MESSAGE(
          expected_schema == processed_schema,
          fmt::format(
            "differences expected_schema-processed_schema:\n{}\n",
            jsoncons::pretty_print(jpatch)));
    }
}
