// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/context.hpp>
#include <fmt/core.h>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

bool check_compatible(
  const pps::json_schema_definition& reader_schema,
  const pps::json_schema_definition& writer_schema) {
    return pps::check_compatible(reader_schema, writer_schema).is_compat;
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
    operator<<(std::ostream& os, error_test_case const& e) {
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
      "Malformed json schema: Missing a name for object member. at offset 1"}},
  error_test_case{
    "",
    pps::error_info{
      pps::error_code::schema_invalid,
      "Malformed json schema: The document is empty. at offset 0"}},
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
      R"(Invalid json schema: '{"$schema":"http://json-schema.org/draft-06/schema#","exclusiveMinimum":false,"minimum":0,"type":"number"}'. Error: '/exclusiveMinimum: Expected number, found boolean')"}},
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
            } catch (pps::exception const& e) {
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
    bool reader_is_compatible_with_writer;
    bool expected_exception = false;
};

static constexpr auto compatibility_test_cases = std::to_array<
  compatibility_test_case>({
  //***** not compatible section *****
  // atoms
  {
    .reader_schema = "false",
    .writer_schema = "true",
    .reader_is_compatible_with_writer = false,
  },
  // not allowed promotion
  {
    .reader_schema = R"({"type": "integer"})",
    .writer_schema = R"({"type": "number"})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": ["integer", "string"]})",
    .writer_schema = R"({"type": ["number", "string"]})",
    .reader_is_compatible_with_writer = false,
  },
  // not allowed new types
  {
    .reader_schema = R"({"type": "integer"})",
    .writer_schema = R"({})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": "boolean"})",
    .writer_schema = R"({"type": ["boolean", "null"]})",
    .reader_is_compatible_with_writer = false,
  },
  // not allowed numeric evolutions
  {
    .reader_schema = R"({"type": "number", "minimum": 11.2})",
    .writer_schema = R"({"type": "number", "maximum": 30})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": "number", "minimum": 1.1, "maximum": 3.199})",
    .writer_schema = R"({"type": "integer", "minimum": 1.1, "maximum": 3.2})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10})",
    .writer_schema = R"({"type": "number", "multipleOf": 21})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 20})",
    .writer_schema = R"({"type": "number", "multipleOf": 10})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10.1})",
    .writer_schema = R"({"type": "number", "multipleOf": 20.2001})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": true})",
    .writer_schema = R"({"type": "number", "maximum": 10})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .writer_schema = R"({"type": "number"})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": true})",
    .writer_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": 10})",
    .reader_is_compatible_with_writer = false,
    .expected_exception = true,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMinimum": 9})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 9})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .reader_is_compatible_with_writer = false,
  },
  // string checks
  {
    .reader_schema = R"({"type": "string", "minLength": 2})",
    .writer_schema = R"({"type": "string", "minLength": 1})",
    .reader_is_compatible_with_writer = false,
  },
  // string + pattern check: reader regex is a superset of writer regex, but
  // the rules specify to reject this
  {
    .reader_schema = R"({"type": "string", "pattern": "^test +"})",
    .writer_schema = R"({"type": "string", "pattern": "^test  +"})",
    .reader_is_compatible_with_writer = false,
  },
  // enum checks
  {
    .reader_schema = R"({"type": "integer", "enum": [1, 2, 4]})",
    .writer_schema = R"({"type": "integer", "enum": [4, 1, 3]})",
    .reader_is_compatible_with_writer = false,
  },
  // objects checks: size increase is not allowed
  {
    .reader_schema
    = R"({"type": "object", "minProperties": 2, "maxProperties": 10})",
    .writer_schema = R"({"type": "object", "maxProperties": 11})",
    .reader_is_compatible_with_writer = false,
  },
  // objects checks: additional properties not compatible
  {
    .reader_schema = R"({"type": "object", "additionalProperties": false})",
    .writer_schema
    = R"({"type": "object", "additionalProperties": {"type": "string"}})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema
    = R"({"type": "object", "additionalProperties": {"type": "string"}})",
    .writer_schema = R"({"type": "object" })",
    .reader_is_compatible_with_writer = false,
  },
  // object checks: new properties not compatible for a closed reader
  {
    .reader_schema = R"({"type": "object", "additionalProperties": false})",
    .writer_schema
    = R"({"type": "object", "properties": {"a": {"type": "null"}}, "additionalProperties": false})",
    .reader_is_compatible_with_writer = false,
  },
  // object checks: existing properties need to be compatible
  {
    .reader_schema
    = R"({"type": "object", "properties": {"aaaa": {"type": "integer"}}})",
    .writer_schema
    = R"({"type": "object", "properties": {"aaaa": {"type": "string"}}})",
    .reader_is_compatible_with_writer = false,
  },
  // object checks: new properties need to be compatible
  {
    .reader_schema
    = R"({"type": "object", "patternProperties": {"^a": {"type": "integer"}}})",
    .writer_schema
    = R"({"type": "object", "properties": {"aaaa": {"type": "string"}}})",
    .reader_is_compatible_with_writer = false,
  },
  // object checks: patternProperties need to be compatible
  {
    .reader_schema
    = R"({"type": "object", "patternProperties": {"^a": {"type": "integer"}}})",
    .writer_schema
    = R"({"type": "object", "patternProperties": {"^a": {"type": "number"}}})",
    .reader_is_compatible_with_writer = false,
  },
  // object checks: required needs to be compatible
  {
    .reader_schema
    = R"({"type": "object", "properties": {"a": {"type": "integer"}}})",
    .writer_schema
    = R"({"type": "object", "properties": {"a": {"type": "integer", "default": 10}}, "required": ["a"]})",
    .reader_is_compatible_with_writer = false,
  },
  // object checks: dependencies removed
  {
    .reader_schema = R"(
{
  "type": "object",
  "dependencies": {"a": ["b"], "b": ["a"]}
})",
    .writer_schema = R"({"type": "object"})",
    .reader_is_compatible_with_writer = false,
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
    .reader_is_compatible_with_writer = false,
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
    .reader_is_compatible_with_writer = false,
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
    .reader_is_compatible_with_writer = false,
  },
  // array checks: size increase is not allowed
  {
    .reader_schema = R"({"type": "array", "minItems": 2, "maxItems": 10})",
    .writer_schema = R"({"type": "array", "maxItems": 11})",
    .reader_is_compatible_with_writer = false,
  },
  // array checks: uniqueItems must be compatible
  {
    .reader_schema = R"({"type": "array", "uniqueItems": true})",
    .writer_schema = R"({"type": "array"})",
    .reader_is_compatible_with_writer = false,
  },
  // array checks: "items" = schema must be superset
  {
    .reader_schema = R"({"type": "array", "items": {"type": "boolean"}})",
    .writer_schema = R"({"type": "array", "items": {"type": "integer"}})",
    .reader_is_compatible_with_writer = false,
  },
  // array checks: "items": array schema should be compatible
  {
    .reader_schema = R"({"type": "array", "items": [{"type":"boolean"}]})",
    .writer_schema = R"({"type": "array", "items": [{"type":"integer"}]})",
    .reader_is_compatible_with_writer = false,
  },
  // array checks: "items": array schema additionalItems should be compatible
  {
    .reader_schema
    = R"({"type": "array", "additionalItems": {"type": "boolean"}, "items": [{"type":"boolean"}]})",
    .writer_schema
    = R"({"type": "array", "additionalItems": {"type": "integer"}, "items": [{"type":"boolean"}]})",
    .reader_is_compatible_with_writer = false,
  },
  // array checks: "items": array schema extra elements should be compatible
  {
    .reader_schema
    = R"({"type": "array", "additionalItems": {"type": "integer"}, "items": [{"type":"boolean"}]})",
    .writer_schema
    = R"({"type": "array", "additionalItems": {"type": "integer"}, "items": [{"type":"boolean"}, {"type": "boolean"}]})",
    .reader_is_compatible_with_writer = false,
  },
  // combinators: "not" is required on both schema
  {
    .reader_schema
    = R"({"not": {"type": ["string", "integer", "number", "object", "array", "null"]}})",
    .writer_schema = R"({"type": "boolean"})",
    .reader_is_compatible_with_writer = false,
  },
  // combinators: "not" subschema has to get less strict
  {
    .reader_schema
    = R"({"type": ["integer", "number"], "not": {"type": "number"}})",
    .writer_schema
    = R"({"type": ["integer", "number"], "not": {"type": "integer"}})",
    .reader_is_compatible_with_writer = false,
  },
  // positive combinators: multiple combs
  {
    .reader_schema = R"({"type": "integer", "oneOf": [true], "anyOf": [true]})",
    .writer_schema = R"({"type": "integer"})",
    .reader_is_compatible_with_writer = false,
    .expected_exception = true,
  },
  // positive combinators: mismatch of type
  {
    .reader_schema = R"({"type": "integer", "anyOf": [true]})",
    .writer_schema = R"({"type": "integer"})",
    .reader_is_compatible_with_writer = false,
  },
  // positive combinators: mismatch of size
  {
    .reader_schema = R"({"allOf": [true, false]})",
    .writer_schema = R"({"allOf": [true]})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"anyOf": [true]})",
    .writer_schema = R"({"anyOf": [true, false]})",
    .reader_is_compatible_with_writer = false,
  },
  {
    .reader_schema = R"({"oneOf": [true]})",
    .writer_schema = R"({"oneOf": [true, false]})",
    .reader_is_compatible_with_writer = false,
  },
  // positive combinators: subschema mismatch
  {
    // note: this fails because for writer, there isn't s distinct matching
    // schema in reader
    .reader_schema = R"({"oneOf": [{"type":"number"}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"oneOf": [{"type":"number", "multipleOf": 10}, {"type": "number", "multipleOf": 1.1}]})",
    .reader_is_compatible_with_writer = false,
  },
  // different dialects
  {
    .reader_schema = R"({"$schema": "http://json-schema.org/draft-04/schema"})",
    .writer_schema
    = R"({"$schema": "http://json-schema.org/draft-06/schema#"})",
    .reader_is_compatible_with_writer = false,
    .expected_exception = true,
  },
  {
    .reader_schema = R"({"$schema": "http://json-schema.org/draft-04/schema"})",
    .writer_schema = "true",
    .reader_is_compatible_with_writer = false,
    .expected_exception = true,
  },
  //***** compatible section *****
  // atoms
  {
    .reader_schema = "true",
    .writer_schema = "false",
    .reader_is_compatible_with_writer = true,
  },
  // same type
  {
    .reader_schema = R"({"type": "boolean"})",
    .writer_schema = R"({"type": "boolean"})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "null"})",
    .writer_schema = R"({"type": "null"})",
    .reader_is_compatible_with_writer = true,
  },
  // restrict types
  {
    .reader_schema = R"({"type": ["null", "boolean"]})",
    .writer_schema = R"({"type": "null"})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": ["number", "boolean"], "minimum": 10.2})",
    .writer_schema = R"({"type": ["integer", "boolean"], "minimum": 11})",
    .reader_is_compatible_with_writer = true,
  },
  // numeric checks
  {
    .reader_schema = R"({"type": "number"})",
    .writer_schema
    = R"({"type": "number", "minimum": 11, "exclusiveMinimum": true})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number"})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema
    = R"({"type": "number", "maximum": 10, "exclusiveMaximum": false})",
    .writer_schema = R"({"type": "number", "maximum": 10})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMinimum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMinimum": 11})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "exclusiveMaximum": 10})",
    .writer_schema = R"({"type": "number", "exclusiveMaximum": 8})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number"})",
    .writer_schema = R"({"type": "number", "maximum": 11})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "minimum": 1.1, "maximum": 4})",
    .writer_schema = R"({"type": "number", "minimum": 1.1, "maximum": 3.2})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10})",
    .writer_schema = R"({"type": "number", "multipleOf": 20})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 10.1})",
    .writer_schema = R"({"type": "number", "multipleOf": 20.2})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "number", "multipleOf": 1.1})",
    .writer_schema = R"({"type": "number", "multipleOf": 3.3})",
    .reader_is_compatible_with_writer = true,
  },
  // string checks
  {
    .reader_schema = R"({"type": "string", "minLength": 0})",
    .writer_schema = R"({"type": "string"})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "string"})",
    .writer_schema = R"({"type": "string", "minLength": 1, "maxLength": 10})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "string", "minLength": 1})",
    .writer_schema = R"({"type": "string", "minLength": 2})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"type": "string", "pattern": "^test"})",
    .writer_schema = R"({"type": "string", "pattern": "^test"})",
    .reader_is_compatible_with_writer = true,
  },
  // metadata is ignored
  {
    .reader_schema
    = R"({"title": "myschema", "description": "this is my schema", "default": true, "type": "boolean"})",
    .writer_schema
    = R"({"title": "MySchema", "description": "this schema is mine", "default": false, "type": "boolean"})",
    .reader_is_compatible_with_writer = true,
  },
  // enum checks
  {
    .reader_schema = R"({"type": "integer", "enum": [1, 2, 4]})",
    .writer_schema = R"({"type": "integer", "enum": [4, 1]})",
    .reader_is_compatible_with_writer = true,
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
    .reader_is_compatible_with_writer = true,
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
    .reader_is_compatible_with_writer = true,
  },
  // array checks: same
  {
    .reader_schema = R"({"type": "array"})",
    .writer_schema = R"({"type": "array"})",
    .reader_is_compatible_with_writer = true,
  },
  // array checks: uniqueItems explicit value to false
  {
    .reader_schema = R"({"type": "array", "uniqueItems": false})",
    .writer_schema = R"({"type": "array"})",
    .reader_is_compatible_with_writer = true,
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
    .reader_is_compatible_with_writer = true,
  },
  // combinators: "not"
  {
    .reader_schema
    = R"({"type": "integer", "not": {"type": "integer", "minimum": 10}})",
    .writer_schema
    = R"({"type": "integer", "not": {"type": "integer", "minimum": 5}})",
    .reader_is_compatible_with_writer = true,
  },
  // positive combinators
  {
    .reader_schema
    = R"({"oneOf": [{"type":"number", "multipleOf": 3}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"oneOf": [{"type":"boolean"}, {"type": "number", "multipleOf": 9}]})",
    .reader_is_compatible_with_writer = true,
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
    .reader_is_compatible_with_writer = true,
  },
  // positive combinators single schema cases: the actual combinator does not
  // matter
  {
    .reader_schema = R"({"anyOf": [{"type": "number"}]})",
    .writer_schema = R"({"oneOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"allOf": [{"type": "number"}]})",
    .writer_schema = R"({"oneOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}]})",
    .writer_schema = R"({"anyOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"allOf": [{"type": "number"}]})",
    .writer_schema = R"({"anyOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"anyOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
  },
  // positive combinators older is single schema, newer is allOf
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}, {"type": "string"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"anyOf": [{"type": "number"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}, {"type": "string"}]})",
    .reader_is_compatible_with_writer = true,
  },
  // positive combinators older is oneOf, newer is single schema
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema = R"({"allOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    .reader_schema = R"({"oneOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema = R"({"anyOf": [{"type": "integer"}]})",
    .reader_is_compatible_with_writer = true,
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
    .reader_is_compatible_with_writer = true,
  },
  {
    // smoke test smaller reader is not compatible
    .reader_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .reader_is_compatible_with_writer = false,
  },
  {
    // smoke test bigger reader is  compatible
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    // oneOf in writer can only be smaller or equal
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"oneOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    // oneOf in writer can only be smaller or equal
    .reader_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema
    = R"({"oneOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .reader_is_compatible_with_writer = false,
  },
  {
    // oneOf in writer can only be smaller or equal
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema = R"({"oneOf": [{"type": "number"}, {"type": "string"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    // allOf in writer can be compatible with any cardinality
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema
    = R"({"allOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    // allOf in writer can be compatible with any cardinality
    .reader_schema = R"({"anyOf": [{"type": "number"}, {"type": "string"}]})",
    .writer_schema
    = R"({"allOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .reader_is_compatible_with_writer = true,
  },
  {
    // allOf in writer can be compatible with any cardinality
    .reader_schema
    = R"({"anyOf": [{"type": "number"}, {"type": "string"}, {"type": "boolean"}]})",
    .writer_schema = R"({"allOf": [{"type": "number"}, {"type": "string"}]})",
    .reader_is_compatible_with_writer = true,
  },
  // dialects
  {
    .reader_schema = R"({"$schema": "http://json-schema.org/draft-06/schema"})",
    .writer_schema
    = R"({"$schema": "http://json-schema.org/draft-06/schema#"})",
    .reader_is_compatible_with_writer = true,
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
          data.reader_is_compatible_with_writer)) {
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
                BOOST_CHECK_EQUAL(
                  data.reader_is_compatible_with_writer,
                  ::check_compatible(
                    make_json_schema(data.reader_schema),
                    make_json_schema(data.writer_schema)));
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
