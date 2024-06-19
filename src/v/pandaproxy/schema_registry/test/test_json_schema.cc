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
      "Invalid json schema: '#/type', invalid metaschema: '#/properties/type', "
      "invalid keyword: 'anyOf'"}},
  error_test_case{
    R"({"$schema": "unsupported_dialect"})",
    pps::error_info{
      pps::error_code::schema_invalid,
      "Invalid json schema: '#/%24schema', invalid metaschema: "
      "'#/properties/%24schema', invalid keyword: 'enum'"}},
});
SEASTAR_THREAD_TEST_CASE(test_make_invalid_json_schema) {
    for (const auto& data : error_test_cases) {
        store_fixture f;
        BOOST_TEST_CONTEXT(data) {
            BOOST_REQUIRE_EXCEPTION(
              pps::make_canonical_json_schema(
                f.store,
                {pps::subject{"test"}, {data.def, pps::schema_type::json}})
                .get(),
              pps::exception,
              [&data](pps::exception const& e) {
                  auto code_matches = e.code() == data.err.code();
                  if (code_matches && e.message() != data.err.message()) {
                      fmt::print(
                        "[{}] does not match expected [{}]\n",
                        e.message(),
                        data.err.message());
                  }
                  return code_matches;
              });
        }
    };
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
  R"({"$schema": "http://json-schema.org/draft-04/schema#"})",
});
SEASTAR_THREAD_TEST_CASE(test_make_valid_json_schema) {
    for (const auto& data : valid_test_cases) {
        store_fixture f;
        BOOST_TEST_CONTEXT(data) {
            auto s = pps::make_json_schema_definition(
              f.store,
              pps::make_canonical_json_schema(
                f.store, {pps::subject{"test"}, {data, pps::schema_type::json}})
                .get());
            BOOST_REQUIRE_NO_THROW(s.get());
        }
    };
}

struct compatibility_test_case {
    std::string_view reader_schema;
    std::string_view writer_schema;
    bool reader_is_compatible_with_writer;
};

static constexpr auto compatibility_test_cases
  = std::to_array<compatibility_test_case>({
    //***** not compatible section *****
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
      .reader_schema
      = R"({"type": "number", "minumum": 1.1, "maximum": 3.199})",
      .writer_schema = R"({"type": "integer", "minimum": 1.1, "maximum": 3.2})",
      .reader_is_compatible_with_writer = false,
    },
    {
      .reader_schema = R"({"type": "number", "multipleOf": 10})",
      .writer_schema = R"({"type": "number", "multipleOf": 21})",
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
    //***** compatible section *****
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
      .writer_schema = R"({"type": "number", "maximum": 11})",
      .reader_is_compatible_with_writer = true,
    },
    {
      .reader_schema = R"({"type": "number", "minumum": 1.1, "maximum": 4})",
      .writer_schema = R"({"type": "number", "minimum": 1.1, "maximum": 3.2})",
      .reader_is_compatible_with_writer = true,
    },
    {
      .reader_schema = R"({"type": "number", "multipleOf": 10})",
      .writer_schema = R"({"type": "number", "multipleOf": 20})",
      .reader_is_compatible_with_writer = true,
    },
    // string checks
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
      = R"({"title": "myschema", "description": "this is my schema",
            "default": true, "type": "boolean"})",
      .writer_schema
      = R"({"title": "MySchema", "description": "this schema is mine",
            "default": false, "type": "boolean"})",
      .reader_is_compatible_with_writer = true,
    },
    // enum checks
    {
      .reader_schema = R"({"type": "integer", "enum": [1, 2, 4]})",
      .writer_schema = R"({"type": "integer", "enum": [4, 1]})",
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
                BOOST_CHECK_EQUAL(
                  data.reader_is_compatible_with_writer,
                  pps::check_compatible(
                    make_json_schema(data.reader_schema),
                    make_json_schema(data.writer_schema)));
            } catch (...) {
                BOOST_CHECK_MESSAGE(
                  false,
                  fmt::format(
                    "terminated with exception {}", std::current_exception()));
            }
        }
    };
}
