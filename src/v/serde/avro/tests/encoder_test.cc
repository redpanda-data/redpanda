/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "serde/avro/encoder.h"
#include "serde/avro/parser.h"
#include "serde/avro/tests/avro_comparator.h"
#include "serde/avro/tests/data_generator.h"
#include "serde/avro/tests/test_utils.h"
#include "test_utils/runfiles.h"
#include "utils/file_io.h"

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <gtest/gtest.h>

using namespace testing;
using serde::avro::testing::generic_datum_eq;
using serde::avro::testing::parsed_to_avro;

struct AvroEncoderTest : ::testing::Test {
    ::avro::ValidSchema load_json_schema(std::string_view schema_file) {
        auto desc_path = test_utils::get_runfile_path(
          fmt::format("src/v/serde/avro/tests/testdata/{}", schema_file));
        auto schema = read_fully_to_string(desc_path).get();
        return ::avro::compileJsonSchemaFromString(schema);
    }

    ::avro::ValidSchema compile_schema(std::string_view json) {
        return ::avro::compileJsonSchemaFromString(std::string(json));
    }

    void avro_write(
      ::avro::EncoderPtr& e,
      ::avro::ValidSchema schema,
      ::avro::GenericDatum message) {
        ::avro::GenericWriter writer(schema, e);
        writer.write(message);
    }

    iobuf serialize_with_avro(
      const ::avro::GenericDatum& datum, const ::avro::ValidSchema& schema) {
        std::unique_ptr<::avro::OutputStream> out
          = ::avro::memoryOutputStream();
        auto e = ::avro::binaryEncoder();
        e->init(*out);
        avro_write(e, schema, datum);
        e->flush();
        auto data = ::avro::snapshot(*out);
        iobuf buffer;
        buffer.append(data->data(), e->byteCount());
        return buffer;
    }

    /// Parse -> encode -> parse roundtrip, returning the re-parsed tree
    /// converted to GenericDatum for comparison.
    ::avro::GenericDatum roundtrip(
      const ::avro::GenericDatum& original, const ::avro::ValidSchema& schema) {
        // Serialize with Apache Avro library
        iobuf original_bytes = serialize_with_avro(original, schema);

        // Parse with our parser
        auto parsed = serde::avro::parse(original_bytes.copy(), schema).get();

        // Encode back with our encoder
        auto encoded = serde::avro::encode(*parsed, schema).get();

        // Parse the re-encoded bytes
        auto reparsed = serde::avro::parse(std::move(encoded), schema).get();

        // Convert to GenericDatum for comparison
        ::avro::GenericDatum result{schema};
        parsed_to_avro(result, reparsed);
        return result;
    }
};

TEST_F(AvroEncoderTest, encode_simple_record) {
    auto schema = compile_schema(R"({
        "type": "record",
        "name": "SimpleRecord",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "active", "type": "boolean"}
        ]
    })");

    avro_generator gen({});
    for (int i = 0; i < 100; ++i) {
        auto original = gen.generate_datum(schema.root());
        auto result = roundtrip(original, schema);
        ASSERT_TRUE(generic_datum_eq(original, result));
    }
}

TEST_F(AvroEncoderTest, encode_nested_record) {
    auto schema = load_json_schema("recinrec");

    avro_generator gen({});
    for (int i = 0; i < 100; ++i) {
        auto original = gen.generate_datum(schema.root());
        auto result = roundtrip(original, schema);
        ASSERT_TRUE(generic_datum_eq(original, result));
    }
}

TEST_F(AvroEncoderTest, encode_with_union) {
    auto schema = compile_schema(R"({
        "type": "record",
        "name": "UnionRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "opt_name", "type": ["null", "string"]}
        ]
    })");

    avro_generator gen({});
    for (int i = 0; i < 100; ++i) {
        auto original = gen.generate_datum(schema.root());
        auto result = roundtrip(original, schema);
        ASSERT_TRUE(generic_datum_eq(original, result));
    }
}

TEST_F(AvroEncoderTest, encode_with_map) {
    auto schema = compile_schema(R"({
        "type": "record",
        "name": "MapRecord",
        "fields": [
            {"name": "tags", "type": {"type": "map", "values": "string"}}
        ]
    })");

    avro_generator gen({.elements_in_collection = 5});
    for (int i = 0; i < 100; ++i) {
        auto original = gen.generate_datum(schema.root());
        auto result = roundtrip(original, schema);
        ASSERT_TRUE(generic_datum_eq(
          original,
          result,
          "root",
          {
            .map_matching = serde::avro::testing::compare_options::
              map_matching_policy::positional,
          }));
    }
}

TEST_F(AvroEncoderTest, encode_with_array) {
    auto schema = compile_schema(R"({
        "type": "record",
        "name": "ArrayRecord",
        "fields": [
            {"name": "values", "type": {"type": "array", "items": "int"}}
        ]
    })");

    avro_generator gen({.elements_in_collection = 5});
    for (int i = 0; i < 100; ++i) {
        auto original = gen.generate_datum(schema.root());
        auto result = roundtrip(original, schema);
        ASSERT_TRUE(generic_datum_eq(original, result));
    }
}

TEST_F(AvroEncoderTest, encode_with_bytes) {
    auto schema = compile_schema(R"({
        "type": "record",
        "name": "BytesRecord",
        "fields": [
            {"name": "data", "type": "bytes"},
            {"name": "label", "type": "string"}
        ]
    })");

    avro_generator gen({});
    for (int i = 0; i < 100; ++i) {
        auto original = gen.generate_datum(schema.root());
        auto result = roundtrip(original, schema);
        ASSERT_TRUE(generic_datum_eq(original, result));
    }
}

/// Comprehensive roundtrip test using all schemas from the parser test suite.
/// parse -> encode -> parse, verify trees match.
struct AvroEncoderRoundtripTest
  : AvroEncoderTest
  , ::testing::WithParamInterface<std::tuple<std::string_view>> {};

TEST_P(AvroEncoderRoundtripTest, roundtrip_preserves_data) {
    const auto params = GetParam();
    auto schema = load_json_schema(std::get<0>(params));

    avro_generator gen({});
    for (int i = 0; i < 100; ++i) {
        auto original = gen.generate_datum(schema.root());

        // Serialize with Apache Avro
        iobuf original_bytes = serialize_with_avro(original, schema);

        // Parse with our parser
        auto parsed = serde::avro::parse(original_bytes.copy(), schema).get();

        // Encode back with our encoder
        auto encoded = serde::avro::encode(*parsed, schema).get();

        // The re-encoded bytes should parse identically
        auto reparsed = serde::avro::parse(std::move(encoded), schema).get();

        // Convert both to GenericDatum for comparison
        ::avro::GenericDatum parsed_datum{schema};
        parsed_to_avro(parsed_datum, parsed);

        ::avro::GenericDatum reparsed_datum{schema};
        parsed_to_avro(reparsed_datum, reparsed);

        ASSERT_TRUE(generic_datum_eq(
          parsed_datum,
          reparsed_datum,
          "root",
          {
            .map_matching = serde::avro::testing::compare_options::
              map_matching_policy::positional,
          }));
    }
}

INSTANTIATE_TEST_SUITE_P(
  RoundtripTest,
  AvroEncoderRoundtripTest,
  ::testing::Values(
    "array",
    "bigrecord",
    "bigrecord2",
    "bigrecord_r",
    "big_union",
    "circulardep",
    "cpp_reserved_words",
    "cpp_reserved_words_union_typedef",
    "crossref",
    "empty_record",
    "enum",
    "fixed",
    "int",
    "large_schema.avsc",
    "map",
    "padded_record",
    "primitivetypes",
    "recinrec",
    "record",
    "record2",
    "reuse",
    "tree1",
    "tree2",
    "tweet",
    "union",
    "union_array_union",
    "union_conflict",
    "union_empty_record",
    "union_map_union",
    "union_redundant_types",
    "unionwithmap"),
  [](
    const ::testing::TestParamInfo<AvroEncoderRoundtripTest::ParamType>& info) {
      auto name = std::string(std::get<0>(info.param));
      std::replace(name.begin(), name.end(), '.', '_');
      return name;
  });
