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

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "serde/avro/parser.h"
#include "serde/avro/tests/avro_comparator.h"
#include "serde/avro/tests/data_generator.h"
#include "serde/avro/tests/test_utils.h"
#include "test_utils/random_bytes.h"
#include "test_utils/runfiles.h"
#include "utils/file_io.h"

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <gtest/gtest.h>

using namespace testing;
using serde::avro::testing::generic_datum_eq;
using serde::avro::testing::parsed_to_avro;

struct AvroParserTest : ::testing::TestWithParam<std::tuple<std::string_view>> {
    ::avro::ValidSchema load_json_schema(std::string_view schema_file) {
        auto desc_path = test_utils::get_runfile_path(
          fmt::format("src/v/serde/avro/tests/testdata/{}", schema_file));

        auto schema = read_fully_to_string(desc_path).get();

        auto s = ::avro::compileJsonSchemaFromString(schema);

        return s;
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
};

// Basic AVRO parsing test. The idea is very simple, the test do the following
// steps:
// - loads JSON schema from a file
// - generates random GenericDatum based on the loaded schema
// - serializes the data using avro::avro_write (generic encoding)
// - deserializes data using `serde::avro::parse`
// - converts deserialized data back to GenericDatum
// - validates if the original value is equal to the one which was deserialized
TEST_P(AvroParserTest, RoundtripTest) {
    const auto params = GetParam();
    auto valid_schema = load_json_schema(std::get<0>(params));

    for (int i = 0; i < 500; ++i) {
        // Generate random value
        avro_generator gen({});
        ::avro::GenericDatum random_value = gen.generate_datum(
          valid_schema.root());
        // serialize data with AVRO library
        iobuf buffer = serialize_with_avro(random_value, valid_schema);
        // read using serde::avro
        auto parsed = serde::avro::parse(std::move(buffer), valid_schema).get();
        ::avro::GenericDatum parsed_avro{valid_schema};
        // covert to GenericDatum
        parsed_to_avro(parsed_avro, parsed);

        ASSERT_TRUE(generic_datum_eq(
          random_value,
          parsed_avro,
          "root",
          {
            .map_matching = serde::avro::testing::compare_options::
              map_matching_policy::positional,
          }));
    }
};

INSTANTIATE_TEST_SUITE_P(
  RoundtripTest,
  AvroParserTest,
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
  [](const ::testing::TestParamInfo<AvroParserTest::ParamType>& info) {
      auto name = std::string(std::get<0>(info.param));
      std::replace(name.begin(), name.end(), '.', '_');
      return name;
  });

TEST_F(AvroParserTest, TestTooManyBytes) {
    auto valid_schema = load_json_schema("record2");
    avro_generator gen({});
    ::avro::GenericDatum random_value = gen.generate_datum(valid_schema.root());

    iobuf buffer = serialize_with_avro(random_value, valid_schema);
    buffer.append(tests::random_iobuf(128));

    ASSERT_THROW(
      serde::avro::parse(std::move(buffer), valid_schema).get(),
      std::invalid_argument);
}

TEST_F(AvroParserTest, TestRandomBytes) {
    auto valid_schema = load_json_schema("record2");
    // check if parser is safe to parse completely random bytes
    ASSERT_THROW(
      serde::avro::parse(tests::random_iobuf(512), valid_schema).get(),
      std::invalid_argument);
}

// TODO: enable this test after we solve bazel test issue
//
//  TEST_F(AvroParserTest, TestMaxNestedLevel) {
//      // schema of type with recursive dependency
//      auto valid_schema = load_json_schema("tree2");
//      generator_state state;
//      ::avro::GenericDatum random_value = generate_datum(
//        valid_schema.root(), state, 120, 1);
//      iobuf buffer = serialize_with_avro(random_value, valid_schema);

//     ASSERT_THROW(
//       serde::avro::parse(std::move(buffer), valid_schema).get(),
//       std::invalid_argument);
// }

// returns true if deserialization was successful

bool try_deserialize_with_avro_lib(
  const ::avro::ValidSchema& schema, const iobuf& buffer) {
    auto linearized_buffer = iobuf_to_bytes(buffer);
    auto in = ::avro::memoryInputStream(
      linearized_buffer.data(), linearized_buffer.size());
    auto decoder = ::avro::binaryDecoder();
    decoder->init(*in);
    ::avro::GenericReader avro_lib_reader(schema, decoder);
    ::avro::GenericDatum datum;
    try {
        avro_lib_reader.read(datum);
        return true;
    } catch (...) {
        return false;
    }
}

TEST_F(AvroParserTest, TestIncorrectSchema) {
    auto valid_schema = load_json_schema("record2");
    avro_generator gen({});
    ::avro::GenericDatum random_value = gen.generate_datum(valid_schema.root());
    iobuf buffer = serialize_with_avro(random_value, valid_schema);
    auto invalid_schema = load_json_schema("tree2");
    auto success = try_deserialize_with_avro_lib(invalid_schema, buffer);
    // parse with incorrect schema
    if (success) {
        ASSERT_NO_THROW(
          serde::avro::parse(std::move(buffer), invalid_schema).get());
    } else {
        ASSERT_THROW(
          serde::avro::parse(std::move(buffer), invalid_schema).get(),
          std::invalid_argument);
    }
}
