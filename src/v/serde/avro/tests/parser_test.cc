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
#include "bytes/random.h"
#include "random/generators.h"
#include "serde/avro/parser.h"
#include "serde/avro/tests/data_generator.h"
#include "test_utils/runfiles.h"
#include "utils/file_io.h"

#include <seastar/util/defer.hh>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
using namespace testing;

void parsed_to_avro(
  ::avro::GenericDatum& datum,
  const std::unique_ptr<serde::avro::parsed::message>& msg);

struct primitive_visitor {
    void operator()(int32_t v) { datum->value<int32_t>() = v; }
    void operator()(int64_t v) {
        if (datum->type() == ::avro::Type::AVRO_ENUM) {
            datum->value<avro::GenericEnum>().set(v);
        } else {
            datum->value<int64_t>() = v;
        }
    }
    void operator()(bool v) { datum->value<bool>() = v; }
    void operator()(serde::avro::parsed::avro_null) {}
    void operator()(double v) { datum->value<double>() = v; }
    void operator()(float v) { datum->value<float>() = v; }
    void operator()(const iobuf& buffer) {
        if (datum->type() == ::avro::Type::AVRO_FIXED) {
            auto& avro_fixed = datum->value<::avro::GenericFixed>();

            avro_fixed.value().reserve(buffer.size_bytes());
            iobuf::iterator_consumer it(buffer.cbegin(), buffer.cend());
            it.consume_to(buffer.size_bytes(), avro_fixed.value().data());

        } else if (datum->type() == ::avro::Type::AVRO_STRING) {
            auto& avro_str = datum->value<std::string>();

            iobuf_parser p(buffer.copy());
            avro_str = p.read_string(buffer.size_bytes());
        } else {
            std::vector<uint8_t> avro_bytes(buffer.size_bytes());
            iobuf::iterator_consumer it(buffer.cbegin(), buffer.cend());
            it.consume_to(buffer.size_bytes(), avro_bytes.data());
            datum->value<std::vector<uint8_t>>() = avro_bytes;
        }
    }
    ::avro::GenericDatum* datum;
};

struct parsed_msg_visitor {
    void operator()(const serde::avro::parsed::record& record) {
        auto& avro_record = datum->value<avro::GenericRecord>();
        for (size_t i = 0; i < avro_record.fieldCount(); ++i) {
            auto& field_datum = avro_record.fieldAt(i);
            parsed_to_avro(field_datum, record.fields[i]);
        }
    }
    void operator()(const serde::avro::parsed::map& parsed_map) {
        auto& avro_map = datum->value<::avro::GenericMap>();
        for (auto& [k, v] : parsed_map.entries) {
            iobuf_const_parser p(k);
            ::avro::GenericDatum value(avro_map.schema()->leafAt(1));
            parsed_to_avro(value, v);
            auto key_str = p.read_string(k.size_bytes());
            avro_map.value().emplace_back(key_str, value);
        }
    }
    void operator()(const serde::avro::parsed::list& v) {
        auto& array = datum->value<::avro::GenericArray>();
        for (auto& e : v.elements) {
            ::avro::GenericDatum value(array.schema()->leafAt(0));
            parsed_to_avro(value, e);
            array.value().push_back(value);
        }
    }
    void operator()(const serde::avro::parsed::avro_union& v) {
        datum->selectBranch(v.branch);
        parsed_to_avro(*datum, v.message);
    }
    void operator()(const serde::avro::parsed::primitive& v) {
        std::visit(primitive_visitor{datum}, v);
    }
    ::avro::GenericDatum* datum;
};

void parsed_to_avro(
  ::avro::GenericDatum& datum,
  const std::unique_ptr<serde::avro::parsed::message>& msg) {
    std::visit(parsed_msg_visitor{&datum}, *msg);
}

struct AvroParserTest : ::testing::TestWithParam<std::tuple<std::string_view>> {
    ::avro::ValidSchema load_json_schema(std::string_view schema_file) {
        auto desc_path = test_utils::get_runfile_path(
                           fmt::format(
                             "src/v/serde/avro/tests/testdata/{}", schema_file))
                           .value_or(std::string(schema_file));

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

    template<typename T>
    AssertionResult check_primitive(
      const ::avro::GenericDatum& expected,
      const ::avro::GenericDatum& actual) {
        if (expected.value<T>() != actual.value<T>()) {
            return AssertionFailure() << fmt::format(
                     "expected: {} to be equal to {}",
                     actual.value<T>(),
                     expected.value<T>());
        }
        return AssertionSuccess();
    }

    AssertionResult are_equal(
      const ::avro::GenericDatum& expected,
      const ::avro::GenericDatum& actual) {
        if (expected.type() != actual.type()) {
            return AssertionFailure() << fmt::format(
                     "Type mismatch, expected: {}, actual: {}",
                     expected.type(),
                     actual.type());
        }
        switch (expected.type()) {
        case avro::AVRO_STRING:
            return check_primitive<std::string>(expected, actual);
        case avro::AVRO_BYTES:
            return check_primitive<std::vector<uint8_t>>(expected, actual);
        case avro::AVRO_INT:
            return check_primitive<int32_t>(expected, actual);
        case avro::AVRO_LONG:
            return check_primitive<int64_t>(expected, actual);
        case avro::AVRO_FLOAT:
            return check_primitive<float>(expected, actual);
        case avro::AVRO_DOUBLE:
            return check_primitive<double>(expected, actual);
        case avro::AVRO_BOOL:
            return check_primitive<bool>(expected, actual);
        case avro::AVRO_NULL:
            return AssertionSuccess();
        case avro::AVRO_RECORD: {
            auto expected_r = expected.value<::avro::GenericRecord>();
            auto actual_r = actual.value<::avro::GenericRecord>();
            if (expected_r.fieldCount() != actual_r.fieldCount()) {
                return AssertionFailure() << fmt::format(
                         "Avro record expected to have {} fields while the "
                         "actual has: {}",
                         expected_r.fieldCount(),
                         actual_r.fieldCount());
            }
            for (size_t i : boost::irange(expected_r.fieldCount())) {
                auto r = are_equal(expected_r.fieldAt(i), actual_r.fieldAt(i));

                if (!r) {
                    return AssertionFailure() << "Record value mismatch at "
                                              << i << " - " << r.message();
                }
            }
            return AssertionSuccess();
        }
        case avro::AVRO_ENUM: {
            auto& expected_enum = expected.value<::avro::GenericEnum>();
            auto& actual_enum = expected.value<::avro::GenericEnum>();
            if (expected_enum.value() != actual_enum.value()) {
                return AssertionFailure() << fmt::format(
                         "enum value mismatch {} != {}",
                         expected_enum.value(),
                         actual_enum.value());
            }
            return AssertionSuccess();
        }
        case avro::AVRO_ARRAY: {
            auto expected_array = expected.value<::avro::GenericArray>();
            auto actual_array = actual.value<::avro::GenericArray>();
            if (expected_array.value().size() != actual_array.value().size()) {
                return AssertionFailure() << fmt::format(
                         "array size mismatch {} != {}",
                         expected_array.value().size(),
                         actual_array.value().size());
            }
            for (size_t i : boost::irange(expected_array.value().size())) {
                auto result = are_equal(
                  expected_array.value()[i], actual_array.value()[i]);
                if (!result) {
                    return AssertionFailure() << fmt::format(
                             "array element mismatch - {}", result.message());
                }
            }
            return AssertionSuccess();
        }
        case avro::AVRO_MAP: {
            auto expected_map = expected.value<::avro::GenericMap>();
            auto actual_map = actual.value<::avro::GenericMap>();
            if (expected_map.value().size() != actual_map.value().size()) {
                return AssertionFailure() << fmt::format(
                         "map size mismatch {} != {}",
                         expected_map.value().size(),
                         actual_map.value().size());
            }
            for (size_t i : boost::irange(expected_map.value().size())) {
                if (
                  expected_map.value()[i].first
                  != actual_map.value()[i].first) {
                    return AssertionFailure() << fmt::format(
                             "map key mismatch {} != {}",
                             expected_map.value()[i].first,
                             actual_map.value()[i].first);
                }
                auto result = are_equal(
                  expected_map.value()[i].second, actual_map.value()[i].second);
                if (!result) {
                    return AssertionFailure() << fmt::format(
                             "map value mismatch - {}", result.message());
                }
            }
            return AssertionSuccess();
        }

        case avro::AVRO_UNION:
            if (expected.unionBranch() != actual.unionBranch()) {
                return AssertionFailure() << fmt::format(
                         "union branch mismatch {} != {}",
                         expected.unionBranch(),
                         actual.unionBranch());
            }
            return are_equal(
              expected.value<::avro::GenericUnion>().datum(),
              actual.value<::avro::GenericUnion>().datum());
        case avro::AVRO_FIXED: {
            auto expected_fixed = expected.value<::avro::GenericFixed>();
            auto actual_fixed = expected.value<::avro::GenericFixed>();
            if (expected_fixed.value() != actual_fixed.value()) {
                return AssertionFailure()
                       << fmt::format("fixed value mismatch");
            }
            return AssertionSuccess();
        }
        case avro::AVRO_UNKNOWN:
        case avro::AVRO_NUM_TYPES:
            break;
        }
        return AssertionFailure();
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
        generator_state state;
        ::avro::GenericDatum random_value = generate_datum(
          valid_schema.root(), state, 10);
        // serialize data with AVRO library
        iobuf buffer = serialize_with_avro(random_value, valid_schema);
        // read using serde::avro
        auto parsed = serde::avro::parse(std::move(buffer), valid_schema).get();
        ::avro::GenericDatum parsed_avro{valid_schema};
        // covert to GenericDatum
        parsed_to_avro(parsed_avro, parsed);

        ASSERT_TRUE(are_equal(random_value, parsed_avro));
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
    "unionwithmap"));

TEST_F(AvroParserTest, TestTooManyBytes) {
    auto valid_schema = load_json_schema("record2");
    generator_state state;
    ::avro::GenericDatum random_value = generate_datum(
      valid_schema.root(), state, 10);

    iobuf buffer = serialize_with_avro(random_value, valid_schema);
    buffer.append(random_generators::make_iobuf(128));

    ASSERT_THROW(
      serde::avro::parse(std::move(buffer), valid_schema).get(),
      std::invalid_argument);
}

TEST_F(AvroParserTest, TestRandomBytes) {
    auto valid_schema = load_json_schema("record2");
    // check if parser is safe to parse completely random bytes
    ASSERT_THROW(
      serde::avro::parse(random_generators::make_iobuf(512), valid_schema)
        .get(),
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
    generator_state state;
    ::avro::GenericDatum random_value = generate_datum(
      valid_schema.root(), state, 10);
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
