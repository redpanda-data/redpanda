/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "bytes/bytes.h"
#include "bytes/streambuf.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "iceberg/avro_utils.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/tests/fake_registry.h"

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <gtest/gtest.h>

#include <variant>

using namespace pandaproxy::schema_registry;
using namespace datalake;
using namespace iceberg;

namespace {
constexpr std::string_view avro_record_schema = R"({
  "type": "record",
  "name": "LongList",
  "fields" : [
    {"name": "value", "type": "long"},
    {"type": "int", "name": "next", "metadata" : "two"}
  ]
})";
iobuf generate_example_avro_record() {
    // Generate a simple avro record that looks like this (as json):
    // {"value":500000000000,"next":12}
    iobuf_istream bis{iobuf::from(avro_record_schema)};
    auto is = avro::istreamInputStream(bis.istream());
    auto schema = avro::compileJsonSchemaFromStream(*is);
    avro::GenericDatum d(schema.root());
    auto& r = d.value<avro::GenericRecord>();
    r.setFieldAt(r.fieldIndex("value"), long(9223372036854775807));
    r.setFieldAt(r.fieldIndex("next"), int(1234));

    size_t bytes_streamed{0};
    avro_iobuf_ostream::buf_container_t bufs;
    auto out = std::make_unique<avro_iobuf_ostream>(
      4096, &bufs, &bytes_streamed);

    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    avro::encode(*e, d);
    e->flush();
    iobuf b;
    for (auto& buf : bufs) {
        b.append(std::move(buf));
    }
    b.trim_back(b.size_bytes() - bytes_streamed);
    return b;
}

// Checks that the given resolved buffer has no schema, and just returns a
// single binary column that matches the input iobuf.
void check_no_schema(const resolved_buf& resolved, const iobuf& expected) {
    EXPECT_FALSE(resolved.schema_identifier.has_value());

    // The type should have a single injected binary field.
    EXPECT_FALSE(resolved.type.has_value()) << *resolved.type;

    // The value should match the expected buf.
    ASSERT_TRUE(resolved.val.has_value());
    ASSERT_TRUE(
      std::holds_alternative<std::unique_ptr<struct_value>>(*resolved.val));
    auto& sval = *std::get<std::unique_ptr<struct_value>>(*resolved.val);
    ASSERT_EQ(1, sval.fields.size()) << sval;
    ASSERT_TRUE(sval.fields[0].has_value());
    EXPECT_EQ(
      sval.fields[0].value(),
      value{primitive_value{binary_value{expected.copy()}}});
}
} // namespace

class RecordSchemaResolverTest : public ::testing::Test {
public:
    RecordSchemaResolverTest()
      : sr(std::make_unique<schema::fake_registry>()) {}

    void SetUp() override {
        auto avro_schema_id = sr
                                ->create_schema(unparsed_schema{
                                  subject{"foo"},
                                  unparsed_schema_definition{
                                    avro_record_schema, schema_type::avro}})
                                .get();
        ASSERT_EQ(1, avro_schema_id());
    }
    std::unique_ptr<schema::fake_registry> sr;
};

TEST_F(RecordSchemaResolverTest, TestAvroSchemaHappyPath) {
    // Kakfa magic byte + schema ID.
    iobuf buf;
    buf.append("\0\0\0\0\1", 5);
    buf.append(generate_example_avro_record());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_schema(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.schema_identifier.has_value());
    EXPECT_EQ(1, resolved_buf.schema_identifier.value().schema_id());
    EXPECT_FALSE(
      resolved_buf.schema_identifier.value().protobuf_offsets.has_value());

    // Check that the resolved schema looks correct. Note, the field IDs are
    // unimportant since they are assigned outside of the resolver -- it's just
    // important the data's structure looks good.
    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(0, "value", field_required::yes, long_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(0, "next", field_required::yes, int_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ(resolved_buf.type, expected_type);

    // Check that the resolved data looks like what we expect.
    EXPECT_TRUE(resolved_buf.val.has_value());
    const auto& data_val = std::get<std::unique_ptr<struct_value>>(
      resolved_buf.val.value());
    ASSERT_EQ(2, data_val->fields.size());
    ASSERT_TRUE(data_val->fields[0].has_value());
    EXPECT_EQ(data_val->fields[0].value(), long_value{9223372036854775807});
    ASSERT_TRUE(data_val->fields[1].has_value());
    EXPECT_EQ(data_val->fields[1].value(), int_value{1234});
}

TEST_F(RecordSchemaResolverTest, TestMissingMagic) {
    iobuf buf;
    // Write avro but no magic.
    buf.append(generate_example_avro_record());
    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_schema(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    const auto& resolved_buf = res.value();
    ASSERT_NO_FATAL_FAILURE(check_no_schema(resolved_buf, buf));
}

TEST_F(RecordSchemaResolverTest, TestBadBuffer) {
    iobuf buf;
    // Write magic but no Avro.
    buf.append("\0\0\0\0\1", 5);
    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_schema(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    const auto& resolved_buf = res.value();
    ASSERT_NO_FATAL_FAILURE(check_no_schema(resolved_buf, buf));
}

TEST_F(RecordSchemaResolverTest, TestSchemaRegistryError) {
    iobuf buf;
    buf.append("\0\0\0\0\1", 5);
    buf.append(generate_example_avro_record());
    sr->set_inject_failures(true);

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_schema(buf.copy()).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), record_schema_resolver::errc::registry_error);

    // We can try again when there are no injected errors and there should be
    // no issue.
    sr->set_inject_failures(false);
    res = resolver.resolve_buf_schema(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.schema_identifier.has_value());
    EXPECT_EQ(1, resolved_buf.schema_identifier.value().schema_id());
    EXPECT_FALSE(
      resolved_buf.schema_identifier.value().protobuf_offsets.has_value());
}
