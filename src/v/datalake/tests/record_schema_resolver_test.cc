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
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "iceberg/datatypes.h"
#include "pandaproxy/schema_registry/types.h"
#include "schema/tests/fake_registry.h"
#include "utils/vint.h"

#include <gtest/gtest.h>

#include <exception>
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
constexpr std::string_view pb_record_schema = R"(
syntax = "proto2";
package datalake.proto;
message empty_message {}
message simple_message {
  optional string label = 1;
  optional int32 number = 3;
  optional int64 big_number = 4;
  optional float float_number = 5;
  optional double double_number = 6;
  optional bool true_or_false = 7;
}
message nested_message {
  message inner_message_t1 {
    optional string inner_label_1 = 1;
    optional int32 inner_number_1 = 2;
  }
  message inner_message_t2 {
    optional string inner_label_2 = 1;
    optional int32 inner_number_2 = 2;
  }
  optional string label = 1;
  optional int32 number = 2;
  optional inner_message_t1 inner = 3;
}
)";
iobuf generate_dummy_body() { return iobuf::from("blob"); }
iobuf encode_pb_offsets(const std::vector<int32_t>& offsets) {
    auto cnt_bytes = vint::to_bytes(offsets.size());
    iobuf buf;
    buf.append(cnt_bytes.data(), cnt_bytes.size());
    for (auto o : offsets) {
        auto bytes = vint::to_bytes(o);
        buf.append(bytes.data(), bytes.size());
    }
    return buf;
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
        auto pb_schema_id = sr
                              ->create_schema(unparsed_schema{
                                subject{"foo"},
                                unparsed_schema_definition{
                                  pb_record_schema, schema_type::protobuf}})
                              .get();
        ASSERT_EQ(2, pb_schema_id());
    }
    std::unique_ptr<schema::fake_registry> sr;
};

TEST_F(RecordSchemaResolverTest, TestAvroSchemaHappyPath) {
    // Kakfa magic byte + schema ID.
    iobuf buf;
    buf.append("\0\0\0\0\1", 5);
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(1, resolved_buf.type->id.schema_id());
    EXPECT_FALSE(resolved_buf.type->id.protobuf_offsets.has_value());

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
    EXPECT_EQ(resolved_buf.type->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaHappyPath) {
    // Kakfa magic byte + schema ID + pb offsets.
    std::vector<int32_t> pb_offsets{2, 0};
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets(pb_offsets));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, resolved_buf.type->id.schema_id());
    EXPECT_TRUE(resolved_buf.type->id.protobuf_offsets.has_value());
    EXPECT_EQ(resolved_buf.type->id.protobuf_offsets.value(), pb_offsets);

    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(nested_field::create(
          1, "inner_label_1", field_required::no, string_type{}));
        expected_struct.fields.emplace_back(nested_field::create(
          2, "inner_number_1", field_required::no, int_type{}));
        return expected_struct;
    }()};
    EXPECT_EQ(resolved_buf.type->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaHappyPathNested) {
    // Kakfa magic byte + schema ID + pb offsets.
    // Point at a nested field.
    std::vector<int32_t> pb_offsets{2};
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets(pb_offsets));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, resolved_buf.type->id.schema_id());
    EXPECT_TRUE(resolved_buf.type->id.protobuf_offsets.has_value());
    EXPECT_EQ(resolved_buf.type->id.protobuf_offsets.value(), pb_offsets);

    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        expected_struct.fields.emplace_back(
          nested_field::create(1, "label", field_required::no, string_type{}));
        expected_struct.fields.emplace_back(
          nested_field::create(2, "number", field_required::no, int_type{}));

        struct_type inner_struct;
        inner_struct.fields.emplace_back(nested_field::create(
          1, "inner_label_1", field_required::no, string_type{}));
        inner_struct.fields.emplace_back(nested_field::create(
          2, "inner_number_1", field_required::no, int_type{}));

        expected_struct.fields.emplace_back(nested_field::create(
          3, "inner", field_required::no, std::move(inner_struct)));
        return expected_struct;
    }()};
    EXPECT_EQ(resolved_buf.type->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaHappyPathNoOffsets) {
    // Kakfa magic byte + schema ID + _empty_ pb offsets.
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets({}));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(2, resolved_buf.type->id.schema_id());
    EXPECT_TRUE(resolved_buf.type->id.protobuf_offsets.has_value());
    EXPECT_EQ(
      resolved_buf.type->id.protobuf_offsets.value(), std::vector<int32_t>{0});

    // When there are no protobuf offsets, we return the first descriptor,
    // which in this case translates to an empty struct.
    const auto expected_type = field_type{[] {
        auto expected_struct = struct_type{};
        return expected_struct;
    }()};
    EXPECT_EQ(resolved_buf.type->type, expected_type);
}

TEST_F(RecordSchemaResolverTest, TestProtobufSchemaBadOffsets) {
    // Kakfa magic byte + schema ID + bogus pb offsets.
    iobuf buf;
    buf.append("\0\0\0\0\2", 5);
    buf.append(encode_pb_offsets({100}));
    buf.append(generate_dummy_body());

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), type_resolver::errc::bad_input);
}

TEST_F(RecordSchemaResolverTest, TestMissingMagic) {
    iobuf buf;
    // Write body but no magic.
    buf.append(generate_dummy_body());
    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), type_resolver::errc::bad_input);
}

TEST_F(RecordSchemaResolverTest, TestSchemaRegistryError) {
    iobuf buf;
    buf.append("\0\0\0\0\1", 5);
    buf.append(generate_dummy_body());
    sr->set_inject_failures(
      std::make_exception_ptr(std::runtime_error("injected")));

    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), type_resolver::errc::registry_error);

    // We can try again when there are no injected errors and there should be
    // no issue.
    sr->set_inject_failures(nullptr);
    res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    auto& resolved_buf = res.value();
    ASSERT_TRUE(resolved_buf.type.has_value());
    EXPECT_EQ(1, resolved_buf.type->id.schema_id());
    EXPECT_FALSE(resolved_buf.type->id.protobuf_offsets.has_value());
}
