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
iobuf generate_dummy_body() { return iobuf::from("blob"); }

// Checks that the given resolved buffer has no schema, and just returns a
// single binary column that matches the input iobuf.
void check_no_schema(const type_and_buf& resolved, const iobuf& expected) {
    EXPECT_FALSE(resolved.type.has_value());

    // The value should match the expected buf.
    ASSERT_EQ(resolved.parsable_buf, expected);
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

TEST_F(RecordSchemaResolverTest, TestMissingMagic) {
    iobuf buf;
    // Write body but no magic.
    buf.append(generate_dummy_body());
    auto resolver = record_schema_resolver(*sr);
    auto res = resolver.resolve_buf_type(buf.copy()).get();
    ASSERT_FALSE(res.has_error());
    const auto& resolved_buf = res.value();
    ASSERT_NO_FATAL_FAILURE(check_no_schema(resolved_buf, buf));
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
    ASSERT_EQ(res.error(), record_schema_resolver::errc::registry_error);

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
