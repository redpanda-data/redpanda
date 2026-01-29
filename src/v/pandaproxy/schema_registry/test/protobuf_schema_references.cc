// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/test/store_fixture.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace pandaproxy::schema_registry {

using ::testing::HasSubstr;

class ProtobufSchemaReferencesTest
  : public ::testing::Test
  , public test_utils::store_fixture {};

// subject_schema.sub().to_string() is used as the protobuf file name.
// Verify protobuf accepts file names with dots from qualified subjects.
TEST_F(ProtobufSchemaReferencesTest, DottedFileName) {
    auto sub = context_subject{context{".my.ctx"}, subject{"test"}};
    ASSERT_THAT(sub.to_string(), HasSubstr("."));

    auto schema
      = make_protobuf_schema_definition(
          _store,
          subject_schema{
            sub,
            schema_definition{
              R"(syntax = "proto3"; message Test { string value = 1; })",
              schema_type::protobuf}})
          .get();

    auto name_result = schema.name({0});
    ASSERT_TRUE(name_result.has_value());
    EXPECT_EQ(name_result.value(), "Test");
}

// Same subject name in different contexts should not collide.
TEST_F(ProtobufSchemaReferencesTest, SameSubjectDifferentContexts) {
    auto dep_ctx_a = context_subject{context{".ctx_a"}, subject{"dep"}};
    auto dep_ctx_b = context_subject{context{".ctx_b"}, subject{"dep"}};

    insert(
      dep_ctx_a,
      schema_definition{
        R"(syntax = "proto3"; message Dep { string a = 1; })",
        schema_type::protobuf},
      schema_version{1});

    insert(
      dep_ctx_b,
      schema_definition{
        R"(syntax = "proto3"; message Dep { int32 b = 1; })",
        schema_type::protobuf},
      schema_version{1});

    // Reference dep from ctx_a
    auto test_schema = R"(syntax = "proto3";
import "dep.proto";
message Test { Dep d = 1; })";
    auto schema_a = make_protobuf_schema_definition(
                      _store,
                      subject_schema{
                        context_subject{context{".ctx_a"}, subject{"test"}},
                        schema_definition{
                          test_schema,
                          schema_type::protobuf,
                          {{"dep.proto", dep_ctx_a, schema_version{1}}},
                          {}}})
                      .get();

    // Reference dep from ctx_b
    auto schema_b = make_protobuf_schema_definition(
                      _store,
                      subject_schema{
                        context_subject{context{".ctx_b"}, subject{"test"}},
                        schema_definition{
                          test_schema,
                          schema_type::protobuf,
                          {{"dep.proto", dep_ctx_b, schema_version{1}}},
                          {}}})
                      .get();

    auto name_a = schema_a.name({0});
    auto name_b = schema_b.name({0});
    ASSERT_TRUE(name_a.has_value());
    ASSERT_TRUE(name_b.has_value());
    EXPECT_EQ(name_a.value(), "Test");
    EXPECT_EQ(name_b.value(), "Test");
}

} // namespace pandaproxy::schema_registry
