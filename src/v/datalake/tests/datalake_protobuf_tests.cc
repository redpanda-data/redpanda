// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "datalake/schema_protobuf.h"
#include "datalake/tests/proto_definitions.h"
#include "gtest/gtest.h"
#include "iceberg/datatypes.h"
#include "test_utils/test.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>
#include <fmt/ostream.h>
#include <gmock/gmock.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <gtest/gtest.h>

#include <memory>

using namespace iceberg;
using namespace testing;

MATCHER_P3(IsField, id, name, type, "") {
    *result_listener << fmt::format(
      "field(id: {}, name: {}, type: {}) expected to be equal to "
      "field(id: {}, name: {}, type: {})\n",
      arg->id,
      arg->name,
      arg->type,
      id,
      name,
      type);

    return arg->id == id && arg->name == name && arg->type == type;
}

TEST(SchemaProtobuf, TestSimpleSchemaConversion) {
    auto descriptor = Person::GetDescriptor();

    auto result = datalake::type_to_iceberg(*descriptor);
    ASSERT_FALSE(result.has_error());
    auto& top_level = result.value();
    // string name = 1;
    // string id = 2;
    // string email = 3;
    // string test_coverage = 5;

    EXPECT_THAT(
      top_level.fields,
      ElementsAre(
        IsField(1, "name", string_type{}),
        IsField(2, "id", int_type{}),
        IsField(3, "email", string_type{}),
        A<nested_field_ptr>(),
        IsField(5, "test_coverage", string_type{})));

    // Department dept = 4; (nested struct)
    EXPECT_EQ(top_level.fields[3]->id, 4);
    EXPECT_EQ(top_level.fields[3]->name, "dept");
    auto& nested = std::get<struct_type>(top_level.fields[3]->type);
    // int32 id = 1;
    // string name = 2;
    EXPECT_THAT(
      nested.fields,
      ElementsAre(
        IsField(1, "id", int_type{}), IsField(2, "name", string_type{})));
}

/**
 *enum State {
 *  Active = 0;
 *  InActive = 1;
 *}
 *
 *message BrokerShard {
 *  int64 id = 1;
 *  int32 shard = 2;
 *}
 *
 *message Partition {
 *    int32 id = 1;
 *    repeated BrokerShard replicas = 2;
 *    State state = 3;
 *}
 *
 *message Topic {
 *    string name = 1;
 *    repeated Partition partitions = 2;
 *}
 *
 *message Metadata {
 *    map<string,Topic> topics = 1;
 *    repeated int64 nodes = 2;
 *}
 */
// this schema contains a list map and nested fields
// static_assert(
// SchemaConverter<protobuf_schema_converter, google::protobuf::Descriptor>);
TEST(SchemaProtobuf, TestComplexSchema) {
    auto d = Metadata::GetDescriptor();

    auto result = datalake::type_to_iceberg(*d);
    ASSERT_FALSE(result.has_exception());

    auto& top_level = result.value();
    auto& topics_map_field = top_level.fields[0];
    auto& topics_map_type = std::get<map_type>(top_level.fields[0]->type);
    EXPECT_EQ(topics_map_field->id, 1);
    EXPECT_EQ(topics_map_field->name, "topics");
    EXPECT_THAT(topics_map_type.key_field, IsField(1, "key", string_type{}));

    EXPECT_EQ(topics_map_type.value_field->id, 2);
    EXPECT_EQ(topics_map_type.value_field->name, "value");

    auto& topic_type = std::get<struct_type>(topics_map_type.value_field->type);
    EXPECT_THAT(topic_type.fields[0], IsField(1, "name", string_type{}));

    EXPECT_EQ(topic_type.fields[1]->id, 2);
    EXPECT_EQ(topic_type.fields[1]->name, "partitions");
    auto& partition_list_type = std::get<list_type>(topic_type.fields[1]->type);
    EXPECT_EQ(partition_list_type.element_field->id, 2);
    EXPECT_EQ(partition_list_type.element_field->name, "element");

    auto& partition_type = std::get<struct_type>(
      partition_list_type.element_field->type);
    EXPECT_THAT(partition_type.fields[0], IsField(1, "id", int_type{}));

    EXPECT_EQ(partition_type.fields[1]->id, 2);
    EXPECT_EQ(partition_type.fields[1]->name, "replicas");
    auto& replica_list_type = std::get<list_type>(
      partition_type.fields[1]->type);

    // enum is mapped to signed integer
    EXPECT_THAT(partition_type.fields[2], IsField(3, "state", int_type{}));

    auto& broker_shard_type = std::get<struct_type>(
      replica_list_type.element_field->type);
    EXPECT_THAT(
      broker_shard_type.fields,
      ElementsAre(
        IsField(1, "id", long_type{}), IsField(2, "shard", int_type{})));
}

TEST_CORO(SchemaProtobuf, TestProtoTestMessages) {
    auto d = protobuf_test_messages::editions::TestAllTypesEdition2023::
      GetDescriptor();
    auto result = datalake::type_to_iceberg(*d);
    ASSERT_FALSE_CORO(result.has_error());
}

TEST(SchemaProtobuf, TestInvalidSchema) {
    for (auto desc :
         {StructWithUnsignedInt::GetDescriptor(),
          StructWithUnsignedFixed::GetDescriptor(),
          RecursiveMessage::GetDescriptor(),
          RecursiveMessageNested::GetDescriptor()}) {
        auto result = datalake::type_to_iceberg(*desc);
        ASSERT_TRUE(result.has_error());
    }
}
