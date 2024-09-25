// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "datalake/schema_protobuf.h"
#include "gtest/gtest.h"
#include "iceberg/datatypes.h"
#include "test_utils/runfiles.h"
#include "test_utils/test.h"
#include "utils/file_io.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>
#include <fmt/ostream.h>
#include <gmock/gmock.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

using namespace iceberg;
using namespace testing;
namespace {

const char* basic_proto_str = R"(
syntax = "proto3";

message Department {
    int32 id = 1;
    string name = 2;
}

message Person {
    string name = 1;
    int32 id = 2;
    string email = 3;
    Department dept = 4;
    string test_coverage = 5;
}
)";
const char* complex_proto_str = R"(
syntax = "proto3";

enum State {
  ACTIVE = 0;
  DISABLED = 1;
}

message BrokerShard {
  int64 id = 1;
  int32 shard = 2;
}

message Partition {
    int32 id = 1;
    repeated BrokerShard replicas = 2;
    State state = 3;
}

message Topic {
    string name = 1;
    repeated Partition partitions = 2;
}

message Metadata {
    map<string,Topic> topics = 1;
    repeated int64 nodes = 2;
}

)";
static constexpr auto file_descriptor_name = "test_schema_file";
AssertionResult parse_proto_definition_string(
  const ss::sstring& proto_str, google::protobuf::DescriptorPool& pool) {
    google::protobuf::compiler::Parser parser;

    google::protobuf::io::ArrayInputStream input_stream(
      proto_str.c_str(), proto_str.size());

    google::protobuf::io::Tokenizer tokenizer(&input_stream, nullptr);
    google::protobuf::FileDescriptorProto fdp;
    fdp.set_name(file_descriptor_name);

    auto parse_result = parser.Parse(&tokenizer, &fdp);
    if (!parse_result) {
        return AssertionFailure(
          Message("Unable to parse protocol buffer defintion string"));
    }
    pool.BuildFile(fdp);
    return AssertionSuccess();
}

}; // namespace

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
    google::protobuf::DescriptorPool d_pool;
    ASSERT_TRUE(parse_proto_definition_string(basic_proto_str, d_pool));
    auto person_descriptor = d_pool.FindMessageTypeByName("Person");

    auto result = datalake::type_to_iceberg(*person_descriptor);
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
    google::protobuf::DescriptorPool d_pool;
    ASSERT_TRUE(parse_proto_definition_string(complex_proto_str, d_pool));
    auto md = d_pool.FindMessageTypeByName("Metadata");

    auto result = datalake::type_to_iceberg(*md);
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
    auto desc_path = test_utils::get_runfile_path(
      "src/v/datalake/tests/testdata/"
      "iceberg_ready_test_messages_edition2023.proto");

    if (!desc_path.has_value()) {
        desc_path = "iceberg_ready_test_messages_edition2023.proto";
    }

    auto proto_string = co_await read_fully_to_string(desc_path.value());

    google::protobuf::DescriptorPool d_pool;
    ASSERT_TRUE_CORO(parse_proto_definition_string(proto_string, d_pool));

    auto message_desc = d_pool.FindMessageTypeByName(
      "protobuf_test_messages.editions.TestAllTypesEdition2023");
    auto result = datalake::type_to_iceberg(*message_desc);
    ASSERT_FALSE_CORO(result.has_error());
}

const char* not_supported_messages = R"(
  syntax = "proto3";
  message StructWithUnsignedInt {
      int64 valid = 1;
      uint64 invalid = 2;
  }

  message StructWithUnsignedFixed {
      int64 valid = 1;
      fixed64 invalid = 2;
  }
  
  message RecursiveMessage {
      int32 field = 1;
      RecursiveMessage recursive = 2;
  }

  message RecursiveMessageNested {
    
    message Nested {
      repeated RecursiveMessageNested foo = 1;
    }


    int32 field = 1;
    Nested corecursive = 2;
  }
)";

TEST(SchemaProtobuf, TestInvalidSchema) {
    google::protobuf::DescriptorPool d_pool;
    ASSERT_TRUE(parse_proto_definition_string(not_supported_messages, d_pool));
    for (auto& m :
         {"StructWithUnsignedInt",
          "StructWithUnsignedFixed",
          "RecursiveMessage",
          "RecursiveMessageNested"}) {
        auto md = d_pool.FindMessageTypeByName(m);
        auto result = datalake::type_to_iceberg(*md);
        ASSERT_TRUE(result.has_error());
    }
}
