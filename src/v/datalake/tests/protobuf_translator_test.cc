/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/conversion_outcome.h"
#include "datalake/protobuf_translator.h"
#include "iceberg/datatypes.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"

#include <google/protobuf/descriptor.h>
#include <gtest/gtest.h>

#include <memory>

namespace datalake {
const std::string combined_schema = (R"schema(
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
  optional string label = 1;
  optional int32 number = 2;
  message inner_message_t1 {
    optional string inner_label_1 = 1;
    optional int32 inner_number_1 = 2;
  }
 message inner_message_t2 {
    optional string inner_label_2 = 1;
    optional int32 inner_number_2 = 2;
  }
}
)schema");

TEST(ProtobufTranslatorTest, BufferTranslator) {
    std::vector<int32_t> message_offsets{2, 0};
    datalake::protobuf_buffer_translator translator(
      combined_schema, message_offsets);

    // Check schema
    iceberg::struct_type expected_schema;
    expected_schema.fields.emplace_back(iceberg::nested_field::create(
      1, "inner_label_1", iceberg::field_required::no, iceberg::string_type{}));
    expected_schema.fields.emplace_back(iceberg::nested_field::create(
      2, "inner_number_1", iceberg::field_required::no, iceberg::int_type{}));
    auto schema_outcome = translator.get_schema();
    ASSERT_TRUE(schema_outcome.has_value());
    iceberg::struct_type& schema = schema_outcome.value();
    EXPECT_EQ(schema, expected_schema);

    // Build test value
    // FIXME: pull this out into a function
    google::protobuf::DynamicMessageFactory factory;
    const google::protobuf::Descriptor* msg_desc = translator.get_descriptor(
      message_offsets);
    const google::protobuf::Message* prototype_msg = factory.GetPrototype(
      msg_desc);
    assert(prototype_msg != nullptr);

    google::protobuf::Message* mutable_msg = prototype_msg->New();
    ASSERT_NE(mutable_msg, nullptr);
    auto reflection = mutable_msg->GetReflection();
    for (int field_idx = 0;
         field_idx < mutable_msg->GetDescriptor()->field_count();
         field_idx++) {
        auto field_desc = mutable_msg->GetDescriptor()->field(field_idx);
        if (field_desc->name() == "inner_label_1") {
            reflection->SetString(mutable_msg, field_desc, "Hello");
        }
        if (field_desc->name() == "inner_number_1") {
            reflection->SetInt32(mutable_msg, field_desc, 13);
        }
    }
    iobuf serialized = iobuf::from(mutable_msg->SerializeAsString());
    delete mutable_msg;

    // Test parsing value
    optional_value_outcome value_outcome
      = translator.translate_value(std::move(serialized)).get0();
    ASSERT_TRUE(value_outcome.has_value());
    auto expected_value = iceberg::tests::make_struct_value(
      iceberg::tests::value_spec{
        .forced_num_val = 13,
        .forced_string_val = "Hello",
      },
      std::move(expected_schema));

    auto& value = std::get<std::unique_ptr<iceberg::struct_value>>(
      value_outcome.value().value());
    EXPECT_EQ(*value, expected_value);
}

} // namespace datalake
