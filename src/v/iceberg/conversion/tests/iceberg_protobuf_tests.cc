/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "gtest/gtest.h"
#include "iceberg/conversion/protobuf_utils.h"
#include "iceberg/conversion/schema_protobuf.h"
#include "iceberg/conversion/tests/gmock_iceberg_matchers.h"
#include "iceberg/conversion/tests/proto_definitions.h"
#include "iceberg/conversion/values_protobuf.h"
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
#include <google/protobuf/message.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <memory>
#include <optional>

using namespace iceberg;
using namespace iceberg::testing;
using namespace ::testing;

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

    auto result = iceberg::type_to_iceberg(*descriptor);
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

    auto result = iceberg::type_to_iceberg(*d);
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

/**
 * message StructWithOneOf {
 *   oneof oneof_field {
 *       int32 oneof_uint32 = 1;
 *       string oneof_string = 2;
 *       bytes oneof_bytes = 3;
 *       bool oneof_bool = 4;
 *   }
 *}
 */

TEST_CORO(SchemaProtobuf, TestMessageWithOneOfField) {
    auto d = StructWithOneOf::GetDescriptor();
    auto result = iceberg::type_to_iceberg(*d);
    ASSERT_FALSE_CORO(result.has_error());
    auto field = std::move(result.value());
    EXPECT_THAT(
      field.fields,
      ElementsAre(
        IsField(1, "oneof_uint32", long_type{}),
        IsField(2, "oneof_string", string_type{}),
        IsField(3, "oneof_bytes", binary_type{}),
        IsField(4, "oneof_bool", boolean_type{})));
}

TEST_CORO(SchemaProtobuf, TestMessageWithTimestamp) {
    auto d = StructWithTimestamp::GetDescriptor();
    auto result = iceberg::type_to_iceberg(*d);
    ASSERT_FALSE_CORO(result.has_error());
    auto field = std::move(result.value());
    EXPECT_THAT(
      field.fields, ElementsAre(IsField(1, "timestamp", timestamp_type{})));
}

TEST_CORO(SchemaProtobuf, TestMessageWithStruct) {
    auto d = StructWithStruct::GetDescriptor();
    auto result = iceberg::type_to_iceberg(*d);
    ASSERT_FALSE_CORO(result.has_error());
    auto field = std::move(result.value());
    EXPECT_THAT(
      field.fields, ElementsAre(IsField(1, "struct_field", string_type{})));
}

TEST_CORO(SchemaProtobuf, TestMessageWithValue) {
    auto d = StructWithValue::GetDescriptor();
    auto result = iceberg::type_to_iceberg(*d);
    ASSERT_FALSE_CORO(result.has_error());
    auto field = std::move(result.value());
    EXPECT_THAT(
      field.fields, ElementsAre(IsField(1, "value_field", string_type{})));
}

TEST_CORO(SchemaProtobuf, TestMessageWithListValue) {
    auto d = StructWithListValue::GetDescriptor();
    auto result = iceberg::type_to_iceberg(*d);
    ASSERT_FALSE_CORO(result.has_error());
    auto field = std::move(result.value());
    EXPECT_THAT(
      field.fields, ElementsAre(IsField(1, "list_value_field", string_type{})));
}

TEST_CORO(SchemaProtobuf, TestProtoTestMessages) {
    auto d = protobuf_test_messages::editions::TestAllTypesEdition2023::
      GetDescriptor();
    auto result = iceberg::type_to_iceberg(*d);
    ASSERT_FALSE_CORO(result.has_error());
}

TEST(SchemaProtobuf, TestInvalidSchema) {
    for (auto desc :
         {RecursiveMessage::GetDescriptor(),
          RecursiveMessageNested::GetDescriptor()}) {
        auto result = iceberg::type_to_iceberg(*desc);
        ASSERT_TRUE(result.has_error());
    }
}

template<typename Message>
ss::future<iceberg::optional_value_outcome>
serialize_and_convert(const Message& msg) {
    auto buffer = iobuf::from(msg.SerializeAsString());
    auto parsed = co_await serde::pb::parse(
      std::move(buffer), *msg.GetDescriptor());

    co_return co_await iceberg::proto_parsed_message_to_value(
      std::move(parsed), *msg.GetDescriptor());
}

TEST_CORO(values_protobuf, TestSimpleValueConversion) {
    Person message;
    message.set_id(1234);
    message.set_name("test person");
    message.set_email("test@redpanda.com");
    message.mutable_dept()->set_name("Redpanda test dept");
    message.mutable_dept()->set_id(1024);

    auto result = co_await serialize_and_convert(message);

    ASSERT_TRUE_CORO(result.has_value());
    auto opt_value = std::move(result.value());
    ASSERT_TRUE_CORO(opt_value.has_value());
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(opt_value.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(
        OptionalIcebergPrimitive<string_value>("test person"),
        OptionalIcebergPrimitive<int_value>(1234),
        OptionalIcebergPrimitive<string_value>("test@redpanda.com"),
        IcebergStruct(
          OptionalIcebergPrimitive<int_value>(1024),
          OptionalIcebergPrimitive<string_value>("Redpanda test dept")),
        Eq(std::nullopt)));
}

Partition
make_partition(int32_t id, const std::vector<std::pair<int, int>>& replicas) {
    Partition ret;
    ret.set_id(id);
    ret.set_state(State::ACTIVE);
    for (auto& bs : replicas) {
        BrokerShard broker_shard;
        broker_shard.set_id(bs.first);
        broker_shard.set_shard(bs.second);
        ret.mutable_replicas()->Add(std::move(broker_shard));
    }

    return ret;
}

TEST_CORO(values_protobuf, TestComplexValueConversion) {
    Metadata message;
    message.mutable_nodes()->Add(11);
    message.mutable_nodes()->Add(12);
    message.mutable_nodes()->Add(13);
    Topic topic_1;
    topic_1.set_name("topic_1");

    topic_1.mutable_partitions()->Add(
      make_partition(0, {{11, 1}, {12, 0}, {13, 4}}));
    topic_1.mutable_partitions()->Add(
      make_partition(1, {{12, 1}, {11, 0}, {13, 4}}));
    topic_1.mutable_partitions()->Add(
      make_partition(3, {{11, 3}, {12, 1}, {13, 2}}));

    message.mutable_topics()->emplace("topic_1", std::move(topic_1));

    Topic topic_2;
    topic_2.set_name("topic_2");
    topic_2.mutable_partitions()->Add(make_partition(0, {{11, 0}}));
    message.mutable_topics()->emplace("topic_2", std::move(topic_2));

    auto result = co_await serialize_and_convert(message);

    ASSERT_TRUE_CORO(result.has_value());
    auto optional_value = std::move(result.value());
    ASSERT_TRUE_CORO(result.value().has_value());
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(optional_value.value()));

    EXPECT_THAT(
      result_value->fields,
      ElementsAre(
        IcebergMap(UnorderedElementsAre(
          IcebergKeyValue(
            IcebergPrimitive<string_value>("topic_2"),
            // topic
            IcebergStruct(
              OptionalIcebergPrimitive<string_value>("topic_2"),
              IcebergList(ElementsAre(
                // partition
                IcebergStruct(
                  OptionalIcebergPrimitive<int_value>(0),
                  IcebergList(ElementsAre(
                    // broker shard
                    IcebergStruct(
                      OptionalIcebergPrimitive<iceberg::long_value>(11),
                      OptionalIcebergPrimitive<iceberg::int_value>(0)))),
                  OptionalIcebergPrimitive<int_value>(0)))))),
          IcebergKeyValue(
            IcebergPrimitive<string_value>("topic_1"),
            // topic
            IcebergStruct(
              OptionalIcebergPrimitive<string_value>("topic_1"),
              IcebergList(ElementsAre(
                // partition
                IcebergStruct(OptionalIcebergPrimitive<int_value>(0), _, _),
                // partition
                IcebergStruct(OptionalIcebergPrimitive<int_value>(1), _, _),
                // partition
                IcebergStruct(
                  OptionalIcebergPrimitive<int_value>(3), _, _))))))),
        IcebergList(ElementsAre(
          OptionalIcebergPrimitive<long_value>(11),
          OptionalIcebergPrimitive<long_value>(12),
          OptionalIcebergPrimitive<long_value>(13)))));
}

TEST_CORO(values_protobuf, TestEmptyMessage) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 test_msg;
    // Make sure that empty message is parsable
    auto result = co_await serialize_and_convert(test_msg);
    ASSERT_TRUE_CORO(result.has_value());

    auto iceberg_value = std::move(result.value().value());

    auto st_value = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(iceberg_value));
    auto descriptor = test_msg.descriptor();
    for (auto i = 0; i < descriptor->field_count(); ++i) {
        auto field_descriptor = descriptor->field(i);
        auto& field_value = st_value->fields[i];
        // explicit presence tracking implies that not set value will be
        // represented with empty optional
        if (field_descriptor->has_presence()) {
            ASSERT_EQ_CORO(field_value, std::nullopt);
        } else {
            if (field_descriptor->is_map()) {
                // empty map
                EXPECT_THAT(field_value, IcebergMap(ElementsAre()));
            } else if (field_descriptor->is_repeated()) {
                // empty list
                EXPECT_THAT(field_value, IcebergList(ElementsAre()));
            } else {
                switch (field_descriptor->type()) {
                case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<double_value>(
                        field_descriptor->default_value_double()));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<float_value>(
                        field_descriptor->default_value_float()));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_INT64:
                case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                case google::protobuf::FieldDescriptor::TYPE_SINT64:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<long_value>(
                        field_descriptor->default_value_int64()));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_UINT64:
                case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                case google::protobuf::FieldDescriptor::TYPE_GROUP:
                case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
                    ASSERT_TRUE_CORO(false);
                    break;
                case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                case google::protobuf::FieldDescriptor::TYPE_SINT32:
                case google::protobuf::FieldDescriptor::TYPE_INT32:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<int_value>(
                        field_descriptor->default_value_int32()));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                case google::protobuf::FieldDescriptor::TYPE_UINT32:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<long_value>(
                        field_descriptor->default_value_uint32()));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_BOOL:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<boolean_value>(
                        field_descriptor->default_value_bool()));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_STRING:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<string_value>(
                        field_descriptor->default_value_string()));
                    break;
                case google::protobuf::FieldDescriptor::TYPE_BYTES: {
                    EXPECT_TRUE(field_value.has_value());
                    EXPECT_TRUE(
                      std::holds_alternative<primitive_value>(*field_value));
                    auto pv = std::get<primitive_value>(
                      std::move(field_value.value()));
                    EXPECT_TRUE(std::holds_alternative<binary_value>(pv));
                    EXPECT_EQ(std::get<binary_value>(pv).val, iobuf{});
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_ENUM:
                    EXPECT_THAT(
                      field_value,
                      OptionalIcebergPrimitive<int_value>(
                        field_descriptor->default_value_enum()->number()));
                    break;
                }
            }
        }
    }
}

TEST_CORO(values_protobuf, TestMapConversions) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 test_msg;
    // put 3 elements to one of the maps
    test_msg.mutable_map_int32_int32()->emplace(0, 1);
    test_msg.mutable_map_int32_int32()->emplace(1, 0);
    test_msg.mutable_map_int32_int32()->emplace(2, 1);
    auto descriptor = test_msg.GetDescriptor();

    auto map_desc = descriptor->FindFieldByName("map_int32_int32");
    auto result = co_await serialize_and_convert(test_msg);

    ASSERT_TRUE_CORO(result.has_value() && result.value().has_value());
    auto result_value = std::get<std::unique_ptr<struct_value>>(
      std::move(result.value().value()));

    EXPECT_THAT(
      result_value->fields[map_desc->index()],
      IcebergMap(UnorderedElementsAre(
        IcebergKeyValue(
          IcebergPrimitive<iceberg::int_value>(0),
          OptionalIcebergPrimitive<iceberg::int_value>(1)),
        IcebergKeyValue(
          IcebergPrimitive<iceberg::int_value>(1),
          OptionalIcebergPrimitive<iceberg::int_value>(0)),
        IcebergKeyValue(
          IcebergPrimitive<iceberg::int_value>(2),
          OptionalIcebergPrimitive<iceberg::int_value>(1)))));
}

TEST_CORO(values_protobuf, TestSettingEmtpyNestedMessage) {
    protobuf_test_messages::editions::TestAllTypesEdition2023 test_msg;
    // just ask for mutable nested message to mark it set
    test_msg.mutable_optional_nested_message();
    auto result = co_await serialize_and_convert(test_msg);
    auto d = test_msg.descriptor();
    auto field_descriptor = d->FindFieldByName("optional_nested_message");

    ASSERT_TRUE_CORO(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE_CORO(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    auto idx = field_descriptor->index();
    auto& field = struct_v->fields[idx];

    // Nested message is empty so it fields are all defaults.
    EXPECT_THAT(
      field,
      IcebergStruct(
        OptionalIcebergPrimitive<int_value>(
          field_descriptor->default_value_int32())));
}

TEST_CORO(values_protobuf, TestSettingDeeplyNestedMessages) {
    TopLevel test_msg;
    // just ask for mutable nested message to mark it set
    test_msg.mutable_nested_level_one();
    auto result = co_await serialize_and_convert(test_msg);

    ASSERT_TRUE_CORO(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE_CORO(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    auto& field = struct_v->fields[0];

    // Nested message is empty so its nested field is explicitly set to empty
    EXPECT_THAT(field, IcebergStruct(Eq(std::nullopt)));
}

TEST(values_protobuf, TestUInt64Fallback) {
    StructWithUnsignedInt uint;
    uint.set_valid(-123);
    uint.set_invalid(123);
    StructWithUnsignedFixed ufixed;
    ufixed.set_valid(-123);
    ufixed.set_invalid(123);
    std::vector<google::protobuf::Message*> messages;
    messages.push_back(static_cast<google::protobuf::Message*>(&uint));
    messages.push_back(static_cast<google::protobuf::Message*>(&ufixed));
    for (auto& m : messages) {
        auto result = serialize_and_convert(*m).get();

        ASSERT_TRUE(result.has_value());
        auto r_opt = std::move(result.value());
        ASSERT_TRUE(r_opt.has_value());
        auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
          std::move(r_opt.value()));

        ASSERT_THAT(
          struct_v->fields,
          ElementsAre(
            OptionalIcebergPrimitive<long_value>(-123),
            OptionalIcebergPrimitive<string_value>("123")));
    }
}

TEST(values_protobuf, TestTimestamp) {
    StructWithTimestamp ts;
    ts.mutable_timestamp()->set_seconds(1743540027);
    ts.mutable_timestamp()->set_nanos(1635902);
    auto result = serialize_and_convert(ts).get();
    ASSERT_TRUE(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    ASSERT_THAT(
      struct_v->fields,
      ElementsAre(OptionalIcebergPrimitive<timestamp_value>(1743540027001635)));
}

TEST(values_protobuf, TestStruct) {
    StructWithStruct s;

    // Look at the EXPECT_THAT below to see the structure.
    auto* struct_field = s.mutable_struct_field();
    (*struct_field->mutable_fields())["string"].set_string_value("test_string");
    (*struct_field->mutable_fields())["number"].set_number_value(12.04);
    (*struct_field->mutable_fields())["bool"].set_bool_value(true);
    (*struct_field->mutable_fields())["null"].set_null_value(
      google::protobuf::NullValue::NULL_VALUE);

    auto* nested
      = (*struct_field->mutable_fields())["nested"].mutable_struct_value();
    (*nested->mutable_fields())["name"].set_string_value("nested");

    auto* array = (*nested->mutable_fields())["items"].mutable_list_value();
    array->add_values()->set_string_value("array_string");
    array->add_values()->set_number_value(42.5);

    auto* array_struct = array->add_values()->mutable_struct_value();
    (*array_struct->mutable_fields())["name"].set_string_value("taco");
    (*array_struct->mutable_fields())["value"].set_number_value(99.9);

    auto* nested_list = array->add_values()->mutable_list_value();
    nested_list->add_values()->set_string_value("nested1");
    nested_list->add_values()->set_string_value("nested2");

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    ASSERT_EQ(struct_v->fields.size(), 1);
    ASSERT_TRUE(struct_v->fields[0].has_value());

    const auto& struct_json = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(struct_v->fields[0].value()));

    EXPECT_THAT(struct_json, IsJSON(R"({
        "string": "test_string",
        "number": 12.04,
        "bool": true,
        "null": null,
        "nested": {
          "name": "nested",
          "items": [
            "array_string",
            42.5,
            {"name": "taco", "value": 99.9},
            ["nested1", "nested2"]
          ]
        }
      })"));
}

TEST(values_protobuf, TestStructWithEmptyStructAndArray) {
    StructWithStruct s;

    // Look at the EXPECT_THAT below to see the structure.
    auto* struct_field = s.mutable_struct_field();
    (*struct_field->mutable_fields())["empty_struct"].mutable_struct_value();
    (*struct_field->mutable_fields())["empty_array"].mutable_list_value();

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    ASSERT_EQ(struct_v->fields.size(), 1);
    ASSERT_TRUE(struct_v->fields[0].has_value());

    const auto& struct_json = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(struct_v->fields[0].value()));

    EXPECT_THAT(struct_json, IsJSON(R"({
        "empty_struct": {},
        "empty_array": []
      })"));
}

TEST(values_protobuf, TestEmptyStruct) {
    // Leave struct_field empty.
    StructWithStruct s;

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    ASSERT_EQ(struct_v->fields.size(), 1);
    EXPECT_FALSE(struct_v->fields[0].has_value());
}

// NB: We test serializing all Value variants in TestStruct.
//     This tests that Value is convertible, not that all variants of
//     Value are serializable to JSON.
TEST(values_protobuf, TestValue) {
    StructWithValue s;
    s.mutable_value_field()->set_string_value("test");

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    ASSERT_EQ(struct_v->fields.size(), 1);
    ASSERT_TRUE(struct_v->fields[0].has_value());

    const auto& value_json = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(struct_v->fields[0].value()));

    EXPECT_THAT(value_json, IsJSON(R"("test")"));
}

// See also TestStruct for ListValue values with all value Variants.
TEST(values_protobuf, TestListValue) {
    StructWithListValue s;
    auto* list = s.mutable_list_value_field();
    list->add_values()->set_string_value("first");
    list->add_values()->set_number_value(2);

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    ASSERT_EQ(struct_v->fields.size(), 1);
    ASSERT_TRUE(struct_v->fields[0].has_value());

    const auto& list_json = std::get<iceberg::string_value>(
      std::get<iceberg::primitive_value>(struct_v->fields[0].value()));

    EXPECT_THAT(list_json, IsJSON(R"(["first", 2])"));
}

TEST_CORO(values_protobuf, TestNotSupportedMessageType) {
    RecursiveMessage recursive;
    recursive.set_field(10);
    recursive.mutable_recursive()->set_field(12);
    RecursiveMessageNested recursive_nested;
    recursive_nested.mutable_corecursive()->add_foo();
    recursive_nested.mutable_corecursive()->add_foo();
    recursive_nested.mutable_corecursive()->add_foo();
    std::vector<google::protobuf::Message*> messages;
    messages.push_back(static_cast<google::protobuf::Message*>(&recursive));
    messages.push_back(
      static_cast<google::protobuf::Message*>(&recursive_nested));
    for (auto& m : messages) {
        auto result = co_await serialize_and_convert(*m);
        ASSERT_TRUE_CORO(result.has_error());
    }
}

//  syntax = "proto2";
// message MessageWithOptionalFields {
//     message Nested {
//         optional int32 field = 1;
//     }

//     required string required_string = 1;
//     optional string string_with_default = 2 [default = "redpanda"];
//     optional int32 optional_int = 3;
//     required int32 required_int = 4;
//     optional Nested optional_nested = 5;
//     map<int32,int64> map = 6;
//     repeated int32 list = 7;

//     oneof oneof_field {
//         uint32 oneof_uint32 = 11;
//         string oneof_string = 12;
//         bytes oneof_bytes = 13;
//         bool oneof_bool = 14;
//     }
// }

TEST_CORO(values_protobuf, TestProto2FieldPresence) {
    proto2::MessageWithOptionalFields msg;
    // set required fields
    msg.set_required_int(123);
    msg.set_required_string("required_str");

    auto result = co_await serialize_and_convert(msg);
    ASSERT_TRUE_CORO(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE_CORO(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    // Protobuf differentiate the two types of field presence tracking:
    //
    // - explicit presence - one can differentiate if field was actually set
    // or left unset (unset value is included into binary representation)
    //
    // - implicit - one can not differentiate if the field was set to
    // default or left unset, the
    //
    // Proto 2 presence rules:
    // | field type                                   | explicit pres. |
    // -----------------------------------------------+----------------
    // | Singular numeric (integer or floating point)	|       ✔️        |
    // | Singular enum	                              |       ✔️        |
    // | Singular string or bytes	                    |       ✔️        |
    // | Singular message	                            |       ✔️        |
    // | Repeated                                     |                |
    // | Oneofs	                                      |       ✔️        |
    // | Maps                                         |                |

    EXPECT_THAT(
      struct_v->fields,
      ElementsAre(
        OptionalIcebergPrimitive<string_value>("required_str"),
        OptionalIcebergPrimitive<string_value>("redpanda"),
        Eq(std::nullopt), // explicit presence, the filed should not be set
        OptionalIcebergPrimitive<int_value>(123),
        Eq(std::nullopt),          // singular message
        IcebergMap(ElementsAre()), // map was not set and no presence is tracked
        IcebergList(ElementsAre()), // not set repeated, no presence is tracked
        Eq(std::nullopt),           // Explicit presence of oneof fields
        Eq(std::nullopt),
        Eq(std::nullopt),
        Eq(std::nullopt)));
}

TEST_CORO(values_protobuf, TestProtoNumbers) {
    proto2::NumericScalars msg;
    msg.set_a(3.14);
    msg.set_b(53.4f);
    msg.set_c(5234);
    msg.set_d(12342542);
    msg.set_e(52345235);
    msg.set_f(45353453435);
    msg.set_g(-234234);
    msg.set_h(-2342352435);
    msg.set_i(23453245);
    msg.set_j(4234234);
    msg.set_k(-234234);
    msg.set_l(-234234342);

    auto result = co_await serialize_and_convert(msg);
    ASSERT_TRUE_CORO(result.has_value());
    auto r_opt = std::move(result.value());
    ASSERT_TRUE_CORO(r_opt.has_value());
    auto struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
      std::move(r_opt.value()));

    EXPECT_THAT(
      struct_v->fields,
      ElementsAre(
        OptionalIcebergPrimitive<double_value>(3.14),
        OptionalIcebergPrimitive<float_value>(53.4f),
        OptionalIcebergPrimitive<int_value>(5234),
        OptionalIcebergPrimitive<long_value>(12342542),
        OptionalIcebergPrimitive<long_value>(52345235),
        OptionalIcebergPrimitive<string_value>("45353453435"),
        OptionalIcebergPrimitive<int_value>(-234234),
        OptionalIcebergPrimitive<long_value>(-2342352435),
        OptionalIcebergPrimitive<long_value>(23453245),
        OptionalIcebergPrimitive<string_value>("4234234"),
        OptionalIcebergPrimitive<int_value>(-234234),
        OptionalIcebergPrimitive<long_value>(-234234342)));
}

TEST(values_protobuf, TestValueWithNaN) {
    StructWithStruct s;
    auto* struct_field = s.mutable_struct_field();
    (*struct_field->mutable_fields())["nan_value"].set_number_value(
      std::numeric_limits<double>::quiet_NaN());

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_error());
    EXPECT_THAT(
      result.error().what(),
      HasSubstr("NaN and Infinity are not supported in JSON"));
}

TEST(values_protobuf, TestValueWithInfinity) {
    StructWithStruct s;
    auto* struct_field = s.mutable_struct_field();
    (*struct_field->mutable_fields())["inf_value"].set_number_value(
      std::numeric_limits<double>::infinity());

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_error());
    EXPECT_THAT(
      result.error().what(),
      HasSubstr("NaN and Infinity are not supported in JSON"));
}

TEST(values_protobuf, TestStructDepthLimit) {
    StructWithStruct s;

    // Create a deeply nested structure exceeding max_recursion_depth
    auto* current = s.mutable_struct_field();
    for (int i = 0; i < max_recursion_depth + 1; ++i) {
        current = (*current->mutable_fields())[fmt::format("nested_{}", i)]
                    .mutable_struct_value();
    }

    auto result = serialize_and_convert(s).get();
    ASSERT_TRUE(result.has_error());
    EXPECT_THAT(
      result.error().what(),
      HasSubstr(
        fmt::format(
          "Maximum recursion depth {} exceeded", max_recursion_depth)));
}
