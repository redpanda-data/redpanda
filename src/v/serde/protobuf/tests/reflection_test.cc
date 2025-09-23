/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "gmock/gmock-matchers.h"
#include "gtest/gtest.h"
#include "src/v/serde/protobuf/tests/codegen_test.proto.h"
#include "src/v/serde/protobuf/tests/test_messages_edition2023.proto.h"

using namespace serde::pb;
using testing::ElementsAre;
using testing::Optional;

TEST(ProtoReflection, CanConvertFieldPathToFieldNumbers) {
    auto as_numbers = [](std::vector<std::string_view> path) {
        protobuf_test_messages::editions::test_all_types_edition2023 proto;
        return proto.convert_field_path_to_numbers(path);
    };
    EXPECT_THAT(as_numbers({"optional_int32"}), Optional(ElementsAre(1)));
    EXPECT_EQ(as_numbers({"optional_foo"}), std::nullopt);
    EXPECT_THAT(as_numbers({"optional_bytes"}), Optional(ElementsAre(15)));
    EXPECT_THAT(
      as_numbers({"optional_nested_message"}), Optional(ElementsAre(18)));
    EXPECT_THAT(
      as_numbers({"optional_nested_message", "a"}),
      Optional(ElementsAre(18, 1)));
    EXPECT_THAT(
      as_numbers({
        "optional_nested_message",
        "corecursive",
        "optional_nested_message",
        "a",
      }),
      Optional(ElementsAre(18, 2, 18, 1)));
    EXPECT_THAT(
      as_numbers({
        "optional_foreign_message",
        "c",
      }),
      Optional(ElementsAre(19, 1)));
    EXPECT_EQ(
      as_numbers({
        "optional_foreign_message",
        "e",
      }),
      std::nullopt);
    EXPECT_THAT(
      as_numbers({
        "repeated_nested_message",
      }),
      Optional(ElementsAre(48)));
    EXPECT_EQ(
      as_numbers({
        "repeated_nested_message",
        "a",
      }),
      std::nullopt);
    EXPECT_THAT(
      as_numbers({
        "map_int32_int32",
      }),
      Optional(ElementsAre(56)));
    EXPECT_EQ(
      as_numbers({
        "map_int32_int32",
        "0",
      }),
      std::nullopt);
    auto well_known = [](std::vector<std::string_view> path) {
        proto::example::well_known_protos proto;
        return proto.convert_field_path_to_numbers(path);
    };
    EXPECT_THAT(well_known({"single_timestamp"}), Optional(ElementsAre(7)));
    EXPECT_THAT(well_known({"single_duration"}), Optional(ElementsAre(1)));
    EXPECT_THAT(well_known({"single_field_mask"}), Optional(ElementsAre(4)));
    EXPECT_EQ(well_known({"single_timestamp", "nanos"}), std::nullopt);
    EXPECT_EQ(well_known({"single_field_mask", "paths"}), std::nullopt);
    EXPECT_EQ(well_known({"single_duration", "seconds"}), std::nullopt);
}

namespace {
template<typename T>
auto FieldValue(const T& field_value) {
    return Optional(
      testing::Field(&field::value, testing::VariantWith<T>(field_value)));
}
template<typename T>
auto FieldType() {
    return Optional(
      testing::Field(&field::value, testing::VariantWith<T>(testing::_)));
}
} // namespace

TEST(ProtoReflection, FieldLookup_TestProto) {
    protobuf_test_messages::editions::test_all_types_edition2023 proto;
    auto lookup_field = [&proto](std::vector<std::string_view> path) {
        return proto.lookup_field_by_path(path);
    };
    EXPECT_THAT(lookup_field({"optional_int32"}), FieldValue<int32_t>(0));
    EXPECT_THAT(lookup_field({"optional_bool"}), FieldValue(false));
    proto.set_optional_int32(42);
    proto.set_optional_bool(true);
    EXPECT_THAT(lookup_field({"optional_int32"}), FieldValue<int32_t>(42));
    EXPECT_THAT(lookup_field({"optional_bool"}), FieldValue(true));
    EXPECT_EQ(proto.get_recursive_message(), nullptr);
    EXPECT_THAT(
      lookup_field({"recursive_message"}), FieldType<base_message*>());
    ASSERT_NE(proto.get_recursive_message(), nullptr);
    EXPECT_THAT(
      lookup_field({"recursive_message", "optional_sint32"}),
      FieldValue<int32_t>(0));
    proto.get_recursive_message()->set_optional_sint32(-9);
    EXPECT_THAT(
      lookup_field({"recursive_message", "optional_sint32"}),
      FieldValue<int32_t>(-9));
    proto = {};
    EXPECT_EQ(proto.get_recursive_message(), nullptr);
    EXPECT_THAT(
      lookup_field({"recursive_message", "optional_sint32"}),
      FieldValue<int32_t>(0));
    ASSERT_NE(proto.get_recursive_message(), nullptr);
    proto.get_recursive_message()->set_optional_sint32(-9);
    EXPECT_THAT(
      lookup_field({"recursive_message", "optional_sint32"}),
      FieldValue<int32_t>(-9));
    EXPECT_EQ(
      lookup_field({"recursive_message", "does_not_exist"}), std::nullopt);
    EXPECT_THAT(
      lookup_field({"map_int64_int64"}),
      FieldType<std::unique_ptr<field::map_value>>());
    EXPECT_EQ(lookup_field({"map_int64_int64", "0"}), std::nullopt);
    EXPECT_THAT(
      lookup_field({"unpacked_float"}),
      FieldType<std::unique_ptr<field::repeated_value>>());
    EXPECT_EQ(lookup_field({"unpacked_float", "0"}), std::nullopt);
    EXPECT_THAT(
      lookup_field({"optional_nested_enum"}),
      FieldValue(raw_enum_value{.number = 0, .name = "FOO"}));
    proto.set_optional_nested_enum(
      protobuf_test_messages::editions::test_all_types_edition2023_nested_enum::
        baz);
    EXPECT_THAT(
      lookup_field({"optional_nested_enum"}),
      FieldValue(raw_enum_value{.number = 2, .name = "BAZ"}));
    EXPECT_FALSE(proto.has_oneof_uint32());
    EXPECT_THAT(lookup_field({"oneof_uint32"}), FieldValue<uint32_t>(0));
    EXPECT_TRUE(proto.has_oneof_uint32());
    proto.set_oneof_string("foo");
    EXPECT_THAT(lookup_field({"oneof_uint32"}), FieldValue<uint32_t>(0));
    EXPECT_TRUE(proto.has_oneof_uint32());
    proto.set_oneof_string("foo");
    EXPECT_THAT(lookup_field({"oneof_string"}), FieldValue<ss::sstring>("foo"));
    EXPECT_THAT(
      lookup_field({"oneof_nested_message", "a"}), FieldValue<int32_t>(0));
}
