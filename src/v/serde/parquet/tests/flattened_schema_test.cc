/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/parquet/flattened_schema.h"
#include "serde/parquet/schema.h"

#include <gtest/gtest.h>

using namespace serde::parquet;

namespace {

template<typename... Args>
schema_element
group_node(ss::sstring name, field_repetition_type rep_type, Args... args) {
    chunked_vector<schema_element> children;
    (children.push_back(std::move(args)), ...);
    return {
      .type = std::monostate{},
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .children = std::move(children),
    };
}

schema_element leaf_node(
  ss::sstring name,
  field_repetition_type rep_type,
  physical_type ptype,
  logical_type ltype = logical_type{}) {
    return {
      .type = ptype,
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .logical_type = ltype,
    };
}

schema_element leaf_node_with_field_id(
  ss::sstring name,
  field_repetition_type rep_type,
  physical_type ptype,
  int32_t fid) {
    return {
      .type = ptype,
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .field_id = fid,
    };
}

// Compare two schemas, ignoring fields that flatten() doesn't preserve
// (position, full path, max_definition_level, max_repetition_level).
void expect_schema_equal(
  const schema_element& actual, const schema_element& expected) {
    EXPECT_EQ(actual.type, expected.type);
    EXPECT_EQ(actual.repetition_type, expected.repetition_type);
    ASSERT_EQ(actual.path.size(), 1)
      << "unflatten should produce single-segment paths";
    EXPECT_EQ(actual.name(), expected.name());
    EXPECT_EQ(actual.field_id, expected.field_id);
    EXPECT_EQ(actual.logical_type, expected.logical_type);
    ASSERT_EQ(actual.children.size(), expected.children.size());
    for (size_t i = 0; i < actual.children.size(); ++i) {
        expect_schema_equal(actual.children[i], expected.children[i]);
    }
}

} // namespace

TEST(FlattenedSchema, RoundTripFlat) {
    using field_repetition_type::optional;
    using field_repetition_type::required;
    auto schema = group_node(
      "schema",
      required,
      leaf_node("a", required, i32_type()),
      leaf_node("b", optional, i64_type()),
      leaf_node("c", required, f64_type()));
    auto flat = flatten(schema);
    auto result = unflatten(flat);
    expect_schema_equal(result, schema);
}

TEST(FlattenedSchema, RoundTripNested) {
    using field_repetition_type::optional;
    using field_repetition_type::required;
    auto schema = group_node(
      "schema",
      required,
      group_node(
        "inner",
        optional,
        leaf_node("x", required, i32_type()),
        leaf_node("y", required, i32_type())),
      leaf_node("z", required, f64_type()));
    auto flat = flatten(schema);
    auto result = unflatten(flat);
    expect_schema_equal(result, schema);
}

TEST(FlattenedSchema, RoundTripDeeplyNested) {
    using field_repetition_type::optional;
    using field_repetition_type::repeated;
    using field_repetition_type::required;
    auto schema = group_node(
      "schema",
      required,
      group_node(
        "persons",
        optional,
        group_node(
          "persons_tuple",
          repeated,
          group_node(
            "name",
            required,
            leaf_node("first", optional, byte_array_type(), string_type()),
            leaf_node("last", required, byte_array_type(), string_type())),
          leaf_node("id", optional, i32_type()),
          group_node(
            "phones",
            repeated,
            group_node(
              "phones_tuple",
              repeated,
              leaf_node(
                "number", optional, byte_array_type(), string_type()))))));
    auto flat = flatten(schema);
    auto result = unflatten(flat);
    expect_schema_equal(result, schema);
}

TEST(FlattenedSchema, RoundTripWithFieldIds) {
    using field_repetition_type::required;
    auto schema = group_node(
      "schema",
      required,
      leaf_node_with_field_id("a", required, i32_type(), 1),
      leaf_node_with_field_id("b", required, i64_type(), 2));
    auto flat = flatten(schema);
    auto result = unflatten(flat);
    expect_schema_equal(result, schema);
}

TEST(FlattenedSchema, RoundTripWithLogicalTypes) {
    using field_repetition_type::optional;
    using field_repetition_type::required;
    auto schema = group_node(
      "schema",
      required,
      leaf_node("s", optional, byte_array_type(), string_type()),
      leaf_node(
        "ts", required, i64_type(), timestamp_type{true, time_unit::micros}),
      leaf_node("d", required, i32_type(), date_type()),
      leaf_node(
        "dec",
        optional,
        byte_array_type(),
        decimal_type{.scale = 2, .precision = 10}));
    auto flat = flatten(schema);
    auto result = unflatten(flat);
    expect_schema_equal(result, schema);
}

TEST(FlattenedSchema, RoundTripSingleLeaf) {
    using field_repetition_type::required;
    auto schema = group_node(
      "schema", required, leaf_node("only", required, bool_type()));
    auto flat = flatten(schema);
    auto result = unflatten(flat);
    expect_schema_equal(result, schema);
}

TEST(FlattenedSchema, EmptyInput) {
    chunked_vector<flattened_schema> empty;
    auto result = unflatten(empty);
    EXPECT_TRUE(result.children.empty());
    EXPECT_TRUE(result.path.empty());
}
