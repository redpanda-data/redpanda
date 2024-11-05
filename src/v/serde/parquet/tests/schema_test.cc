/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

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
      .name = std::move(name),
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
      .name = std::move(name),
      .logical_type = ltype,
    };
}

struct indexed_info {
    int32_t index;
    int16_t def_level;
    int16_t rep_level;
};

template<typename... Args>
schema_element indexed_group_node(
  indexed_info info,
  ss::sstring name,
  field_repetition_type rep_type,
  Args... args) {
    chunked_vector<schema_element> children;
    (children.push_back(std::move(args)), ...);
    return {
      .position = info.index,
      .type = std::monostate{},
      .repetition_type = rep_type,
      .name = std::move(name),
      .children = std::move(children),
      .max_definition_level = def_level(info.def_level),
      .max_repetition_level = rep_level(info.rep_level),
    };
}

schema_element indexed_leaf_node(
  indexed_info info,
  ss::sstring name,
  field_repetition_type rep_type,
  physical_type ptype,
  logical_type ltype = logical_type{}) {
    return {
      .position = info.index,
      .type = ptype,
      .repetition_type = rep_type,
      .name = std::move(name),
      .logical_type = ltype,
      .max_definition_level = def_level(info.def_level),
      .max_repetition_level = rep_level(info.rep_level),
    };
}
} // namespace

// NOLINTBEGIN(*magic-number*)
TEST(Schema, CanBeIndexed) {
    using field_repetition_type::optional;
    using field_repetition_type::repeated;
    using field_repetition_type::required;
    auto root = group_node(
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
            leaf_node("first_name", optional, byte_array_type(), string_type()),
            leaf_node("last_name", required, byte_array_type(), string_type())),
          leaf_node("id", optional, i32_type()),
          leaf_node("email", required, byte_array_type(), string_type()),
          group_node(
            "phones",
            repeated,
            group_node(
              "phones_tuple",
              repeated,
              leaf_node("number", optional, byte_array_type(), string_type()),
              leaf_node("type", optional, byte_array_type(), enum_type()))))));
    index_schema(root);
    auto expected = indexed_group_node(
      {.index = 0, .def_level = 0, .rep_level = 0},
      "schema",
      required,
      indexed_group_node(
        {.index = 1, .def_level = 1, .rep_level = 0},
        "persons",
        optional,
        indexed_group_node(
          {.index = 2, .def_level = 2, .rep_level = 1},
          "persons_tuple",
          repeated,
          indexed_group_node(
            {.index = 3, .def_level = 2, .rep_level = 1},
            "name",
            required,
            indexed_leaf_node(
              {.index = 4, .def_level = 3, .rep_level = 1},
              "first_name",
              optional,
              byte_array_type(),
              string_type()),
            indexed_leaf_node(
              {.index = 5, .def_level = 2, .rep_level = 1},
              "last_name",
              required,
              byte_array_type(),
              string_type())),
          indexed_leaf_node(
            {.index = 6, .def_level = 3, .rep_level = 1},
            "id",
            optional,
            i32_type()),
          indexed_leaf_node(
            {.index = 7, .def_level = 2, .rep_level = 1},
            "email",
            required,
            byte_array_type(),
            string_type()),
          indexed_group_node(
            {.index = 8, .def_level = 3, .rep_level = 2},
            "phones",
            repeated,
            indexed_group_node(
              {.index = 9, .def_level = 4, .rep_level = 3},
              "phones_tuple",
              repeated,
              indexed_leaf_node(
                {.index = 10, .def_level = 5, .rep_level = 3},
                "number",
                optional,
                byte_array_type(),
                string_type()),
              indexed_leaf_node(
                {.index = 11, .def_level = 5, .rep_level = 3},
                "type",
                optional,
                byte_array_type(),
                enum_type()))))));
    EXPECT_EQ(root, expected)
      << fmt::format("root={}\nexpected={}", root, expected);
}
// NOLINTEND(*magic-number*)
