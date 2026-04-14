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

#include "serde/parquet/assembler.h"
#include "serde/parquet/column_array.h"
#include "serde/parquet/encoding.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/shredder.h"
#include "serde/parquet/value.h"

#include <seastar/util/variant_utils.hh>

#include <gtest/gtest.h>

#include <climits>

namespace serde::parquet {

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

template<typename... Args>
group_value record(Args... field) {
    chunked_vector<group_member> fields;
    (fields.push_back(group_member{std::move(field)}), ...);
    return fields;
}

template<typename... Args>
repeated_value list(Args... field) {
    chunked_vector<repeated_element> fields;
    (fields.push_back(repeated_element{std::move(field)}), ...);
    return fields;
}

/// Shred records into a columnar_batch suitable for the assembler.
/// Consumes the schema (moves it into the batch).
columnar_batch
shred_to_batch(schema_element& schema, chunked_vector<group_value>& rows) {
    index_schema(schema);

    // Collect per-leaf column data
    struct leaf_data {
        chunked_vector<value> values;
        chunked_vector<def_level> def_levels;
        chunked_vector<rep_level> rep_levels;
    };

    // Map from schema position to leaf index
    chunked_vector<int32_t> pos_to_leaf;
    chunked_vector<leaf_data> leaves;
    int32_t max_pos = 0;
    schema.for_each([&](const schema_element& elem) {
        max_pos = std::max(max_pos, elem.position);
    });
    for (int32_t i = 0; i <= max_pos; ++i) {
        pos_to_leaf.push_back(-1);
    }
    schema.for_each([&](const schema_element& elem) {
        if (!elem.is_leaf()) {
            return;
        }
        pos_to_leaf[elem.position] = static_cast<int32_t>(leaves.size());
        leaves.emplace_back();
    });

    for (auto& row : rows) {
        auto row_copy = std::get<group_value>(copy(value(std::move(row))));
        shred_record(schema, std::move(row_copy), [&](shredded_value sv) {
            auto leaf_idx = pos_to_leaf[sv.schema_element_position];
            auto& ld = leaves[leaf_idx];
            ld.values.push_back(std::move(sv.val));
            ld.def_levels.push_back(sv.def_level);
            ld.rep_levels.push_back(sv.rep_level);
            return ss::make_ready_future();
        }).get();
    }

    // Build the columnar_batch from the shredded data
    columnar_batch batch;
    batch.num_rows = static_cast<int64_t>(rows.size());

    int32_t leaf_idx = 0;
    schema.for_each([&](const schema_element& elem) {
        if (!elem.is_leaf()) {
            return;
        }
        auto& ld = leaves[leaf_idx];

        column_array arr;
        arr.ptype = elem.type;
        arr.ltype = elem.logical_type;
        arr.length = static_cast<int64_t>(ld.values.size());

        // Build the data based on physical type, collecting non-null values
        ss::visit(
          elem.type,
          [&](const bool_type&) {
              auto& d = arr.data.emplace<column_array::boolean_data>();
              uint8_t current_byte = 0;
              for (auto& v : ld.values) {
                  if (auto* bv = std::get_if<boolean_value>(&v)) {
                      current_byte |= static_cast<uint8_t>(bv->val)
                                      << (d.num_values % CHAR_BIT);
                      ++d.num_values;
                      if (d.num_values % CHAR_BIT == 0) {
                          d.packed_bits.append(&current_byte, 1);
                          current_byte = 0;
                      }
                  }
              }
              if (d.num_values % CHAR_BIT != 0) {
                  d.packed_bits.append(&current_byte, 1);
              }
          },
          [&](const i32_type&) {
              auto& d = arr.data.emplace<column_array::i32_data>();
              for (auto& v : ld.values) {
                  if (auto* iv = std::get_if<int32_value>(&v)) {
                      d.values.push_back(iv->val);
                  }
              }
          },
          [&](const i64_type&) {
              auto& d = arr.data.emplace<column_array::i64_data>();
              for (auto& v : ld.values) {
                  if (auto* iv = std::get_if<int64_value>(&v)) {
                      d.values.push_back(iv->val);
                  }
              }
          },
          [&](const f32_type&) {
              auto& d = arr.data.emplace<column_array::f32_data>();
              for (auto& v : ld.values) {
                  if (auto* fv = std::get_if<float32_value>(&v)) {
                      d.values.push_back(fv->val);
                  }
              }
          },
          [&](const f64_type&) {
              auto& d = arr.data.emplace<column_array::f64_data>();
              for (auto& v : ld.values) {
                  if (auto* fv = std::get_if<float64_value>(&v)) {
                      d.values.push_back(fv->val);
                  }
              }
          },
          [&](const byte_array_type& t) {
              if (t.fixed_length.has_value()) {
                  auto& d
                    = arr.data.emplace<column_array::fixed_byte_array_data>();
                  d.fixed_length = *t.fixed_length;
                  for (auto& v : ld.values) {
                      if (auto* bav = std::get_if<fixed_byte_array_value>(&v)) {
                          d.data.append(bav->val.copy());
                      }
                  }
              } else {
                  auto& d = arr.data.emplace<column_array::byte_array_data>();
                  d.offsets.push_back(0);
                  int64_t offset = 0;
                  for (auto& v : ld.values) {
                      if (auto* bav = std::get_if<byte_array_value>(&v)) {
                          auto len = static_cast<int64_t>(
                            bav->val.size_bytes());
                          d.data.append(bav->val.copy());
                          offset += len;
                          d.offsets.push_back(offset);
                      }
                  }
              }
          },
          [&](const std::monostate&) {
              arr.data.emplace<column_array::boolean_data>();
          });

        batch.columns.push_back(std::move(arr));
        batch.levels.push_back(
          columnar_batch::level_data{
            .def_levels = std::move(ld.def_levels),
            .rep_levels = std::move(ld.rep_levels),
          });
        ++leaf_idx;
    });

    return batch;
}

/// Shred-then-assemble consumes rows (via move), so we need to build
/// the expected values upfront. Since group_value uses chunked_vector
/// (not copyable), we compare assembled results against freshly
/// constructed expected values in each test.

} // namespace

// NOLINTBEGIN(*magic-number*)

TEST(Assembler, FlatRequired) {
    using field_repetition_type::required;
    auto schema = group_node(
      "schema",
      required,
      leaf_node("a", required, i32_type()),
      leaf_node("b", required, i64_type()));

    chunked_vector<group_value> rows;
    rows.push_back(record(int32_value{1}, int64_value{10}));
    rows.push_back(record(int32_value{2}, int64_value{20}));
    rows.push_back(record(int32_value{3}, int64_value{30}));

    auto batch = shred_to_batch(schema, rows);
    auto result = assemble_records(schema, batch);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], record(int32_value{1}, int64_value{10}));
    EXPECT_EQ(result[1], record(int32_value{2}, int64_value{20}));
    EXPECT_EQ(result[2], record(int32_value{3}, int64_value{30}));
}

TEST(Assembler, FlatOptional) {
    using field_repetition_type::optional;
    using field_repetition_type::required;
    auto schema = group_node(
      "schema",
      required,
      leaf_node("a", optional, i32_type()),
      leaf_node("b", optional, i64_type()));

    chunked_vector<group_value> rows;
    rows.push_back(record(int32_value{1}, null_value{}));
    rows.push_back(record(null_value{}, int64_value{20}));
    rows.push_back(record(int32_value{3}, int64_value{30}));

    auto batch = shred_to_batch(schema, rows);
    auto result = assemble_records(schema, batch);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], record(int32_value{1}, null_value{}));
    EXPECT_EQ(result[1], record(null_value{}, int64_value{20}));
    EXPECT_EQ(result[2], record(int32_value{3}, int64_value{30}));
}

TEST(Assembler, NestedOptionalGroup) {
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
      leaf_node("z", required, i64_type()));

    chunked_vector<group_value> rows;
    rows.push_back(
      record(value(record(int32_value{1}, int32_value{2})), int64_value{100}));
    rows.push_back(record(null_value{}, int64_value{200}));
    rows.push_back(
      record(value(record(int32_value{5}, int32_value{6})), int64_value{300}));

    auto batch = shred_to_batch(schema, rows);
    auto result = assemble_records(schema, batch);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(
      result[0],
      record(value(record(int32_value{1}, int32_value{2})), int64_value{100}));
    EXPECT_EQ(result[1], record(null_value{}, int64_value{200}));
    EXPECT_EQ(
      result[2],
      record(value(record(int32_value{5}, int32_value{6})), int64_value{300}));
}

TEST(Assembler, RepeatedLeaf) {
    using field_repetition_type::repeated;
    using field_repetition_type::required;
    auto schema = group_node(
      "schema", required, leaf_node("vals", repeated, i32_type()));

    chunked_vector<group_value> rows;
    rows.push_back(record(value(list(
      value(int32_value{1}), value(int32_value{2}), value(int32_value{3})))));
    rows.push_back(
      record(value(list(value(int32_value{10}), value(int32_value{20})))));

    auto batch = shred_to_batch(schema, rows);
    auto result = assemble_records(schema, batch);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(
      result[0],
      record(value(list(
        value(int32_value{1}), value(int32_value{2}), value(int32_value{3})))));
    EXPECT_EQ(
      result[1],
      record(value(list(value(int32_value{10}), value(int32_value{20})))));
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
