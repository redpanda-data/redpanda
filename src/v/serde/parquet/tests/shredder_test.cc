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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "serde/parquet/shredder.h"

#include <gtest/gtest.h>

#include <variant>

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

template<typename... Args>
struct_value record(Args... field) {
    chunked_vector<struct_field> fields;
    (fields.push_back(struct_field{std::move(field)}), ...);
    return fields;
}

template<typename... Args>
repeated_value list(Args... field) {
    chunked_vector<repeated_element> fields;
    (fields.push_back(repeated_element{std::move(field)}), ...);
    return fields;
}

struct value_collector {
    explicit value_collector(const schema_element& root) {
        int32_t col_index = 0;
        root.for_each([this, &col_index](const schema_element& node) {
            int32_t my_index = col_index++;
            if (!node.is_leaf()) {
                // Skip inner nodes.
                return;
            }
            column_mapping[my_index] = columns.size();
            columns.emplace_back();
        });
    }

    void operator()(shredded_value val) const {
        auto it = column_mapping.find(val.schema_element_index);
        ASSERT_NE(it, column_mapping.end()) << "invalid column index";
        columns[it->second].push_back(std::move(val));
    }

    std::unordered_map<int32_t, size_t> column_mapping;
    mutable chunked_vector<chunked_vector<shredded_value>> columns;
};

MATCHER_P3(IsVal, val, r, d, "") {
    return fmt::format("{}", arg.val) == val && r == arg.rep_level
           && d == arg.def_level;
}

} // namespace

// NOLINTNEXTLINE(*internal-linkage*)
void PrintTo(
  const chunked_vector<chunked_vector<shredded_value>>& columns,
  std::ostream* os) {
    *os << "[\n";
    for (const auto& column : columns) {
        *os << "  [\n";
        for (const auto& val : column) {
            *os << fmt::format(
              "    {{val: {}, r: {}, d: {}}}\n",
              val.val,
              val.rep_level,
              val.def_level);
        }
        *os << "  ]\n";
    }
    *os << "]\n";
}

using testing::ElementsAre;

// NOLINTBEGIN(*magic-number*)
TEST(RecordShredding, ExampleFromDremelPaper) {
    schema_element document_schema = group_node(
      "Document",
      field_repetition_type::required,
      leaf_node("DocId", field_repetition_type::required, i64_type{}),
      group_node(
        "Links",
        field_repetition_type::optional,
        leaf_node("Forward", field_repetition_type::repeated, i64_type{}),
        leaf_node("Backward", field_repetition_type::repeated, i64_type{})),
      group_node(
        "Name",
        field_repetition_type::repeated,
        group_node(
          "Language",
          field_repetition_type::repeated,
          leaf_node(
            "Code",
            field_repetition_type::required,
            byte_array_type{},
            string_type{}),
          leaf_node(
            "Country",
            field_repetition_type::optional,
            byte_array_type{},
            string_type{})),
        leaf_node(
          "Url",
          field_repetition_type::optional,
          byte_array_type{},
          string_type{})));
    struct_value r1 = record(
      /*DocId*/ int64_value(10),
      /*Links*/
      record(
        /*Forward*/ list(int64_value(20), int64_value(40), int64_value(60)),
        /*Backward*/ list()),
      list(
        /*Name*/
        record(
          list(
            /*Language*/
            record(
              /*Code*/
              string_value(iobuf::from("en-us")),
              /*Country*/
              string_value(iobuf::from("us"))),
            /*Language*/
            record(string_value(iobuf::from("en")), null_value())),
          /*Url*/
          string_value(iobuf::from("http://A"))),
        /*Name*/
        record(
          list(),
          /*Url*/
          string_value(iobuf::from("http://B"))),
        /*Name*/
        record(
          list(
            /*Language*/
            record(
              /*Code*/
              string_value(iobuf::from("en-gb")),
              /*Country*/
              string_value(iobuf::from("gb")))),
          /*Url*/
          null_value())));
    struct_value r2 = record(
      /*DocId*/ int64_value(20),
      /*Links*/
      record(
        /*Forward*/ list(int64_value(80)),
        /*Backward*/ list(int64_value(10), int64_value(30))),
      /*Name*/
      list(record(
        /*Language*/
        repeated_value(),
        /*Url*/
        string_value(iobuf::from("http://C")))));
    value_collector collector(document_schema);
    auto indexed_schema = index_schema(document_schema);
    shred_record(indexed_schema, std::move(r1), collector);
    shred_record(indexed_schema, std::move(r2), collector);
    EXPECT_THAT(
      collector.columns,
      ElementsAre(
        // DocId
        ElementsAre(IsVal("10", 0, 0), IsVal("20", 0, 0)),
        // Links.Forward
        ElementsAre(
          IsVal("20", 0, 2),
          IsVal("40", 1, 2),
          IsVal("60", 1, 2),
          IsVal("80", 0, 2)),
        // Links.Backward
        ElementsAre(IsVal("NULL", 0, 1), IsVal("10", 0, 2), IsVal("30", 1, 2)),
        // Name.Language.Code
        ElementsAre(
          IsVal("en-us", 0, 2),
          IsVal("en", 2, 2),
          IsVal("NULL", 1, 1),
          IsVal("en-gb", 1, 2),
          IsVal("NULL", 0, 1)),
        // Name.Language.Country
        ElementsAre(
          IsVal("us", 0, 3),
          IsVal("NULL", 2, 2),
          IsVal("NULL", 1, 1),
          IsVal("gb", 1, 3),
          IsVal("NULL", 0, 1)),
        // Name.Url
        ElementsAre(
          IsVal("http://A", 0, 2),
          IsVal("http://B", 1, 2),
          IsVal("NULL", 1, 1),
          IsVal("http://C", 0, 2))));
}
// NOLINTEND(*magic-number*)
