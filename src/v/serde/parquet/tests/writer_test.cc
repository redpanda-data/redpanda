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

#include "bytes/iostream.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <gtest/gtest.h>

namespace serde::parquet {
namespace {

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

schema_element simple_schema() {
    chunked_vector<schema_element> children;
    children.push_back(
      leaf_node("data", field_repetition_type::required, byte_array_type{}));
    return {
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
      .children = std::move(children),
    };
}

group_value make_row(size_t data_size) {
    chunked_vector<group_member> fields;
    fields.push_back(
      group_member{byte_array_value{iobuf::from(std::string(data_size, 'x'))}});
    return fields;
}

} // namespace

// NOLINTBEGIN(*magic-number*)

TEST(ParquetWriter, FlushesRowGroupWhenSizeExceeded) {
    constexpr size_t row_group_size = 1_KiB;
    constexpr size_t row_size = 256;
    constexpr size_t rows_to_write = 5;

    iobuf file;
    writer w(
      {
        .schema = simple_schema(),
        .row_group_size = row_group_size,
      },
      make_iobuf_ref_output_stream(file));
    w.init().get();

    file_stats stats;
    for (size_t i = 0; i < rows_to_write; ++i) {
        stats = w.write_row(make_row(row_size)).get();
    }
    EXPECT_GE(stats.flushed_size, row_group_size);
    EXPECT_LT(stats.buffered_size, row_group_size);

    w.close().get();
}

TEST(ParquetWriter, NoEarlyFlushWhenUnderLimit) {
    constexpr size_t row_group_size = 64_MiB;
    constexpr size_t row_size = 256;
    constexpr size_t rows_to_write = 5;

    iobuf file;
    writer w(
      {
        .schema = simple_schema(),
        .row_group_size = row_group_size,
      },
      make_iobuf_ref_output_stream(file));
    w.init().get();

    file_stats stats;
    for (size_t i = 0; i < rows_to_write; ++i) {
        stats = w.write_row(make_row(row_size)).get();
    }

    // flushed_size should only be the magic bytes (PAR1 = 4 bytes)
    EXPECT_EQ(stats.flushed_size, 4);
    EXPECT_GE(stats.buffered_size, row_size * rows_to_write);

    w.close().get();
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
