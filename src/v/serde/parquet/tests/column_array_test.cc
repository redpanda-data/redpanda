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

#include "serde/parquet/column_array.h"

#include <gtest/gtest.h>

using namespace serde::parquet;

TEST(ColumnArray, I32Construction) {
    column_array arr;
    arr.ptype = i32_type();
    arr.length = 3;
    auto& d = arr.data.emplace<column_array::i32_data>();
    d.values.push_back(10);
    d.values.push_back(20);
    d.values.push_back(30);
    EXPECT_EQ(arr.length, 3);
    auto* i32d = std::get_if<column_array::i32_data>(&arr.data);
    ASSERT_NE(i32d, nullptr);
    EXPECT_EQ(i32d->values.size(), 3);
    EXPECT_EQ(i32d->values[0], 10);
}

TEST(ColumnarBatch, Construction) {
    columnar_batch batch;
    batch.num_rows = 100;

    column_array col;
    col.ptype = i64_type();
    col.length = 100;
    col.data.emplace<column_array::i64_data>();
    batch.columns.push_back(std::move(col));

    columnar_batch::level_data ld;
    batch.levels.push_back(std::move(ld));

    EXPECT_EQ(batch.num_rows, 100);
    EXPECT_EQ(batch.columns.size(), 1);
    EXPECT_EQ(batch.levels.size(), 1);
}
