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

#include "serde/parquet/value.h"

#include <gtest/gtest.h>

#include <variant>

using namespace serde::parquet;

TEST(Value, Lists) {
    chunked_vector<list_element> list;
    // It's not actually valid to have heterogenuous lists in parquet, but
    // our current encoding of the data structures isn't that strict.
    list.emplace_back(int32_value(2));
    list.emplace_back(boolean_value(true));
    value v = std::move(list);
    EXPECT_TRUE(std::holds_alternative<chunked_vector<list_element>>(v));
}

TEST(Value, Maps) {
    chunked_vector<map_entry> map;
    map.emplace_back(int32_value(2), std::make_optional(boolean_value(true)));
    value v = std::move(map);
    EXPECT_TRUE(std::holds_alternative<chunked_vector<map_entry>>(v));
}
