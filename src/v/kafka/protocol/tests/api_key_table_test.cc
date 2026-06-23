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
#include "kafka/protocol/api_key_indexed_array.h"
#include "kafka/protocol/messages.h"
#include "kafka/protocol/types.h"

#include <gtest/gtest.h>

namespace {

// One reserved API (DescribeRedpandaRoles at 15000) is registered today.
static_assert(kafka::reserved_api_key_table_size == 1);
static_assert(
  kafka::api_key_table<int>::reserved_base() == kafka::redpanda_api_key_base);
static_assert(
  kafka::api_key_table<int>::standard_size()
  == kafka::standard_api_key_table_size);
static_assert(kafka::api_key_table<int>::reserved_size() == 1);

TEST(ApiKeyTable, alias_dimensions_match_type_lists) {
    // The static_asserts above carry the assertions; this confirms the
    // translation unit links and the alias is usable at run time.
    kafka::api_key_table<int> table;
    EXPECT_TRUE(table.contains(kafka::redpanda_api_key_base));
    EXPECT_FALSE(table.contains(
      kafka::api_key(
        static_cast<int16_t>(kafka::standard_api_key_table_size))));
}

} // namespace
