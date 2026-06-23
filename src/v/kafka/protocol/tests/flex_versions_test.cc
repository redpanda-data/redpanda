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
#include "kafka/protocol/flex_versions.h"
#include "kafka/protocol/messages.h"
#include "kafka/protocol/types.h"

#include <gtest/gtest.h>

namespace {

TEST(FlexVersions, standard_keys_unchanged) {
    EXPECT_TRUE(
      kafka::flex_versions::is_api_in_schema(kafka::produce_api::key));
    // Negative and gap keys are not in schema.
    EXPECT_FALSE(kafka::flex_versions::is_api_in_schema(kafka::api_key(-1)));
    EXPECT_FALSE(
      kafka::flex_versions::is_api_in_schema(
        kafka::api_key(
          static_cast<int16_t>(kafka::standard_api_key_table_size))));
    EXPECT_FALSE(kafka::flex_versions::is_api_in_schema(kafka::api_key(5000)));
}

TEST(FlexVersions, reserved_key_in_schema_and_flexible) {
    // DescribeRedpandaRoles: flexibleVersions "0+", so v0 is flexible.
    EXPECT_TRUE(
      kafka::flex_versions::is_api_in_schema(
        kafka::describe_redpanda_roles_api::key));
    EXPECT_TRUE(
      kafka::flex_versions::is_flexible_request(
        kafka::describe_redpanda_roles_api::key, kafka::api_version(0)));
}

} // namespace
