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
#include "kafka/protocol/describe_redpanda_roles.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/types.h"
#include "kafka/server/server.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace {

TEST(ThroughputControlledApiKeys, named_standard_key_is_controlled) {
    auto bitmap = kafka::server::convert_api_names_to_key_bitmap(
      {ss::sstring(kafka::produce_api::name)});
    EXPECT_TRUE(bitmap.at(kafka::produce_api::key));
    EXPECT_FALSE(bitmap.at(kafka::fetch_api::key));
}

TEST(ThroughputControlledApiKeys, reserved_key_returns_false_not_throw) {
    auto bitmap = kafka::server::convert_api_names_to_key_bitmap({});
    // Reserved keys are valid indices that read as "not controlled" -- this is
    // what lets a reserved request survive the pre-dispatch throttle check.
    EXPECT_NO_THROW({ EXPECT_FALSE(bitmap.at(kafka::redpanda_api_key_base)); });
}

TEST(ThroughputControlledApiKeys, out_of_range_key_still_throws) {
    auto bitmap = kafka::server::convert_api_names_to_key_bitmap({});
    // Behavior preserved: a key in the gap throws std::out_of_range, exactly
    // like the previous std::vector<bool>::at(key).
    EXPECT_THROW((void)bitmap.at(kafka::api_key(100)), std::out_of_range);
}

TEST(ThroughputControlledApiKeys, reserved_key_can_be_opted_in) {
    // api_name_to_key is a pure name->key map, so naming a reserved API in the
    // throughput config opts it into throttling.
    auto bitmap = kafka::server::convert_api_names_to_key_bitmap(
      {ss::sstring(kafka::describe_redpanda_roles_api::name)});
    EXPECT_TRUE(bitmap.at(kafka::redpanda_api_key_base));
}

} // namespace
