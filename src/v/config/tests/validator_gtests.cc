// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/tests/constraint_utils.h"
#include "config/validators.h"
#include "test_utils/test.h"

TEST(ConfigConstraintsTest, InvaldConstraintConfig) {
    auto valid_constraints = make_constraints(
      config::constraint_type::restrikt);
    auto invalid_integral_constraints = std::
      unordered_map<config::constraint_t::key_type, config::constraint_t>{
        {"invalid-integral",
         config::constraint_t{
           .name = "invalid-integral",
           .type = config::constraint_type::restrikt,
           .flags = config::range_values<int64_t>(LIMIT_MAX, LIMIT_MIN)}}};

    EXPECT_FALSE(config::validate_constraints(valid_constraints).has_value());
    EXPECT_TRUE(
      config::validate_constraints(invalid_integral_constraints).has_value());
}
