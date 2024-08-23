// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/transform.h"
#include "iceberg/transform_utils.h"
#include "iceberg/values.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

#include <variant>

using namespace iceberg;

value make_timestamp_val(model::timestamp ts) {
    static constexpr auto micros_per_millis = 1000;
    auto ts_micros = ts.value() * micros_per_millis;
    return timestamp_value{ts_micros};
}

TEST(TestTransforms, TestHourlyTransform) {
    const auto start_time = model::timestamp::now();
    auto start_transformed = apply_transform(
      make_timestamp_val(start_time), hour_transform{});
    static constexpr auto millis_per_hr = 60 * 60 * 1000;
    ASSERT_TRUE(std::holds_alternative<primitive_value>(start_transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(start_transformed)));
    auto start_val = std::get<int_value>(
      std::get<primitive_value>(start_transformed));

    auto plus_1hr = model::timestamp(start_time.value() + millis_per_hr);
    auto plus_1hr_transformed = apply_transform(
      make_timestamp_val(plus_1hr), hour_transform{});
    ASSERT_NE(start_transformed, plus_1hr_transformed);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(plus_1hr_transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(plus_1hr_transformed)));
    auto plus_1hr_val = std::get<int_value>(
      std::get<primitive_value>(plus_1hr_transformed));
    ASSERT_EQ(start_val.val + 1, plus_1hr_val.val);

    auto minus_1hr = model::timestamp(start_time.value() - millis_per_hr);
    auto minus_1hr_transformed = apply_transform(
      make_timestamp_val(minus_1hr), hour_transform{});
    ASSERT_NE(start_transformed, minus_1hr_transformed);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(minus_1hr_transformed));
    ASSERT_TRUE(std::holds_alternative<int_value>(
      std::get<primitive_value>(minus_1hr_transformed)));
    auto minus_1hr_val = std::get<int_value>(
      std::get<primitive_value>(minus_1hr_transformed));
    ASSERT_EQ(start_val.val - 1, minus_1hr_val.val);
}
