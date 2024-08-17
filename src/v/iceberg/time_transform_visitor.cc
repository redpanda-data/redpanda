// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/time_transform_visitor.h"

#include "iceberg/values.h"

namespace iceberg {

namespace {
int32_t micros_to_hr(int64_t micros) {
    static constexpr int64_t s_per_hr = 3600;
    static constexpr int64_t micros_per_s = 1000000;
    static constexpr int64_t micros_per_hr = micros_per_s * s_per_hr;
    return static_cast<int32_t>(micros / micros_per_hr);
}
} // namespace

int32_t hour_transform_visitor::operator()(const primitive_value& v) {
    if (std::holds_alternative<time_value>(v)) {
        return micros_to_hr(std::get<time_value>(v).val);
    }
    if (std::holds_alternative<timestamp_value>(v)) {
        return micros_to_hr(std::get<timestamp_value>(v).val);
    }
    if (std::holds_alternative<timestamptz_value>(v)) {
        return micros_to_hr(std::get<timestamptz_value>(v).val);
    }
    throw std::invalid_argument(
      fmt::format("hourly_visitor not implemented for primitive value {}", v));
}

} // namespace iceberg
