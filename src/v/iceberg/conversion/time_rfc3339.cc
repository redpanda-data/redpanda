/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/time_rfc3339.h"

#include "absl/time/time.h"

namespace iceberg::conversion::time_rfc3339 {

basic_value_outcome<iceberg::timestamptz_value>
date_time_str_to_timestampz(const ss::sstring& input) {
    absl::Time time;
    std::string err;

    if (!absl::ParseTime(absl::RFC3339_full, input, &time, &err)) {
        return value_conversion_exception(
          fmt::format("Failed to parse date-time value '{}': {}", input, err));
    }
    return timestamptz_value{
      absl::ToInt64Microseconds(time - absl::UnixEpoch())};
}

basic_value_outcome<iceberg::date_value>
date_str_to_date(const ss::sstring& input) {
    absl::CivilDay day;

    if (!absl::ParseCivilTime(input, &day)) {
        return value_conversion_exception(
          fmt::format("Failed to parse date value '{}'", input));
    }
    return date_value{static_cast<int32_t>(day - absl::CivilDay{})};
}

basic_value_outcome<iceberg::time_value>
time_str_to_time(const ss::sstring& input) {
    absl::Time res;
    std::string err;

    if (!absl::ParseTime("%H:%M:%E*S%Ez", input, &res, &err)) {
        return value_conversion_exception(
          fmt::format("Failed to parse date-time value '{}': {}", input, err));
    }

    return time_value{absl::ToInt64Microseconds(res - absl::UnixEpoch())};
}

} // namespace iceberg::conversion::time_rfc3339
