/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/values.h"

/// Functions to convert date/time strings to Iceberg values. Useful for
/// parsing date/time values from JSON or other text formats that use the
/// RFC 3339 format. I.e. when "format" keyword is used in JSON Schema
/// definitions.
///
/// See:
/// https://datatracker.ietf.org/doc/html/draft-handrews-json-schema-validation-01#section-7.3.1
/// See: https://datatracker.ietf.org/doc/html/rfc3339#section-5.6
namespace iceberg::conversion::time_rfc3339 {

basic_value_outcome<iceberg::timestamptz_value>
date_time_str_to_timestampz(const ss::sstring&);

basic_value_outcome<iceberg::date_value> date_str_to_date(const ss::sstring&);

basic_value_outcome<iceberg::time_value> time_str_to_time(const ss::sstring&);

} // namespace iceberg::conversion::time_rfc3339
