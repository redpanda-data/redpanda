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

#pragma once

#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "utils/uuid.h"

#include <absl/numeric/int128.h>

#include <cstdint>
#include <variant>

// A subset of parquet values that is needed for iceberg.

namespace serde::parquet {

struct null_value {};
struct boolean_value {
    bool val;
};
struct int32_value {
    int32_t val;
};
struct int64_value {
    int64_t val;
};
struct float32_value {
    float val;
};
struct float64_value {
    double val;
};
struct decimal_value {
    absl::int128 val;
};
// DATE is used to for a logical date type, without a time of day. It must
// annotate an int32 that stores the number of days from the Unix epoch, 1
// January 1970.
struct date_value {
    int32_t val;
};
// TIME is used for a logical time type without a date with millisecond or
// microsecond precision. The type has two type parameters: UTC adjustment (true
// or false) and unit (MILLIS or MICROS, NANOS).
struct time_value {
    int64_t val;
};
// In data annotated with the TIMESTAMP logical type, each value is a single
// int64 number that can be decoded into year, month, day, hour, minute, second
// and subsecond fields using calculations detailed below. Please note that a
// value defined this way does not necessarily correspond to a single instant on
// the time-line and such interpretations are allowed on purpose.
struct timestamp_value {
    int64_t val;
};
struct string_value {
    iobuf val;
};
struct uuid_value {
    uuid_t val;
};
struct byte_array_value {
    iobuf val;
};
struct fixed_byte_array_value {
    iobuf val;
};

struct list_element;
struct map_entry;

// Parquet proper actually supports a lot more types than this, but we only need
// a few of them for iceberg.
using value = std::variant<
  null_value,
  boolean_value,
  int32_value,
  int64_value,
  float32_value,
  float64_value,
  decimal_value,
  date_value,
  time_value,
  timestamp_value,
  string_value,
  uuid_value,
  byte_array_value,
  fixed_byte_array_value,
  chunked_vector<list_element>,
  chunked_vector<map_entry>>;

struct list_element {
    value element;
};

struct map_entry {
    // Only non-primative values are supported here
    // no lists and maps are not valid.
    value key;
    std::optional<value> val;
};

} // namespace serde::parquet
