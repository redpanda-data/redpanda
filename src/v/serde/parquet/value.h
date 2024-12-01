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

#include <cstdint>
#include <unistd.h>

// A subset of parquet values that is needed for iceberg.

namespace serde::parquet {

struct null_value {
    bool operator==(const null_value&) const = default;
};
struct boolean_value {
    bool val;

    bool operator==(const boolean_value&) const = default;
};
struct int32_value {
    int32_t val;

    bool operator==(const int32_value&) const = default;
};
struct int64_value {
    int64_t val;

    bool operator==(const int64_value&) const = default;
};
struct float32_value {
    float val;

    bool operator==(const float32_value&) const = default;
};
struct float64_value {
    double val;

    bool operator==(const float64_value&) const = default;
};
struct byte_array_value {
    iobuf val;

    bool operator==(const byte_array_value&) const = default;
};
struct fixed_byte_array_value {
    iobuf val;

    bool operator==(const fixed_byte_array_value&) const = default;
};

struct repeated_element;
struct group_member;

// The group member ordering here matters - it must be the same as specified in
// the schema.
//
// This maps to any "group node" (protobuf v2 naming) or non-leaf element in a
// schema.
using group_value = chunked_vector<group_member>;

// Repeated values are what should wrap any repeated value in the schema,
// it does *not* directly encode a logical list type value (this is the same as
// protocol buffer semantics). See the parquet spec on logical types for more
// information about how translate logical lists into a parquet schema.
using repeated_value = chunked_vector<repeated_element>;

// We only support physical types for parquet. Logical types are encoded in the
// schema and should be translated during value conversion.
using value = std::variant<
  null_value,
  boolean_value,
  int32_value,
  int64_value,
  float32_value,
  float64_value,
  byte_array_value,
  fixed_byte_array_value,
  group_value,
  repeated_value>;

struct group_member {
    value field;

    bool operator==(const group_member&) const = default;
};

struct repeated_element {
    value element;

    bool operator==(const repeated_element&) const = default;
};

value copy(const value&);

} // namespace serde::parquet

template<>
struct fmt::formatter<serde::parquet::value>
  : fmt::formatter<std::string_view> {
    // This does not truncate the output, so it could print a very large value
    auto format(const serde::parquet::value&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<serde::parquet::group_member>
  : fmt::formatter<std::string_view> {
    auto format(const serde::parquet::group_member&, fmt::format_context& ctx)
      const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<serde::parquet::repeated_element>
  : fmt::formatter<std::string_view> {
    auto format(
      const serde::parquet::repeated_element&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
