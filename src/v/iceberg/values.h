// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "utils/uuid.h"

#include <absl/numeric/int128.h>

#include <optional>
#include <variant>

namespace iceberg {

struct boolean_value {
    bool val;
};

struct int_value {
    int32_t val;
};

struct long_value {
    int64_t val;
};

struct float_value {
    float val;
};

struct double_value {
    double val;
};

struct date_value {
    // Days since 1970-01-01.
    int32_t val;
};

struct time_value {
    // Microseconds since midnight.
    int64_t val;
};

struct timestamp_value {
    // Microseconds since 1970-01-01 00:00:00.
    int64_t val;
};

struct timestamptz_value {
    // Microseconds since 1970-01-01 00:00:00 UTC.
    int64_t val;
};

struct string_value {
    iobuf val;
};

struct uuid_value {
    uuid_t val;
};

struct fixed_value {
    iobuf val;
};

struct binary_value {
    iobuf val;
};

struct decimal_value {
    absl::int128 val;
};

using primitive_value = std::variant<
  boolean_value,
  int_value,
  long_value,
  float_value,
  double_value,
  date_value,
  time_value,
  timestamp_value,
  timestamptz_value,
  string_value,
  uuid_value,
  fixed_value,
  binary_value,
  decimal_value>;
bool operator==(const primitive_value&, const primitive_value&);
bool operator<(const primitive_value&, const primitive_value&);
primitive_value make_copy(const primitive_value&);

struct struct_value;
struct list_value;
struct map_value;
using value = std::variant<
  primitive_value,
  std::unique_ptr<struct_value>,
  std::unique_ptr<list_value>,
  std::unique_ptr<map_value>>;

struct struct_value {
    // The order of these fields must align with the corresponding struct type
    // as defined in the schema, see `iceberg::struct_type`.
    chunked_vector<std::optional<value>> fields;
};
bool operator==(const struct_value&, const struct_value&);
bool operator==(
  const std::unique_ptr<struct_value>&, const std::unique_ptr<struct_value>&);

struct list_value {
    chunked_vector<std::optional<value>> elements;
};
bool operator==(const list_value&, const list_value&);
bool operator==(
  const std::unique_ptr<struct_value>&, const std::unique_ptr<struct_value>&);

struct kv_value {
    // Shouldn't be null, according to the Iceberg spec.
    value key;

    // May be null if the value is null.
    std::optional<value> val;
};
bool operator==(const kv_value&, const kv_value&);

struct map_value {
    chunked_vector<kv_value> kvs;
};
bool operator==(const map_value&, const map_value&);
bool operator==(
  const std::unique_ptr<map_value>&, const std::unique_ptr<map_value>&);
bool operator==(const value&, const value&);

std::ostream& operator<<(std::ostream&, const boolean_value&);
std::ostream& operator<<(std::ostream&, const int_value&);
std::ostream& operator<<(std::ostream&, const long_value&);
std::ostream& operator<<(std::ostream&, const float_value&);
std::ostream& operator<<(std::ostream&, const double_value&);
std::ostream& operator<<(std::ostream&, const date_value&);
std::ostream& operator<<(std::ostream&, const time_value&);
std::ostream& operator<<(std::ostream&, const timestamp_value&);
std::ostream& operator<<(std::ostream&, const timestamptz_value&);
std::ostream& operator<<(std::ostream&, const string_value&);
std::ostream& operator<<(std::ostream&, const uuid_value&);
std::ostream& operator<<(std::ostream&, const fixed_value&);
std::ostream& operator<<(std::ostream&, const binary_value&);
std::ostream& operator<<(std::ostream&, const decimal_value&);
std::ostream& operator<<(std::ostream&, const primitive_value&);
std::ostream& operator<<(std::ostream&, const struct_value&);
std::ostream& operator<<(std::ostream&, const list_value&);
std::ostream& operator<<(std::ostream&, const map_value&);
std::ostream& operator<<(std::ostream&, const std::unique_ptr<struct_value>&);
std::ostream& operator<<(std::ostream&, const std::unique_ptr<list_value>&);
std::ostream& operator<<(std::ostream&, const std::unique_ptr<map_value>&);
std::ostream& operator<<(std::ostream&, const value&);

size_t value_hash(const struct_value&);
size_t value_hash(const value&);

} // namespace iceberg

namespace std {

template<>
struct hash<iceberg::struct_value> {
    size_t operator()(const iceberg::struct_value& v) const {
        return iceberg::value_hash(v);
    }
};

template<>
struct hash<iceberg::value> {
    size_t operator()(const iceberg::value& v) const {
        return iceberg::value_hash(v);
    }
};

} // namespace std
