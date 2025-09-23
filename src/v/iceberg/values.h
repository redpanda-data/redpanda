/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "absl/numeric/int128.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "iceberg/datatypes.h"
#include "utils/uuid.h"

#include <optional>
#include <variant>

namespace iceberg {

struct boolean_value {
    static std::string_view name() { return "boolean"; }
    bool val;
};

struct int_value {
    static std::string_view name() { return "int"; }
    int32_t val;
};

struct long_value {
    static std::string_view name() { return "long"; }
    int64_t val;
};

struct float_value {
    static std::string_view name() { return "float"; }
    float val;
};

struct double_value {
    static std::string_view name() { return "double"; }
    double val;
};

struct date_value {
    static std::string_view name() { return "date"; }
    // Days since 1970-01-01.
    int32_t val;
};

struct time_value {
    static std::string_view name() { return "time"; }
    // Microseconds since midnight.
    int64_t val;
};

struct timestamp_value {
    static std::string_view name() { return "timestamp"; }
    // Microseconds since 1970-01-01 00:00:00.
    int64_t val;
};

struct timestamptz_value {
    static std::string_view name() { return "timestamptz"; }
    // Microseconds since 1970-01-01 00:00:00 UTC.
    int64_t val;
};

struct string_value {
    static std::string_view name() { return "string"; }
    iobuf val;
};

struct uuid_value {
    static std::string_view name() { return "uuid"; }
    uuid_t val;
};

struct fixed_value {
    static std::string_view name() { return "fixed"; }
    iobuf val;
};

struct binary_value {
    static std::string_view name() { return "binary"; }
    iobuf val;
};

struct decimal_value {
    static std::string_view name() { return "decimal"; }
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

value make_copy(const value&);

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

// Provides the mapping between the c++ type of a primitive iceberg value
// variant and the c++ type of the corresponding primitive iceberg type variant.
template<typename TVal>
struct primitive_value_type {};
template<>
struct primitive_value_type<boolean_value> {
    using type = boolean_type;
};
template<>
struct primitive_value_type<int_value> {
    using type = int_type;
};
template<>
struct primitive_value_type<long_value> {
    using type = long_type;
};
template<>
struct primitive_value_type<float_value> {
    using type = float_type;
};
template<>
struct primitive_value_type<double_value> {
    using type = double_type;
};
template<>
struct primitive_value_type<decimal_value> {
    using type = decimal_type;
};
template<>
struct primitive_value_type<date_value> {
    using type = date_type;
};
template<>
struct primitive_value_type<time_value> {
    using type = time_type;
};
template<>
struct primitive_value_type<timestamp_value> {
    using type = timestamp_type;
};
template<>
struct primitive_value_type<timestamptz_value> {
    using type = timestamptz_type;
};
template<>
struct primitive_value_type<string_value> {
    using type = string_type;
};
template<>
struct primitive_value_type<uuid_value> {
    using type = uuid_type;
};
template<>
struct primitive_value_type<fixed_value> {
    using type = fixed_type;
};
template<>
struct primitive_value_type<binary_value> {
    using type = binary_type;
};

template<typename TVal>
using primitive_value_type_t = typename primitive_value_type<TVal>::type;

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
