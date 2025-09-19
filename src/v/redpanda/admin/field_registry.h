/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/time/time.h"
#include "serde/protobuf/base.h"
#include "utils/named_type.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace admin {

/// Generic reusable accessor for looking up the value of a specific field on an
/// object
template<typename T>
struct field_accessor_info {
    // clang-format off
    using int64_getter = named_type<std::function<int64_t(T&)>, struct int64_getter_tag>;
    using uint64_getter = named_type<std::function<uint64_t(T&)>, struct uint64_getter_tag>;
    using double_getter = named_type<std::function<double(T&)>, struct double_getter_tag>;
    using bool_getter = named_type<std::function<bool(T&)>, struct bool_getter_tag>;
    using string_getter = named_type<std::function<ss::sstring(T&)>, struct string_getter_tag>;
    using duration_getter = named_type<std::function<absl::Duration(T&)>, struct duration_getter_tag>;
    using time_getter = named_type<std::function<absl::Time(T&)>, struct time_getter_tag>;
    using enum_getter = named_type<std::function<ss::sstring(T&)>, struct enum_getter_tag>;
    // clang-format on

    using getter_t = std::variant<
      int64_getter,
      uint64_getter,
      double_getter,
      bool_getter,
      string_getter,
      duration_getter,
      time_getter,
      enum_getter>;

    getter_t getter;
};

/// Interface for translating field names to field accessors for an object T
template<typename T>
class field_registry {
public:
    virtual ~field_registry() = default;

    /**
     * Get field accessor information for a given field path.
     * @throws std::invalid_argument if field path is not found
     */
    virtual field_accessor_info<T>
    get_field_info(const std::string& field_path) const = 0;
};

template<typename T>
concept RedpandaProtobufMessage = std::derived_from<T, serde::pb::base_message>;

/// A registry implementation that uses reflection to dynamically look up fields
/// on a protobuf message generated using redpanda's protobuf serde generator.
template<RedpandaProtobufMessage T>
class protobuf_field_registry : public field_registry<T> {
public:
    field_accessor_info<T>
    get_field_info(const std::string& field_path) const override;

private:
    std::optional<std::vector<int32_t>>
    convert_field_path_to_numbers(const std::string& field_path) const;

    template<typename ValueType>
    static field_accessor_info<T> create_field_accessor(
      const std::string& field_path, const std::vector<int32_t>& field_numbers);

    template<typename ReturnType>
    static ReturnType
    extract_field_value(T& obj, const std::vector<int32_t>& field_numbers);
};

template<typename T>
std::unique_ptr<field_registry<T>> make_field_registry();

} // namespace admin
