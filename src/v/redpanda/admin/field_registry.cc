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

#include "redpanda/admin/field_registry.h"

#include "absl/strings/str_split.h"
#include "src/v/redpanda/admin/tests/aip_filter_test_messages.proto.h"

#include <string_view>
#include <type_traits>

namespace admin {

template<RedpandaProtobufMessage T>
field_accessor_info<T> protobuf_field_registry<T>::get_field_info(
  const std::string& field_path) const {
    auto field_numbers_opt = convert_field_path_to_numbers(field_path);
    if (!field_numbers_opt) {
        throw std::invalid_argument("Invalid field path: " + field_path);
    }

    T sample_instance;
    auto field_opt = sample_instance.lookup_field(*field_numbers_opt);
    if (!field_opt) {
        throw std::invalid_argument("Field not found: " + field_path);
    }

    const auto& field = *field_opt;
    auto field_numbers = *field_numbers_opt; // Copy for lambda capture

    return std::visit(
      [&field_path,
       field_numbers](const auto& value) -> field_accessor_info<T> {
          using ValueType = std::decay_t<decltype(value)>;
          return create_field_accessor<ValueType>(field_path, field_numbers);
      },
      field.value);
}

template<RedpandaProtobufMessage T>
std::optional<std::vector<int32_t>>
protobuf_field_registry<T>::convert_field_path_to_numbers(
  const std::string& field_path) const {
    std::vector<std::string_view> path_components = absl::StrSplit(
      field_path, '.');

    T sample_instance;
    return sample_instance.convert_field_path_to_numbers(path_components);
}

// clang-format off
// Helper that maps protobuf field types to their corresponding field accessor function types
template<typename T, typename ValueType> struct field_getter_type;
template<typename T> struct field_getter_type<T, bool> { using type = typename field_accessor_info<T>::bool_getter; };
template<typename T> struct field_getter_type<T, serde::pb::raw_enum_value> { using type = typename field_accessor_info<T>::enum_getter; };
template<typename T> struct field_getter_type<T, uint32_t> { using type = typename field_accessor_info<T>::uint64_getter; };
template<typename T> struct field_getter_type<T, uint64_t> { using type = typename field_accessor_info<T>::uint64_getter; };
template<typename T> struct field_getter_type<T, int32_t> { using type = typename field_accessor_info<T>::int64_getter; };
template<typename T> struct field_getter_type<T, int64_t> { using type = typename field_accessor_info<T>::int64_getter; };
template<typename T> struct field_getter_type<T, float> { using type = typename field_accessor_info<T>::double_getter; };
template<typename T> struct field_getter_type<T, double> { using type = typename field_accessor_info<T>::double_getter; };
template<typename T> struct field_getter_type<T, ss::sstring> { using type = typename field_accessor_info<T>::string_getter; };
template<typename T> struct field_getter_type<T, iobuf> { using type = typename field_accessor_info<T>::string_getter; };
template<typename T> struct field_getter_type<T, absl::Time> { using type = typename field_accessor_info<T>::time_getter; };
template<typename T> struct field_getter_type<T, absl::Duration> { using type = typename field_accessor_info<T>::duration_getter; };
// clang-format on

template<RedpandaProtobufMessage T>
template<typename ValueType>
field_accessor_info<T> protobuf_field_registry<T>::create_field_accessor(
  const std::string& field_path, const std::vector<int32_t>& field_numbers) {
    if constexpr (std::is_same_v<ValueType, std::monostate>) {
        throw std::invalid_argument(
          "Cannot create accessor for unset field: " + field_path);
    } else if constexpr (requires {
                             typename field_getter_type<T, ValueType>::type;
                         }) {
        using GetterType = typename field_getter_type<T, ValueType>::type;
        using ReturnType = std::invoke_result_t<typename GetterType::type, T&>;

        return field_accessor_info<T>{
          .getter = GetterType{[field_numbers](T& obj) {
              return extract_field_value<ReturnType>(obj, field_numbers);
          }}};
    } else {
        throw std::invalid_argument(
          "Unsupported field type for filtering: " + field_path);
    }
}

template<RedpandaProtobufMessage T>
template<typename ReturnType>
ReturnType protobuf_field_registry<T>::extract_field_value(
  T& obj, const std::vector<int32_t>& field_numbers) {
    auto field_opt = obj.lookup_field(field_numbers);
    if (!field_opt) {
        throw std::runtime_error("Field lookup failed during extraction");
    }

    return std::visit(
      [](const auto& value) -> ReturnType {
          using ValueType = std::decay_t<decltype(value)>;

          if constexpr (std::is_same_v<ReturnType, ValueType>) {
              return value;
          } else if constexpr (std::is_same_v<ReturnType, int64_t>) {
              if constexpr (std::is_same_v<ValueType, int32_t>) {
                  return static_cast<int64_t>(value);
              }
          } else if constexpr (std::is_same_v<ReturnType, uint64_t>) {
              if constexpr (std::is_same_v<ValueType, uint32_t>) {
                  return static_cast<uint64_t>(value);
              }
          } else if constexpr (std::is_same_v<ReturnType, double>) {
              if constexpr (std::is_same_v<ValueType, float>) {
                  return static_cast<double>(value);
              }
          } else if constexpr (std::is_same_v<ReturnType, ss::sstring>) {
              if constexpr (std::
                              is_same_v<ValueType, serde::pb::raw_enum_value>) {
                  return ss::sstring{value.name};
              }
          }
          throw std::runtime_error("Unsupported field value type conversion");
      },
      field_opt->value);
}

template<typename T>
std::unique_ptr<field_registry<T>> make_field_registry() {
    return std::make_unique<protobuf_field_registry<T>>();
}

// Explicitly instantiating for all supported protobuf types to avoid having to
// expose all the templated code in the header
template class protobuf_field_registry<aip_filter_test::test_message>;
template std::unique_ptr<field_registry<aip_filter_test::test_message>>
make_field_registry<aip_filter_test::test_message>();

} // namespace admin
