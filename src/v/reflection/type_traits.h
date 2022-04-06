/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <type_traits>
#include <vector>

namespace reflection {

template<typename T>
struct is_std_vector : std::false_type {};
template<typename... Args>
struct is_std_vector<std::vector<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_vector_v = is_std_vector<T>::value;

template<typename T>
struct is_ss_circular_buffer : std::false_type {};
template<typename... Args>
struct is_ss_circular_buffer<ss::circular_buffer<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_ss_circular_buffer_v = is_ss_circular_buffer<T>::value;

template<typename T>
struct is_std_optional : std::false_type {};
template<typename... Args>
struct is_std_optional<std::optional<Args...>> : std::true_type {};
template<typename T>
inline constexpr bool is_std_optional_v = is_std_optional<T>::value;

template<typename T>
struct is_named_type : std::false_type {};
template<typename T, typename Tag>
struct is_named_type<named_type<T, Tag>> : std::true_type {};
template<typename T>
inline constexpr bool is_named_type_v = is_named_type<T>::value;

template<typename T>
struct is_ss_bool : std::false_type {};
template<typename T>
struct is_ss_bool<ss::bool_class<T>> : std::true_type {};
template<typename T>
inline constexpr bool is_ss_bool_v = is_ss_bool<T>::value;

} // namespace reflection
