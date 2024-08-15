/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <optional>
#include <type_traits>

namespace base {

/**
 * A utility for statically asserting false.
 *
 * Example usage:
 *
 * ```
 * template<typename T>
 * void foo() {
 *   if constexpr (...) {
 *     // ...
 *   } else {
 *     static_assert(base::unsupported_type<T>::value, "unsupported type");
 *   }
 * }
 *
 * ```
 *
 */
template<class T>
struct unsupported_type : std::false_type {};

/**
 * Similar to `unsupported_type` but for values instead of types.
 *
 * Example usage:
 *
 * enum class foo { bar, baz };
 *
 * template<foo value>
 * void check() {
 *   if constexpr (value == foo::bar) {
 *     // ...
 *   } else if constexpr (value == foo::baz) {
 *     // ...
 *   } else {
 *     static_assert(base::unsupported_value<value>::value, "supported foo");
 *   }
 * }
 */
template<auto V>
struct unsupported_value : std::false_type {};

} // namespace base

namespace detail {

template<typename T, template<typename...> class C>
struct is_specialization_of : std::false_type {};
template<template<typename...> class C, typename... Args>
struct is_specialization_of<C<Args...>, C> : std::true_type {};
template<typename T, template<typename...> class C>
inline constexpr bool is_specialization_of_v
  = is_specialization_of<T, C>::value;

} // namespace detail

namespace reflection {

template<typename T>
concept is_std_optional = ::detail::is_specialization_of_v<T, std::optional>;

}
