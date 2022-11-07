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
#include "tristate.h"
#include "utils/named_type.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
#include <type_traits>
#include <vector>

namespace detail {

template<typename T, template<typename...> class C>
struct is_specialization_of : std::false_type {};
template<template<typename...> class C, typename... Args>
struct is_specialization_of<C<Args...>, C> : std::true_type {};
template<typename T, template<typename...> class C>
inline constexpr bool is_specialization_of_v
  = is_specialization_of<std::decay_t<T>, C>::value;

} // namespace detail

namespace reflection {

template<typename T>
concept is_std_vector = ::detail::is_specialization_of_v<T, std::vector>;

template<typename T>
concept is_ss_circular_buffer
  = ::detail::is_specialization_of_v<T, ss::circular_buffer>;

template<typename T>
concept is_std_optional = ::detail::is_specialization_of_v<T, std::optional>;

template<typename T>
concept is_rp_named_type
  = ::detail::is_specialization_of_v<T, ::detail::base_named_type>;

template<typename T>
concept is_ss_bool_class = ::detail::is_specialization_of_v<T, ss::bool_class>;

template<typename T>
concept is_tristate = ::detail::is_specialization_of_v<T, tristate>;

} // namespace reflection
