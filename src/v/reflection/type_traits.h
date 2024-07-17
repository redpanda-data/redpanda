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

#include "base/seastarx.h"
#include "base/type_traits.h"
#include "container/fragmented_vector.h"
#include "utils/named_type.h"
#include "utils/tristate.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/btree_set.h>

#include <array>
#include <optional>
#include <type_traits>
#include <vector>

namespace detail {

template<class T, template<class, size_t> class C>
struct is_specialization_of_sized : std::false_type {};
template<template<class, size_t> class C, class T, size_t N>
struct is_specialization_of_sized<C<T, N>, C> : std::true_type {};
template<typename T, template<class, size_t> class C>
inline constexpr bool is_specialization_of_sized_v
  = is_specialization_of_sized<T, C>::value;

template<class T>
struct is_std_array_t : std::false_type {};
template<class T, std::size_t N>
struct is_std_array_t<std::array<T, N>> : std::true_type {};

template<typename T>
struct is_duration : std::false_type {};
template<typename Rep, typename Period>
struct is_duration<std::chrono::duration<Rep, Period>> : std::true_type {};
template<typename T>
inline constexpr bool is_duration_v = is_duration<T>::value;

} // namespace detail

namespace reflection {

template<typename T>
concept is_std_vector = ::detail::is_specialization_of_v<T, std::vector>;

template<typename T>
concept is_fragmented_vector
  = ::detail::is_specialization_of_sized_v<T, fragmented_vector>;

template<typename T>
concept is_ss_chunked_fifo
  = ::detail::is_specialization_of_sized_v<T, ss::chunked_fifo>;

template<typename T>
concept is_absl_btree_set
  = ::detail::is_specialization_of_v<T, absl::btree_set>;

template<typename T>
concept is_std_array = ::detail::is_std_array_t<T>::value;

template<typename T>
concept is_ss_circular_buffer
  = ::detail::is_specialization_of_v<T, ss::circular_buffer>;

template<typename T>
concept is_rp_named_type
  = ::detail::is_specialization_of_v<T, ::detail::base_named_type>;

template<typename T>
concept is_ss_bool_class = ::detail::is_specialization_of_v<T, ss::bool_class>;

template<typename T>
concept is_tristate = ::detail::is_specialization_of_v<T, tristate>;

} // namespace reflection
