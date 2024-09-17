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

#include <absl/hash/hash.h>
#include <boost/functional/hash.hpp>

#include <functional>

namespace hash {
namespace impl {
/// Pulled from utils/functional.h to prevent making v::utils a dependency of
/// v::hashing
template<typename>
inline constexpr bool always_false_v = false;

template<typename T>
concept is_absl_hashable = requires(const T& t) {
    { absl::Hash<T>{}(t) } -> std::convertible_to<std::size_t>;
};

template<typename T>
concept is_std_hashable = requires(const T& t) {
    { std::hash<T>{}(t) } -> std::convertible_to<std::size_t>;
};

template<typename T>
concept is_boost_hashable = requires(const T& t) {
    { boost::hash<T>{}(t) } -> std::convertible_to<std::size_t>;
};

template<typename T>
size_t combine(size_t& seed, const T& t) {
    if constexpr (is_std_hashable<T>) {
        boost::hash_combine(seed, std::hash<T>{}(t));
    } else if constexpr (is_absl_hashable<T>) {
        boost::hash_combine(seed, absl::Hash<T>{}(t));
    } else if constexpr (is_boost_hashable<T>) {
        boost::hash_combine(seed, boost::hash<T>{}(t));
    } else {
        static_assert(always_false_v<T>, "No hasher found for T");
    }
    return seed;
}

} // namespace impl

template<typename... Args>
size_t combine(size_t& seed, Args&&... args) {
    return (impl::combine(seed, std::forward<Args>(args)), ...);
}

} // namespace hash
