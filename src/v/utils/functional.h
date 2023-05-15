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

#include <functional>
#include <optional>

template<typename>
inline constexpr bool always_false_v = false;

template<typename T, typename U = typename T::value_type>
concept SupportsPushBack = requires(T a, U b) {
    { a.push_back(b) } -> std::same_as<void>;
};

namespace reduce {
struct push_back {
    template<typename VecLike>
    requires SupportsPushBack<VecLike>
    VecLike operator()(VecLike acc, typename VecLike::value_type t) const {
        acc.push_back(std::move(t));
        return acc;
    }
};

struct push_back_opt {
    template<typename VecLike>
    requires SupportsPushBack<VecLike>
    VecLike operator()(
      VecLike acc, std::optional<typename VecLike::value_type> ot) const {
        if (ot) {
            acc.push_back(std::move(*ot));
        }
        return acc;
    }
};

} // namespace reduce

namespace xform {
/// Even though std::identity is in the spec for C++20 seems as though clang
/// devs haven't gotten around to implementing it yet.
#ifdef __GLIBCXX__
using identity = std::identity;
#else
struct identity {
    template<typename T>
    constexpr T&& operator()(T&& t) const noexcept {
        return std::forward<T>(t);
    }
};
#endif

template<typename T>
struct equal_to {
    explicit equal_to(T value)
      : _value(std::move(value)) {}
    bool operator()(const T& other) const { return _value == other; }
    T _value;
};

template<typename T>
struct not_equal_to {
    explicit not_equal_to(T value)
      : _value(std::move(value)) {}
    bool operator()(const T& other) const { return _value != other; }
    T _value;
};

} // namespace xform
