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

#include <fmt/format.h>

#include <string_view>

#pragma once

/**
 * @brief A wrapper around a compile-time constant string.
 *
 * A static_str wraps an compile-time constant string, and is intended to be
 * used at runtime as a guarantee that a string has static lifetime. Such string
 * have trivial and cheap copy operations, since we just copy pointers (or
 * views) to the underlying string.
 */
class static_str {
public:
    /**
     * @brief Create a static_str based on a compile-time string literal.
     *
     * The use of consteval prevents passing anything other than compile-time
     * values, which necessarily have static lifetime.
     */
    consteval static_str(const char* str) // NOLINT(hicpp-explicit-conversions)
      : _data{str} {}

    /**
     * @brief Create a static_str based on a compile-time string literal.
     *
     * The use of consteval prevents passing anything other than compile-time
     * values, which necessarily have static lifetime.
     */
    explicit consteval static_str(std::string_view str)
      : _data{str} {}

    constexpr auto operator<=>(const static_str& rhs) const = default;

    /**
     * @brief Return the static_str as a string_view.
     */
    constexpr operator std::string_view() const { return _data; } // NOLINT

private:
    // We could validly store the underlying string as a const char* or
    // std::string_view, while offering the same API. We choose the string_view
    // approach because storing the size speeds up several common operations.
    // The pointer approach wins for size and the cost of copying, which is also
    // common on the fast path.
    //
    // Should this not be the right tradeoff for some specific use case it is
    // easy to define the other variant simply by changing the storage type.
    //
    // Comparison of generated assembly for a few interesting cases:
    // string_view based: https://godbolt.org/z/Pzq9d93qj
    // pointer based    : https://godbolt.org/z/azd8a3xT5

    // the underlying string_view
    std::string_view _data;
};

template<>
struct fmt::formatter<static_str> : public formatter<std::string_view> {
    constexpr auto format(static_str s, fmt::format_context& ctx) const {
        return formatter<std::string_view>::format(s, ctx);
    }
};
