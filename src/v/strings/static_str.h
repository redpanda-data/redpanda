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
#include <type_traits>

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
     * @brief Create a static_str based on a compile-time literal.
     *
     * The use of consteval prevents passing anything other than compile-time
     * values.
     */
    consteval static_str(const char* str) // NOLINT
      : _data{str} {
        // do a compile-time check that the string is zero-terminated
        std::string_view{str};
    }

    constexpr bool operator==(const static_str& rhs) const {
        if (!std::is_constant_evaluated() && _data == rhs._data) {
            // fast path based on pointer comparison, which is a sufficient but
            // not necessary condition for equality
            return true;
        }
        return *this == static_cast<std::string_view>(rhs);
    }

    constexpr auto operator<=>(const static_str& rhs) const {
        if (!std::is_constant_evaluated() && _data == rhs._data) {
            return std::strong_ordering::equal;
        }
        return *this <=> rhs.operator std::string_view();
    }

    /**
     * @brief Return the static_str as a string_view.
     */
    constexpr operator std::string_view() const { return _data; } // NOLINT

private:
    // We could validly store the underlying string as a const char* or
    // std::string_view, while offering the same API. We choose the pointer
    // approach because it optimizes for size and the cost of copying, which is
    // common on the fast path. It is slower to calculate the size, but the
    // strings in the intended use case tend to be short and the actual use of
    // string may not occur frequently.
    //
    // Should this not be the right tradeoff for some specific use case it is
    // easy to define the other variant simply by changing the storage type.
    //
    // Comparison of generated assembly for a few interesting cases:
    // string_view based: https://godbolt.org/z/Pzq9d93qj
    // pointer based    : https://godbolt.org/z/Pzq9d93qj

    // pointer to the underlying zero-terminated string
    const char* _data;
};

template<>
struct fmt::formatter<static_str> : public formatter<std::string_view> {
    constexpr auto format(static_str s, fmt::format_context& ctx) const {
        return formatter<std::string_view>::format(s, ctx);
    }
};
