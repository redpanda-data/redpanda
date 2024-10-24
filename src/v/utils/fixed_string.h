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

#include <algorithm>
#include <string_view>

/**
 * Compile time string owning string literal.
 * Inspired by
 * https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2016/p0259r0.pdf
 */
template<size_t N>
struct fixed_string {
    // NOLINTNEXTLINE(hicpp-explicit-conversions,hicpp-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays,cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    constexpr fixed_string(const char (&str)[N + 1]) noexcept {
        std::copy_n(str, N, value);
    }
    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    constexpr operator const char*() const { return value; }
    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    constexpr operator std::string_view() const {
        return std::string_view(value, N);
    }
    // NOLINTNEXTLINE(hicpp-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
    char value[N];
};

/**
 * Helper for deducing N (size of the string based on the size of reference to
 * an array of chars)
 * N - 1 is used to remove the terminating argument
 */
template<size_t N>
// NOLINTNEXTLINE(hicpp-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
fixed_string(const char (&)[N]) -> fixed_string<N - 1>;
