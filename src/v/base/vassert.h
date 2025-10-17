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

#include "base/likely.h"

#include <fmt/core.h>

#include <type_traits>

namespace detail {

// Pass by value for 16-byte values which are trivially copyable.
// This captures most/all of the values which can be passed in registers,
// and avoids large copies. The condition here only affects binary size
// and performance, not correctness: it could be hardcoded to false or
// true and still be correct.
template<typename T>
constexpr bool pass_by_value = std::is_trivially_copy_constructible_v<T>
                               && sizeof(T) <= 16;

template<typename T, typename T_ = std::remove_cvref_t<T>>
using fwd_type = std::conditional_t<pass_by_value<T_>, T_, const T&>;

// This thunk is fully type erased. See implementation details in vassert.cc.
[[gnu::cold]] [[noreturn]]
void assert_failed_thunk2(
  const char* prefix, const char* msg, fmt::format_args args) noexcept;

// This thunk accepts all the format arguments using the selected passing method
// and erases them. It is cold so appears in the cold section of the binary.
template<typename... Args>
[[gnu::cold]] [[noreturn]] [[gnu::noinline]]
void assert_failed_thunk1(
  const char* prefix, const char* msg, Args... args) noexcept {
    assert_failed_thunk2(prefix, msg, fmt::make_format_args(args...));
}

// This thunk will be inlined into the calling function and is responsible for
// dispatching. We need the always_inline since otherwise the noreturn attribute
// causes clang to outline it.
template<typename... Args>
[[noreturn]] [[gnu::always_inline]]
inline void assert_failed_thunk0(
  const char* prefix, const char* msg, const Args&... args) noexcept {
    ::detail::assert_failed_thunk1<fwd_type<Args>...>(prefix, msg, args...);
}

} // namespace detail

// helpers to turn __LINE__ into a string literal
#define STR_VASSERT2(x) #x
#define STR_VASSERT(x) STR_VASSERT2(x)

/// Assertion macro that's always enabled, including in release builds.
///
/// This macro provides detailed error messages and backtraces when assertions
/// fail. Unlike standard assert(), vassert() remains active in release builds,
/// making it suitable for critical invariant checking in production code.
///
/// Use dassert() for expensive debug-only checks that should be compiled out
/// in release builds.
///
/// Meant to be used in the same way as assert(condition, msg);
/// which means we use the negative conditional.
/// i.e.:
///
/// open_fileset::~open_fileset() noexcept {
///   vassert(_closed, "fileset not closed");
/// }

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define vassert(x, msg, args...)                                               \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        /*The !(x) is not an error. see description above*/                    \
        if (unlikely(!(x))) {                                                  \
            ::detail::assert_failed_thunk0(                                    \
              "(" __FILE__ ":" STR_VASSERT(__LINE__) ") '" #x "'",             \
              msg,                                                             \
              ##args);                                                         \
        }                                                                      \
    } while (0)

/// Debug-only assertion that is compiled out in release builds.
///
/// This macro behaves identically to vassert() in debug builds but becomes
/// a no-op in release builds (when NDEBUG is defined). Use dassert() for
/// expensive checks that should only run during development and testing.
///
/// Prefer over standard assert() for consistent calling conventions and
/// backtraces.
#ifndef NDEBUG
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define dassert(x, msg, args...) vassert(x, msg, ##args)
#else
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define dassert(...)
#endif

/// Crash the process if this line is ever reached, including in release builds.
///
/// This macro provides a safe alternative to std::unreachable() that guarantees
/// program termination with a clear error message and backtrace when reached.
/// Unlike std::unreachable() which results in undefined behavior if reached,
/// vunreachable() will always terminate the process cleanly with diagnostic
/// information.
///
/// Note: This macro might result in slightly worse assembly code and does not
/// enable certain compiler optimizations like std::unreachable() would.
#define vunreachable(msg, args...)                                             \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do                                                                         \
        [[unlikely]] {                                                         \
            ::detail::assert_failed_thunk0(                                    \
              "(" __FILE__ ":" STR_VASSERT(__LINE__) ")", msg, ##args);        \
        }                                                                      \
    while (0)
