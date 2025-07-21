/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

/**
 * Helper macros to declare and implement fmt::formatter for a type.
 *
 * This will aid in the libfmt migration from v9 into v10.
 *
 * Note that we use VFMT because libfmt's own macros start with FMT_ and we
 * don't want them to clash.
 */

#pragma once

// Declare a fmt::formatter for #type (hiding the verbose boilerplate)
//
// This macro should only be used in root scope (so outside any namespaces).
//
// Note this only is for declaring the formatter, so if you want to provide
// implementation entirely in the header as well you'll need to use VFMT_INLINE.
// Otherwise use VFMT_IMPL in the implementation file.
//
// Example usage:
//
//   namespace foo {
//   struct my_type {
//     int a;
//     std::string b;
//   };
//   } // namespace foo
//
//   VFMT_DECL(foo::my_type);
//
#define VFMT_DECL(type)                                                        \
    template<>                                                                 \
    struct fmt::formatter<type> : fmt::formatter<std::string_view> {           \
        auto format(const type&, fmt::format_context& ctx) const               \
          -> decltype(ctx.out());                                              \
    };                                                                         \
    std::ostream& operator<<(std::ostream& os, const type&)

// Implement a fmt::formatter for #type (hiding the verbose boilerplate)
//
// This macro should only be used in root scope (so outside any namespaces).
//
// Note this only is for implementing the formatter, so you'll need to also use
// VFMT_DECL in the header as well.
//
// The type's argument is always named `v` in the function implementation.
//
// Example usage:
//
//   VFMT_DECL(foo::my_type) {
//     return fmt::format_to(ctx.out(), "{{a: {}, b: {}}}", v.a, v.b)
//   }
//
#define VFMT_IMPL(type)                                                        \
    std::ostream& operator<<(std::ostream& os, const type& v) {                \
        fmt::print(os, "{}", v);                                               \
        return os;                                                             \
    }                                                                          \
    auto fmt::formatter<type>::format(const type& v, fmt::format_context& ctx) \
      const -> decltype(ctx.out())

// Declare and implement a fmt::formatter for #type (hiding the verbose
// boilerplate)
//
// This macro should only be used in root scope (so outside any namespaces).
//
// Note this is for inline usage of creating a formatter, if you want to break
// it up between a header and implementation file, see VFMT_DECL and VFMT_IMPL
//
// Example usage:
//
//   namespace foo {
//   struct my_type {
//     int a;
//     std::string b;
//   };
//   } // namespace foo
//   VFMT_INLINE(foo::my_type) {
//     return fmt::format_to(ctx.out(), "{{a: {}, b: {}}}", v.a, v.b);
//   }
//
#define VFMT_INLINE(type)                                                      \
    template<>                                                                 \
    struct fmt::formatter<type> : fmt::formatter<std::string_view> {           \
        auto format(const type&, fmt::format_context& ctx) const               \
          -> decltype(ctx.out());                                              \
    };                                                                         \
    inline std::ostream& operator<<(std::ostream& os, const type& v) {         \
        fmt::print(os, "{}", v);                                               \
        return os;                                                             \
    }                                                                          \
    inline auto fmt::formatter<type>::format(                                  \
      const type& v, fmt::format_context& ctx) const -> decltype(ctx.out())
