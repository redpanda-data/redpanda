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

#include "base/vlog.h"
#include "serde/rw/rw.h"

#include <array>
#include <type_traits>
#include <utility>
#include <variant>

namespace serde {

// A small wrapper around std::variant that is marked to be serializable.
//
// Special precation needs to be taken to mark a variant type as serializable
// with respect to compatibility. `serde::variant` should be a drop in
// replacement for `std::variant`, but allows for serde operations.
//
// # Variant Wire Compatibility:
//
// Variant is treated as a primitive atomic type, that means that *any* changes
// to the variant itself is not backwards compatible. `serde::variant` should
// always be wrapped in another `serde::envelope` to allow for changing the
// variant, and that wrapper struct needs to handle changes to the variant.
template<typename... Types>
struct variant : public std::variant<Types...> {
    using type = std::variant<Types...>;

    constexpr variant() noexcept(std::is_nothrow_default_constructible_v<
                                 std::variant_alternative_t<0, type>>)
      = default;
    constexpr variant(const variant&) noexcept(
      std::is_nothrow_copy_constructible_v<type>)
      = default;
    constexpr variant(variant&&) noexcept(
      std::is_nothrow_move_constructible_v<type>)
      = default;

    // Ensure that this is not implicitly convertable from std::variant
    // but allow assignment from each individual type. For example:
    //
    // ```cpp
    // using my_variant = serde::variant<int, bool>
    //
    // my_variant v = false; // should compile
    //
    // my_variant v = std::variant<int, bool>(false); // should NOT compile
    // ```
    template<class T>
    constexpr variant(T&& t) // NOLINT(*-explicit-*)
      noexcept(std::is_nothrow_constructible_v<type, decltype(t)>)
    requires(!std::is_same_v<std::decay_t<T>, type>)
      : type(std::forward<T>(t)){};
    // Allow explicit conversion from std::variant
    explicit constexpr variant(type v) noexcept(
      std::is_nothrow_move_constructible_v<type>)
      : type(std::move(v)) {};

    template<class T, class... Args>
    constexpr explicit variant(
      std::in_place_type_t<T> in_place,
      Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>)
      : type(in_place, std::forward<Args...>(args)...) {}
    template<std::size_t I, class... Args>
    constexpr explicit variant(
      std::in_place_index_t<I> in_place,
      Args&&... args) noexcept(std::
                                 is_nothrow_constructible_v<
                                   std::variant_alternative_t<I, type>,
                                   Args...>)
      : type(in_place, std::forward<Args...>(args)...) {}

    variant&
    operator=(const variant&) noexcept(std::is_nothrow_copy_assignable_v<type>)
      = default;
    variant&
    operator=(variant&&) noexcept(std::is_nothrow_move_assignable_v<type>)
      = default;

    constexpr ~variant() noexcept = default;

    using type::emplace;
    using type::index;
    using type::swap;
    using type::valueless_by_exception;
};

template<typename... T>
void tag_invoke(tag_t<write_tag>, iobuf& out, variant<T...> v) {
    write<size_t>(
      out, std::variant_size_v<typename std::decay_t<decltype(v)>::type>);
    write<size_t>(out, v.index());
    std::visit(
      [&out]<typename V>(V&& v) { write(out, std::forward<V>(v)); },
      std::move(v));
}

namespace detail {

template<typename Variant>
struct variant_factory {
    using constructor = Variant (*)(iobuf_parser&, std::size_t);
    using constructor_table
      = std::array<constructor, std::variant_size_v<Variant>>;

    consteval variant_factory()
      : constructors([]<std::size_t... Index>(std::index_sequence<Index...>) {
          return std::to_array<constructor>({
            [](iobuf_parser& in, std::size_t bytes_left_limit) {
                return Variant{
                  std::in_place_index<Index>,
                  read_nested<std::variant_alternative_t<Index, Variant>>(
                    in, bytes_left_limit)};
            }...,
          });
      }(std::make_index_sequence<std::variant_size_v<Variant>>())) {}

    constructor_table constructors;
};

} // namespace detail

template<typename... T>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  variant<T...>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    using UnderlyingType = Type::type;

    auto size = read_nested<size_t>(in, bytes_left_limit);
    auto index = read_nested<size_t>(in, bytes_left_limit);

    if (size != std::variant_size_v<UnderlyingType>) [[unlikely]] {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "reading type {} of size {}: {} bytes left - unexpected variant "
          "size: {}, current variant size: {}, likely backwards compat issues.",
          type_str<Type>(),
          sizeof(Type),
          in.bytes_left(),
          size,
          std::variant_size_v<UnderlyingType>));
    }
    if (index >= std::variant_size_v<UnderlyingType>) [[unlikely]] {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "reading type {} of size {}: {} bytes left - unexpected variant "
          "index: {}, variant size: {}",
          type_str<Type>(),
          sizeof(Type),
          in.bytes_left(),
          index,
          std::variant_size_v<UnderlyingType>));
    }
    constexpr detail::variant_factory<UnderlyingType> factory{};
    t = Type(factory.constructors[index](in, bytes_left_limit));
}

} // namespace serde
