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

// Credits: originally taken from cista.rocks (MIT License)
#include <type_traits>
#include <utility>

// Credits: Implementation by Anatoliy V. Tomilov (@tomilov),
//          based on gist by Rafal T. Janik (@ChemiaAion)
//
// Resources:
// https://playfulprogramming.blogspot.com/2016/12/serializing-structs-with-c17-structured.html
// https://codereview.stackexchange.com/questions/142804/get-n-th-data-member-of-a-struct
// https://stackoverflow.com/questions/39768517/structured-bindings-width
// https://stackoverflow.com/questions/35463646/arity-of-aggregate-in-logarithmic-time
// https://stackoverflow.com/questions/38393302/returning-variadic-aggregates-struct-and-syntax-for-c17-variadic-template-c

namespace reflection {

namespace detail {

struct instance {
    template<typename Type>
    operator Type() const; // NOLINt
};

template<
  typename Aggregate,
  typename IndexSequence = std::index_sequence<>,
  typename = void>
struct arity_impl : IndexSequence {};

template<typename Aggregate, std::size_t... Indices>
struct arity_impl<
  Aggregate,
  std::index_sequence<Indices...>,
  std::void_t<decltype(Aggregate{
    (static_cast<void>(Indices), std::declval<instance>())...,
    std::declval<instance>()})>>
  : arity_impl<Aggregate, std::index_sequence<Indices..., sizeof...(Indices)>> {
};
} // namespace detail

template<typename T>
constexpr std::size_t arity() {
    return detail::arity_impl<std::decay_t<T>>().size();
}

} // namespace reflection
