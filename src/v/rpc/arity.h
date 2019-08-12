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

namespace rpc {

namespace detail {

struct instance {
    template<typename Type>
    operator Type() const;
};

template<
  typename Aggregate,
  typename IndexSequence = std::index_sequence<>,
  typename = void>
struct arity_impl : IndexSequence {};

#pragma warning(disable : 4068)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#pragma warning(default : 4068)

template<typename Aggregate, std::size_t... Indices>
struct arity_impl<
  Aggregate,
  std::index_sequence<Indices...>,
  std::void_t<decltype(
    Aggregate{(static_cast<void>(Indices), std::declval<instance>())...,
              std::declval<instance>()})>>
  : arity_impl<Aggregate, std::index_sequence<Indices..., sizeof...(Indices)>> {
};

#pragma warning(disable : 4068)
#pragma GCC diagnostic pop
#pragma warning(default : 4068)

} // namespace detail

template<typename T>
constexpr std::size_t arity() {
    return detail::arity_impl<std::decay_t<T>>().size();
}

} // namespace rpc
