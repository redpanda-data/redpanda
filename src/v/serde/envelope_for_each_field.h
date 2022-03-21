// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "reflection/arity.h"
#include "reflection/to_tuple.h"
#include "serde/envelope.h"

#include <tuple>

namespace serde {

namespace detail {

template<typename T, typename = void>
struct has_serde_fields : std::false_type {};

template<typename T>
struct has_serde_fields<
  T,
  std::void_t<decltype(std::declval<std::decay_t<T>>().serde_fields())>>
  : std::true_type {};

template<typename T>
inline constexpr auto const has_serde_fields_v = has_serde_fields<T>::value;

} // namespace detail

template<
  typename T,
  std::enable_if_t<detail::has_serde_fields_v<T>, void*> = nullptr>
constexpr inline auto envelope_to_tuple(T&& t) {
    return t.serde_fields();
}

template<
  typename T,
  std::enable_if_t<!detail::has_serde_fields_v<T>, void*> = nullptr>
constexpr inline auto envelope_to_tuple(T& t) {
    static_assert(std::is_aggregate_v<T>);
    static_assert(std::is_standard_layout_v<T>);
    static_assert(!std::is_polymorphic_v<T>);

    constexpr auto const a = reflection::arity<T>() - 1;
    if constexpr (a == 0) {
        return std::tie();
    } else if constexpr (a == 1) {
        auto& [p1] = t;
        return std::tie(p1);
    } else if constexpr (a == 2) {
        auto& [p1, p2] = t;
        return std::tie(p1, p2);
    } else if constexpr (a == 3) {
        auto& [p1, p2, p3] = t;
        return std::tie(p1, p2, p3);
    } else if constexpr (a == 4) {
        auto& [p1, p2, p3, p4] = t;
        return std::tie(p1, p2, p3, p4);
    } else if constexpr (a == 5) {
        auto& [p1, p2, p3, p4, p5] = t;
        return std::tie(p1, p2, p3, p4, p5);
    } else if constexpr (a == 6) {
        auto& [p1, p2, p3, p4, p5, p6] = t;
        return std::tie(p1, p2, p3, p4, p5, p6);
    } else if constexpr (a == 7) {
        auto& [p1, p2, p3, p4, p5, p6, p7] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7);
    } else if constexpr (a == 8) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8);
    } else if constexpr (a == 9) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9);
    } else if constexpr (a == 10) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
    } else if constexpr (a == 11) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11);
    } else if constexpr (a == 12) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12);
    } else if constexpr (a == 13) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13] = t;
        return std::tie(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);
    } else if constexpr (a == 14) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14] = t;
        return std::tie(
          p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14);
    } else if constexpr (a == 15) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15]
          = t;
        return std::tie(
          p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15);
    } else if constexpr (a == 16) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16);
    } else if constexpr (a == 17) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17);
    } else if constexpr (a == 18) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17,
          p18);
    } else if constexpr (a == 19) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17,
          p18,
          p19);
    } else if constexpr (a == 20) {
        auto& [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20]
          = t;
        return std::tie(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p16,
          p17,
          p18,
          p19,
          p20);
    }
}

template<typename T, typename Fn>
inline auto envelope_for_each_field(T& t, Fn&& fn) -> std::enable_if_t<
  !std::is_convertible_v<decltype(fn(std::declval<int&>())), bool>> {
    static_assert(is_envelope_v<std::decay_t<T>>);
    if constexpr (inherits_from_envelope_v<std::decay_t<T>>) {
        std::apply(
          [&](auto&&... args) { (fn(args), ...); }, envelope_to_tuple(t));
    } else {
        std::apply(
          [&](auto&&... args) { (fn(args), ...); }, reflection::to_tuple(t));
    }
}

template<typename T, typename Fn>
inline auto envelope_for_each_field(T& t, Fn&& fn) -> std::enable_if_t<
  std::is_convertible_v<decltype(fn(std::declval<int&>())), bool>> {
    static_assert(is_envelope_v<std::decay_t<T>>);
    if constexpr (inherits_from_envelope_v<std::decay_t<T>>) {
        std::apply(
          [&](auto&&... args) { (void)(fn(args) && ...); },
          envelope_to_tuple(t));
    } else {
        std::apply(
          [&](auto&&... args) { (void)(fn(args) && ...); },
          reflection::to_tuple(t));
    }
}

} // namespace serde
