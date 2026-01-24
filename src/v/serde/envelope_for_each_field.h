// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde/envelope.h"

#include <tuple>

namespace serde {

template<typename T>
[[clang::always_inline]]
constexpr inline auto envelope_to_tuple(T&& t) {
    [[clang::always_inline]] return t.serde_fields();
}

template<typename Fn>
concept check_for_more_fn = requires(Fn&& fn, int& f) {
    { fn(f) } -> std::convertible_to<bool>;
};

template<is_envelope T, typename Fn>
[[clang::always_inline]]
inline auto envelope_for_each_field(T& t, Fn&& fn) {
    [[clang::always_inline]] std::apply(
      [&](auto&&... args) { (fn(args), ...); }, envelope_to_tuple(t));
}

template<is_envelope T, check_for_more_fn Fn>
[[clang::always_inline]]
inline auto envelope_for_each_field(T& t, Fn&& fn) {
    [[clang::always_inline]] std::apply(
      [&] [[clang::always_inline]] (auto&&... args) {
          (void)(fn(args) && ...);
      },
      envelope_to_tuple(t));
}

} // namespace serde
