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
constexpr inline auto envelope_to_tuple(T&& t) {
    return t.serde_fields();
}

template<typename Fn>
concept check_for_more_fn = requires(Fn&& fn, int& f) {
    { fn(f) } -> std::convertible_to<bool>;
};

template<is_envelope T, typename Fn>
inline auto envelope_for_each_field(T& t, Fn&& fn) {
    std::apply([&](auto&&... args) { (fn(args), ...); }, envelope_to_tuple(t));
}

template<is_envelope T, check_for_more_fn Fn>
inline auto envelope_for_each_field(T& t, Fn&& fn) {
    std::apply(
      [&](auto&&... args) { (void)(fn(args) && ...); }, envelope_to_tuple(t));
}

} // namespace serde
