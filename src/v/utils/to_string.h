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

#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/print.hh>

#include <optional>
#include <ostream>
#include <variant>

namespace std {

template<typename T>
std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt) {
        fmt::print(os, "{{{}}}", *opt);
        return os;
    }
    return os << "{nullopt}";
}

template<typename... T>
requires(sizeof...(T) > 0) std::ostream&
operator<<(std::ostream& os, const std::variant<T...>& v) {
    std::visit([&os](auto& arg) { fmt::print(os, "{{{}}}", arg); }, v);
    return os;
}

inline std::ostream&
operator<<(std::ostream& o, const ss::lowres_clock::duration& d) {
    fmt::print(
      o,
      "{}",
      std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
    return o;
}

template<typename T, typename Hash, typename KeyEqual, typename Allocator>
std::ostream& operator<<(
  std::ostream& os, const std::unordered_set<T, Hash, KeyEqual, Allocator>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem;
    }
    os << "}";
    return os;
}

} // namespace std
