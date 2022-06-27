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
#include "model/metadata.h"
#include "net/unresolved_address.h"
#include "random/generators.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <bits/stdint-uintn.h>

#include <limits>
#include <optional>
#include <vector>

namespace tests {

inline net::unresolved_address random_net_address() {
    return net::unresolved_address(
      random_generators::gen_alphanum_string(
        random_generators::get_int(1, 100)),
      random_generators::get_int(1025, 65535));
}

inline bool random_bool() { return random_generators::get_int(0, 100) > 50; }

template<typename T>
T random_named_string(size_t size = 20) {
    return T{random_generators::gen_alphanum_string(size)};
}

template<typename T>
T random_named_int() {
    return T{random_generators::get_int<typename T::type>(
      0, std::numeric_limits<T>::max())};
}

template<typename Func>
auto random_optional(Func f) {
    using T = decltype(f());
    if (random_bool()) {
        return std::optional<T>(f());
    }
    return std::optional<T>();
}

template<typename Func>
auto random_tristate(Func f) {
    using T = decltype(f());
    if (random_bool()) {
        return tristate<T>{};
    }
    return tristate<T>(random_optional(f));
}

inline std::chrono::milliseconds random_duration_ms() {
    return std::chrono::milliseconds(random_generators::get_int<uint64_t>(
      0, std::chrono::milliseconds::max().count()));
}

} // namespace tests
