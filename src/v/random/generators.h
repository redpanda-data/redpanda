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

#include <seastar/core/sstring.hh>

#include <boost/range/algorithm/generate.hpp>
#include <bytes/bytes.h>

#include <random>

// Random generators useful for testing.
namespace random_generators {

namespace internal {

inline std::random_device::result_type get_seed() {
    std::random_device rd;
    auto seed = rd();
    return seed;
}
// NOLINTNEXTLINE
static thread_local std::default_random_engine gen(internal::get_seed());
} // namespace internal

template<typename T>
T get_int() {
    std::uniform_int_distribution<T> dist;
    return dist(internal::gen);
}

template<typename T>
T get_int(T min, T max) {
    std::uniform_int_distribution<T> dist(min, max);
    return dist(internal::gen);
}

template<typename T>
T get_int(T max) {
    return get_int<T>(0, max);
}

inline bytes get_bytes(size_t n = 128 * 1024) {
    auto b = ss::uninitialized_string<bytes>(n);
    std::generate_n(b.begin(), n, [] { return get_int<bytes::value_type>(); });
    return b;
}

inline ss::sstring gen_alphanum_string(size_t n) {
    static constexpr std::string_view chars
      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    // do not include \0
    static constexpr std::size_t max_index = chars.size() - 2;
    std::uniform_int_distribution<size_t> dist(0, max_index);
    auto s = ss::uninitialized_string(n);
    std::generate_n(
      s.begin(), n, [&dist] { return chars[dist(internal::gen)]; });
    return s;
}

} // namespace random_generators
