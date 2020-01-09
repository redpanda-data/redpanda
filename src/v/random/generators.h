#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <boost/range/algorithm/generate.hpp>
#include <bytes/bytes.h>

#include <iostream>
#include <random>

// Random generators useful for testing.
namespace random_generators {

namespace internal {

inline std::random_device::result_type get_seed() {
    std::random_device rd;
    auto seed = rd();
    return seed;
}
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
    bytes b(bytes::initialized_later(), n);
    std::generate_n(b.begin(), n, [] { return get_int<bytes::value_type>(); });
    return b;
}

inline ss::sstring gen_alphanum_string(size_t n) {
    static constexpr char chars[]
      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    // do not include \0
    static constexpr std::size_t max_index = sizeof(chars) - 2;
    std::uniform_int_distribution<size_t> dist(0, max_index);
    ss::sstring s(ss::sstring::initialized_later(), n);
    std::generate_n(
      s.begin(), n, [&dist] { return chars[dist(internal::gen)]; });
    return s;
}

} // namespace random_generators
