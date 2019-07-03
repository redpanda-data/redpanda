#pragma once

#include <boost/range/algorithm/generate.hpp>

#include <iostream>
#include <random>

// Random generators useful for testing.
namespace random_generators {

namespace internal {

inline std::random_device::result_type get_seed() {
    std::random_device rd;
    auto seed = rd();
    std::cout << "random::generators seed = " << seed << "\n";
    return seed;
}

} // namespace internal

inline std::default_random_engine gen(internal::get_seed());

template<typename T>
T get_int() {
    std::uniform_int_distribution<T> dist;
    return dist(gen);
}

template<typename T>
T get_int(T min, T max) {
    std::uniform_int_distribution<T> dist(min, max);
    return dist(gen);
}

template<typename T>
T get_int(T max) {
    return get_int<T>(0, max);
}

inline bytes get_bytes(size_t n) {
    bytes b(bytes::initialized_later(), n);
    boost::generate(b, [] { return get_int<bytes::value_type>(); });
    return b;
}

inline bytes get_bytes() {
    return get_bytes(get_int<unsigned>(128 * 1024));
}

} // namespace random_generators