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

#include "bytes/bytes.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

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

bytes get_bytes(size_t n = 128 * 1024);

/**
 * Random string generator. Total number of distinct values that may be
 * generated is unlimited (within all possible values of given size).
 */
ss::sstring gen_alphanum_string(size_t n);

static constexpr size_t alphanum_max_distinct_strlen = 32;
/**
 * Random string generator that limits the maximum number of distinct values
 * that will be returned. That is, this function is a generator, which creates
 * members of a set of strings, one at a time. Each generated string has maximum
 * length `alphanum_max_distinct_strlen`. The total set of generated strings
 * will have a maximum cardinality of `max_cardinality`. See the unit test
 * `alphanum_max_distinct_generator` for an example.
 */
ss::sstring gen_alphanum_max_distinct(size_t max_cardinality);

// Makes an random alphanumeric string, encoded in an iobuf.
iobuf make_iobuf(size_t n = 128);

void fill_buffer_randomchars(char* start, size_t amount);

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

template<typename T>
const T& random_choice(const std::vector<T>& elements) {
    auto idx = get_int<size_t>(0, elements.size() - 1);
    return elements[idx];
}

template<typename T>
T random_choice(std::initializer_list<T> choices) {
    auto idx = get_int<size_t>(0, choices.size() - 1);
    auto& choice = *(choices.begin() + idx);
    return std::move(choice);
}

template<typename T>
T get_real() {
    std::uniform_real_distribution<T> dist;
    return dist(internal::gen);
}

template<typename T>
T get_real(T min, T max) {
    std::uniform_real_distribution<T> dist(min, max);
    return dist(internal::gen);
}

template<typename T>
T get_real(T max) {
    std::uniform_real_distribution<T> dist(0, max);
    return dist(internal::gen);
}

} // namespace random_generators
