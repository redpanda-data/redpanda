/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "random/generators.h"
#include "vassert.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <concepts>
#include <cstdint>
#include <limits>

/*
 * An implementation of a Morris counter. The general idea is that the
 * closer the constant 'a' is to infinity the more accurate the count will be
 * at the cost of a much lower 'max_count'.
 *
 * Bounds for variance and space can be found at:
 * https://www.cs.princeton.edu/~hy2/files/approx_counting.pdf
 */
template<std::unsigned_integral count_t, size_t a>
class approximate_count {
    static constexpr long double inv_a = 1.0L / a;
    static constexpr long double factor = 1.0L + inv_a;

    static constexpr uint64_t get_count(count_t n) {
        long double val_d = (std::pow(factor, n) - 1.0) / inv_a;
        return std::llroundl(val_d);
    }

public:
    // The highest value this counter can count to for a given value of 'a'.
    // TODO: in C++23 this should be a constexpr.
    static uint64_t max_count() {
        return get_count(std::numeric_limits<count_t>::max());
    }

    approximate_count& operator++() {
        // Cap exponent at upper limit for count_t to avoid overflowing.
        if (_exp == std::numeric_limits<count_t>::max()) {
            return *this;
        }

        long double p = std::pow(factor, -1 * _exp);
        auto r = random_generators::get_real<long double>(
          0.0, std::nextafter(1.0, std::numeric_limits<long double>::max()));

        if (r < p) {
            _exp += 1;
        }

        return *this;
    }

    uint64_t get() const { return get_count(_exp); }

private:
    count_t _exp{0};
};

/*
 * The constant factor from the original Morris
 * paper. Allows for a max count of ~130k.
 */
constexpr size_t a_const_default = 30;

/*
 * Defines a histogram with a fixed number of buckets each of
 * which a even value is mapped to via the hash function.
 */
template<
  typename key_t,
  std::unsigned_integral bucket_t,
  size_t number_of_buckets,
  size_t a = a_const_default,
  class hash_t = std::hash<key_t>>
class approximate_histogram {
public:
    approximate_histogram& operator+=(const key_t& val) {
        auto bucket = hash_t{}(val);
        vassert(0 <= bucket < number_of_buckets, "out of range");

        ++_hist[bucket];

        return *this;
    }

    constexpr uint64_t operator[](size_t bucket) const {
        return _hist[bucket].get();
    }

private:
    std::array<approximate_count<bucket_t, a>, number_of_buckets> _hist{};
};

template<size_t number_of_buckets, size_t min_pow_2>
class size_to_buckets {
    static constexpr size_t max_pow_2 = number_of_buckets + min_pow_2 - 1;

public:
    size_t operator()(size_t bytes) const noexcept {
        auto pow_2 = std::bit_width(bytes) - 1;

        return std::clamp(pow_2, min_pow_2, max_pow_2) - min_pow_2;
    }
};

/*
 * Creates a histogram with 16 buckets each representing
 * a count of allocations of size [2^(i-1), 2^i) bytes i.e,
 * [0, 2^5), [2^5, 2^6), [2^6, 2^7), ... , [2^19, inf)
 *
 * Each bucket can count up to 6.12 × 10^8 allocations.
 *
 * sizeof(memory_histogram_t) == 16
 */
using memory_histogram_t
  = approximate_histogram<size_t, uint8_t, 16, 14, size_to_buckets<16, 4>>;
