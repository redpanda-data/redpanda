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

#include "absl/random/random.h"
#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <random>

// Random generators useful for testing.
namespace random_generators {

// tag to select the random-seed constructor
struct random_seed_tag {};

/**
 * Holds random generator state and allows generation of random values.
 *
 * Each object of this class encapsulates its own state. I.e., two objects
 * created with the same seed will generate the same sequence of values.
 *
 * It is similar in principle to a std "random engine", but with methods to
 * directly generate random values of various types, and with a default
 * seeding policy that allows seeding to vary between fuzz and non-fuzz tests.
 */
class rng {
public:
    using seed_type = uint64_t;
    using engine_type = absl::random_internal::pcg64_2018_engine;

    /// Initializes an rng object using the _default_ seed
    /// policy, which is fixed in most contexts, but random in
    /// fuzz test contexts.
    rng();

    /// Initializes an rng object using a random seed regardless
    /// of the default policy.
    explicit rng(random_seed_tag);

    /// Initializes with a given seed.
    /// @param seed The seed value to initialize the generator with
    explicit rng(seed_type seed);

    /// The initial seed used to create this rng object.
    /// If you create another object with the same seed it should
    /// generate the same sequence of values.
    /// @return The seed value used to initialize this generator
    seed_type initial_seed() const { return initial_seed_; }

    /// Generate a random integer of type T using uniform distribution
    /// over the entire range of T.
    /// @return A random integer of type T
    template<typename T>
    T get_int() {
        std::uniform_int_distribution<T> dist;
        return dist(gen_);
    }

    /// Generate a random integer of type T uniformly distributed
    /// between min and max (inclusive).
    /// @param min The minimum value (inclusive)
    /// @param max The maximum value (inclusive)
    /// @return A random integer between min and max
    template<typename T>
    T get_int(T min, T max) {
        std::uniform_int_distribution<T> dist(min, max);
        return dist(gen_);
    }

    /// Generate a random integer of type T uniformly distributed
    /// between 0 and max (inclusive).
    /// @param max The maximum value (inclusive)
    /// @return A random integer between 0 and max
    template<typename T>
    T get_int(T max) {
        return get_int<T>(0, max);
    }

    /// Select a random element from a vector with uniform probability.
    /// @param elements The vector to select from
    /// @return A const reference to the selected element
    template<typename T>
    const T& random_choice(const std::vector<T>& elements) {
        auto idx = get_int<size_t>(0, elements.size() - 1);
        return elements[idx];
    }

    /// Select a random element from a vector with uniform probability.
    /// @param elements The vector to select from
    /// @return A mutable reference to the selected element
    template<typename T>
    T& random_choice(std::vector<T>& elements) {
        auto idx = get_int<size_t>(0, elements.size() - 1);
        return elements[idx];
    }

    /// Select a random element from an initializer list with uniform
    /// probability.
    /// @param choices The initializer list to select from
    /// @return A copy of the selected element
    template<typename T>
    T random_choice(std::initializer_list<T> choices) {
        auto idx = get_int<size_t>(0, choices.size() - 1);
        auto& choice = *(choices.begin() + idx);
        return std::move(choice);
    }

    /// Generate a random floating-point number of type T using uniform
    /// distribution over the range [0, 1).
    /// @return A random floating-point number in [0, 1)
    template<typename T>
    T get_real() {
        std::uniform_real_distribution<T> dist;
        return dist(gen_);
    }

    /// Generate a random floating-point number of type T uniformly
    /// distributed between min and max.
    /// @param min The minimum value
    /// @param max The maximum value
    /// @return A random floating-point number between min and max
    template<typename T>
    T get_real(T min, T max) {
        std::uniform_real_distribution<T> dist(min, max);
        return dist(gen_);
    }

    /// Generate a random floating-point number of type T uniformly
    /// distributed between 0 and max.
    /// @param max The maximum value
    /// @return A random floating-point number between 0 and max
    template<typename T>
    T get_real(T max) {
        std::uniform_real_distribution<T> dist(0, max);
        return dist(gen_);
    }

    /// Generate a vector containing all integers from min to max-1
    /// in random order using Fisher-Yates shuffle.
    /// @param min The minimum value (inclusive)
    /// @param max The maximum value (exclusive)
    /// @return A vector of shuffled integers from min to max-1
    template<typename T>
    std::vector<T> randomized_range(T min, T max) {
        std::vector<T> r(max - min);
        std::iota(r.begin(), r.end(), min);
        std::shuffle(r.begin(), r.end(), gen_);
        return r;
    }

    /// Generate a random string of alphanumeric characters of length n.
    /// @param n The length of the string to generate
    /// @return A random alphanumeric string
    ss::sstring gen_alphanum_string(size_t n);

    /// Returns the underlying random engine, for use with standard
    /// library distributions or algorithms that require a random engine.
    /// @return A reference to the underlying PCG64 engine
    engine_type& engine() { return gen_; }

private:
    engine_type gen_;
    seed_type initial_seed_;
};

/// Return a rng object which is seeded randomly regardless of
/// the default seeding policy.
/// @return A randomly seeded rng object
rng with_random_seed();

/// Get the global rng instance, creating and seeding it if necessary.
/// @return A reference to the global rng object
rng& global();

namespace internal {

enum class seeding_mode {
    random_seed = 1,
    fixed_seed = 2,
};

/// Get the global seeding mode
/// @return The current global seeding mode policy
seeding_mode default_seeding_policy();

/// Increment the seed generation which causes the subsequent call to
/// global() to reseed the global generator based on the default seed policy.
void increment_seed_generation();

static constexpr std::string_view chars
  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

} // namespace internal

/**
 * Random string generator. Total number of distinct values that may be
 * generated is unlimited (within all possible values of given size).
 * @param n The length of the string to generate
 * @return A random alphanumeric string
 */
ss::sstring gen_alphanum_string(size_t n);

inline constexpr size_t alphanum_max_distinct_strlen = 32;
/**
 * Random string generator that limits the maximum number of distinct
 * values that will be returned. That is, this function is a generator, which
 * creates members of a set of strings, one at a time. Each generated string has
 * maximum length `alphanum_max_distinct_strlen`. The total set of generated
 * strings will have a maximum cardinality of `max_cardinality`. See the unit
 * test `alphanum_max_distinct_generator` for an example.
 * @param max_cardinality The maximum number of distinct strings to generate
 * @return A random string from the limited set
 */
ss::sstring gen_alphanum_max_distinct(size_t max_cardinality);

/// Fill a buffer with random printable characters.
/// @param start Pointer to the start of the buffer
/// @param amount Number of bytes to fill
void fill_buffer_randomchars(char* start, size_t amount);

/// Generate a vector containing all integers from min to max-1
/// in random order using the global rng instance.
/// @param min The minimum value (inclusive)
/// @param max The maximum value (exclusive)
/// @return A vector of shuffled integers from min to max-1
template<typename T>
std::vector<T> randomized_range(T min, T max) {
    return global().randomized_range(min, max);
}

// Global forwarding functions: each of these just calls the corresponding
// method on the global() rng object.

/// Generate a random integer of type T using the global rng instance.
/// @return A random integer of type T
template<typename T>
T get_int() {
    return global().get_int<T>();
}

/// Generate a random integer using the global rng instance.
/// @param min The minimum value (inclusive)
/// @param max The maximum value (inclusive)
/// @return A random integer between min and max
template<typename T>
T get_int(T min, T max) {
    return global().get_int(min, max);
}

/// Generate a random integer using the global rng instance.
/// @param max The maximum value (inclusive)
/// @return A random integer between 0 and max
template<typename T>
T get_int(T max) {
    return global().get_int(max);
}

/// Select a random element from a vector using the global rng instance.
/// @param elements The vector to select from
/// @return A const reference to the selected element
template<typename T>
const T& random_choice(const std::vector<T>& elements) {
    return global().random_choice(elements);
}

/// Select a random element from a vector using the global rng instance.
/// @param elements The vector to select from
/// @return A mutable reference to the selected element
template<typename T>
T& random_choice(std::vector<T>& elements) {
    return global().random_choice(elements);
}

/// Select a random element from an initializer list using the global rng
/// instance.
/// @param choices The initializer list to select from
/// @return A copy of the selected element
template<typename T>
T random_choice(std::initializer_list<T> choices) {
    return global().random_choice(choices);
}

/// Generate a random floating-point number using the global rng
/// instance.
/// @return A random floating-point number in [0, 1)
template<typename T>
T get_real() {
    return global().get_real<T>();
}

/// Generate a random floating-point number using the global rng
/// instance.
/// @param min The minimum value
/// @param max The maximum value
/// @return A random floating-point number between min and max
template<typename T>
T get_real(T min, T max) {
    return global().get_real<T>(min, max);
}

/// Generate a random floating-point number using the global rng
/// instance.
/// @param max The maximum value
/// @return A random floating-point number between 0 and max
template<typename T>
T get_real(T max) {
    return global().get_real<T>(max);
}

} // namespace random_generators
