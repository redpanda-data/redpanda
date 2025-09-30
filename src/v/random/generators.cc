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

#include "random/generators.h"

#include "absl/random/random.h"
#include "base/vassert.h"

#include <algorithm>
#include <atomic>
#include <random>

namespace random_generators {

using internal::seeding_mode;

using internal::chars;

using seed_type = rng::seed_type;

namespace {
constexpr seed_type fixed_seed = 1234567891u;

std::atomic<int64_t> seed_generation = 0;

// get the default seeding mode from the environment
const seeding_mode global_seeding_mode = [] {
    // REDPANDA_RNG_SEEDING_MODE overrides REDPANDA_RNG_SEEDING_MODE_DEFAULT
    // which overrides the default
    auto mode_cstr = std::getenv("REDPANDA_RNG_SEEDING_MODE");
    if (!mode_cstr) {
        mode_cstr = std::getenv("REDPANDA_RNG_SEEDING_MODE_DEFAULT");
    }
    if (!mode_cstr) {
        // default mode when nothing is specified
        return seeding_mode::random_seed;
    }

    std::string_view mode = mode_cstr;
    if (mode == "random") {
        return seeding_mode::random_seed;
    } else if (mode == "fixed") {
        return seeding_mode::fixed_seed;
    }
    vassert(false, "Invalid REDPANDA_RNG_SEEDING_MODE: {}", mode);
}();

seed_type random_seed() {
    // use a thread-local random device as random_device creation is
    // expensive (involves opening `/dev/urandom` and closing it again)
    static thread_local absl::BitGen bitgen;
    // generate a random seed with the full range of seed_type
    return absl::Uniform<seed_type>(bitgen);
}

// Given a seed, turn it into a seed-sequence, which is needed to properly
// seed a 128-bit state from 64-bits of input entropy
std::seed_seq seed_to_seq(seed_type seed) {
    static_assert(sizeof(seed) == 8, "this is written for 64-bit seeds");

    // create the seed_seq from high and low halves, as seed_seq
    // is always a sequence of 32-bit values
    return {seed, seed >> 32u};
}

// state to implement the global() rng object and its reseeding semantics
thread_local rng global_instance;
thread_local int64_t last_seed_gen = -1;

std::random_device::result_type get_initial_seed() {
    return global_seeding_mode == seeding_mode::fixed_seed ? fixed_seed
                                                           : random_seed();
}
} // namespace

namespace internal {
seeding_mode default_seeding_policy() { return global_seeding_mode; }
} // namespace internal

rng::rng()
  : rng(get_initial_seed()) {}

rng::rng(random_seed_tag)
  : rng(random_seed()) {}

rng::rng(seed_type seed)
  : gen_(seed_to_seq(seed))
  , initial_seed_(seed) {}

rng with_random_seed() { return rng{random_seed_tag{}}; }

rng& global() {
    // if a reseeding has been requested, apply it here
    if (last_seed_gen != seed_generation.load(std::memory_order_relaxed))
      [[unlikely]] {
        global_instance = rng{};
        last_seed_gen = seed_generation;
    }

    return global_instance;
}

void fill_buffer_randomchars(char* start, size_t amount) {
    static std::uniform_int_distribution<int> rand_fill('@', '~');
    memset(start, rand_fill(global().engine()), amount);
}

ss::sstring rng::gen_alphanum_string(size_t n) {
    // do not include \0
    static constexpr std::size_t max_index = chars.size() - 2;
    std::uniform_int_distribution<size_t> dist(0, max_index);
    auto s = ss::uninitialized_string(n);

    std::generate_n(s.begin(), n, [this, &dist] { return chars[dist(gen_)]; });
    return s;
}

ss::sstring gen_alphanum_string(size_t n) {
    return global().gen_alphanum_string(n);
}

ss::sstring gen_alphanum_max_distinct(size_t cardinality) {
    static constexpr std::size_t num_chars = chars.size() - 1;
    // everything is deterministic once you choose key_num
    auto key_num = get_int(cardinality - 1);
    auto next_index = key_num % num_chars;
    auto s = ss::uninitialized_string(alphanum_max_distinct_strlen);
    std::generate_n(s.begin(), alphanum_max_distinct_strlen, [&] {
        auto c = chars[next_index];
        next_index = (next_index + key_num) % num_chars;
        return c;
    });
    return s;
}

namespace internal {
void increment_seed_generation() { ++seed_generation; }
} // namespace internal

} // namespace random_generators
