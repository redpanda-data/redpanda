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

#include <fmt/ostream.h>

#include <algorithm>
#include <atomic>
#include <random>
#include <sstream>

namespace random_generators {

using internal::seeding_mode;

using internal::chars;

using seed_type = rng::seed_type;

namespace internal {
// get the default seeding mode from the environment
seeding_mode default_seeding_policy() {
    static const seeding_mode global_seeding_mode = [] {
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
        vunreachable("Invalid REDPANDA_RNG_SEEDING_MODE: {}", mode);
    }();

    return global_seeding_mode;
}

} // namespace internal

namespace {
constexpr seed_type fixed_seed = 0xDEADBEEF;

std::atomic<int64_t> seed_generation = 0;

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
struct global_state_state {
    rng global_instance;
    int64_t last_seed_gen = -1;
    // the number of times global() has been called, since process start
    size_t access_count = 0;
    // the value of access_count at the last reseeding event
    size_t access_count_at_reseed = 0;
};

static thread_local global_state_state global_state;

seed_type get_initial_seed() {
    return internal::default_seeding_policy() == seeding_mode::fixed_seed
             ? fixed_seed
             : random_seed();
}
} // namespace

rng::rng() noexcept
  : rng(get_initial_seed()) {}

rng::rng(random_seed_tag) noexcept
  : rng(random_seed()) {}

rng::rng(seed_type seed) noexcept
  : gen_(seed_to_seq(seed))
  , initial_seed_(seed) {}

rng with_random_seed() {
    return rng{random_seed_tag{}};
}

fmt::iterator rng::format_to(fmt::iterator it) const {
    it = fmt::format_to(
      it, "rng{{initial_seed: {:08X}, state: ", initial_seed_);

    // the only way to get the internal pcg64 state is to parse it out of the
    // ostream<< output for the engine
    auto gen_state = fmt::format("{}", fmt_streamed(gen_));
    std::stringstream ss(gen_state);
    std::vector<uint64_t> state_vector;
    uint64_t temp{};

    // Use the extraction operator >> to read numbers one by one
    while (ss >> temp) {
        state_vector.emplace_back(temp);
    }

    if (state_vector.size() == 6) {
        // only the last two numbers are the variable engine state, the first 4
        // are fixed
        it = fmt::format_to(
          it, "[{:016X}, {:016X}]}}", state_vector[4], state_vector[5]);
    } else {
        // unexpected format, just print the raw output
        it = fmt::format_to(it, " (raw) {}}}", gen_state);
    }

    return it;
}

rng& global() {
    auto& gs = global_state;
    // if a reseeding has been requested, apply it here
    if (gs.last_seed_gen != seed_generation.load(std::memory_order_relaxed))
      [[unlikely]] {
        gs.global_instance = rng{};
        gs.last_seed_gen = seed_generation;
        gs.access_count_at_reseed = gs.access_count;
    }

    ++gs.access_count;

    return gs.global_instance;
}

ss::sstring global_state_string() {
    // avoid global() here to avoid incrementing access_count
    return fmt::format(
      "global rng state: accesses: {}, accesses since reseed: {}, rng: {}",
      global_state.access_count,
      global_state.access_count - global_state.access_count_at_reseed,
      global_state.global_instance);
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
