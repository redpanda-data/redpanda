#pragma once

#include <absl/random/internal/pcg_engine.h>
#include <absl/random/seed_sequences.h>

#include <random>

// dumb wrapper around pcg32
class fast_prng {
public:
    fast_prng() noexcept
      : _rng(absl::MakeSeedSeq()) {}
    ~fast_prng() noexcept = default;
    fast_prng(const fast_prng&) = delete;
    fast_prng& operator=(const fast_prng&) = delete;
    fast_prng(fast_prng&& o) noexcept = default;
    fast_prng& operator=(fast_prng&& o) noexcept = default;

    inline uint32_t operator()() { return _rng(); }

private:
    absl::random_internal::pcg32_2018_engine _rng;
};
