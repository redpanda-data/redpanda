#pragma once

#include <pcg_random.hpp>
#include <random>

// dumb wrapper around pcg32
class fast_prng {
public:
    fast_prng() noexcept
      : _rng(pcg_extras::seed_seq_from<std::random_device>()) {}
    ~fast_prng() noexcept = default;
    fast_prng(const fast_prng&) = delete;
    fast_prng& operator=(const fast_prng&) = delete;
    fast_prng(fast_prng&& o) noexcept = default;
    fast_prng& operator=(fast_prng&& o) noexcept = default;

    inline uint32_t operator()() { return _rng(); }

private:
    pcg32 _rng;
};
