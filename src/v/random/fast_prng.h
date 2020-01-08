#pragma once

#include <pcg_random.hpp>
#include <random>

// dumb wrapper around pcg32
class fast_prng {
public:
    fast_prng()
      : _rng(pcg_extras::seed_seq_from<std::random_device>()) {}
    fast_prng(fast_prng&& o) noexcept
      : _rng(o._rng) {}
    fast_prng& operator=(fast_prng&& o) noexcept {
        if (this != &o) {
            this->~fast_prng();
            new (this) fast_prng(std::move(o));
        }
        return *this;
    }
    inline uint32_t operator()() { return _rng(); }

private:
    pcg32 _rng;
};
