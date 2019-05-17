#pragma once

#include <random>
// lib
#include <pcg_random.hpp>
#include <smf/macros.h>

// dumb wrapper around pcg32
class fast_prng {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(fast_prng);
  fast_prng() : rng_(pcg_extras::seed_seq_from<std::random_device>()) {}
  fast_prng(fast_prng &&o) noexcept : rng_(std::move(o.rng_)) {}
  fast_prng &
  operator=(fast_prng &&o) noexcept {
    if (this != &o) {
      this->~fast_prng();
      new (this) fast_prng(std::move(o));
    }
    return *this;
  }
  inline uint32_t
  operator()() {
    return rng_();
  }

 private:
  pcg32 rng_;
};
