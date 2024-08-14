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

#include <absl/random/internal/pcg_engine.h>
#include <absl/random/seed_sequences.h>

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

inline uint32_t fast_prng_source() {
    thread_local static fast_prng source;
    return source();
}
