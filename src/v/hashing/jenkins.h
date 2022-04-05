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
#include <cstdint>
/*
 * Robert Jenkins' reversible 32 bit mix hash function
 */

inline uint32_t jenkins_rev_mix32(uint32_t key) noexcept {
    key += (key << 12); // key *= (1 + (1 << 12))
    key ^= (key >> 22);
    key += (key << 4); // key *= (1 + (1 << 4))
    key ^= (key >> 9);
    key += (key << 10); // key *= (1 + (1 << 10))
    key ^= (key >> 2);
    // key *= (1 + (1 << 7)) * (1 + (1 << 12))
    key += (key << 7);
    key += (key << 12);
    return key;
}

/*
 * Inverse of jenkins_rev_mix32
 *
 * Note that jenkinks_rev_unmix32 is significantly slower than
 * jenkins_rev_mix32.
 */

inline uint32_t jenkins_rev_unmix32(uint32_t key) noexcept {
    // These are the modular multiplicative inverses (in Z_2^32) of the
    // multiplication factors in jenkins_rev_mix32, in reverse order.  They were
    // computed using the Extended Euclidean algorithm, see
    // http://en.wikipedia.org/wiki/Modular_multiplicative_inverse
    key *= 2364026753U;

    // The inverse of a ^= (a >> n) is
    // b = a
    // for (int i = n; i < 32; i += n) {
    //   b ^= (a >> i);
    // }
    key ^= (key >> 2) ^ (key >> 4) ^ (key >> 6) ^ (key >> 8) ^ (key >> 10)
           ^ (key >> 12) ^ (key >> 14) ^ (key >> 16) ^ (key >> 18) ^ (key >> 20)
           ^ (key >> 22) ^ (key >> 24) ^ (key >> 26) ^ (key >> 28)
           ^ (key >> 30);
    key *= 3222273025U;
    key ^= (key >> 9) ^ (key >> 18) ^ (key >> 27);
    key *= 4042322161U;
    key ^= (key >> 22);
    key *= 16773121U;
    return key;
}
