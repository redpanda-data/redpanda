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
 * Thomas Wang 64 bit mix hash function
 */

inline uint64_t twang_mix64(uint64_t key) noexcept {
    key = (~key) + (key << 21); // key *= (1 << 21) - 1; key -= 1;
    key = key ^ (key >> 24);
    key = key + (key << 3) + (key << 8); // key *= 1 + (1 << 3) + (1 << 8)
    key = key ^ (key >> 14);
    key = key + (key << 2) + (key << 4); // key *= 1 + (1 << 2) + (1 << 4)
    key = key ^ (key >> 28);
    key = key + (key << 31); // key *= 1 + (1 << 31)
    return key;
}

/*
 * Inverse of twang_mix64
 *
 * Note that twang_unmix64 is significantly slower than twang_mix64.
 */

inline uint64_t twang_unmix64(uint64_t key) noexcept {
    // See the comments in jenkins_rev_unmix32 for an explanation as to how this
    // was generated
    key *= 4611686016279904257U;
    key ^= (key >> 28) ^ (key >> 56);
    key *= 14933078535860113213U;
    key ^= (key >> 14) ^ (key >> 28) ^ (key >> 42) ^ (key >> 56);
    key *= 15244667743933553977U;
    key ^= (key >> 24) ^ (key >> 48);
    key = (key + 1) * 9223367638806167551U;
    return key;
}

/*
 * Thomas Wang downscaling hash function
 */

inline uint32_t twang_32from64(uint64_t key) noexcept {
    key = (~key) + (key << 18);
    key = key ^ (key >> 31);
    key = key * 21;
    key = key ^ (key >> 11);
    key = key + (key << 6);
    key = key ^ (key >> 22);
    return (uint32_t)key;
}
