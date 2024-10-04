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

/// google's jump consistent hash
/// https://arxiv.org/pdf/1406.2294.pdf
constexpr uint32_t jump_consistent_hash(uint64_t key, uint32_t num_buckets) {
    int64_t b = -1, j = 0;
    while (j < num_buckets) {
        b = j;
        key = key * 2862933555777941757ULL + 1;
        j = (b + 1)
            * (static_cast<double>(1LL << 31) / static_cast<double>((key >> 33) + 1));
    }
    return static_cast<uint32_t>(b);
}
