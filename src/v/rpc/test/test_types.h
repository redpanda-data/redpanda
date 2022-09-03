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

#include "bytes/iobuf.h"

#include <seastar/core/sstring.hh>
#include <seastar/core/unaligned.hh>

#include <array>
#include <cstdint>
#include <vector>

struct pod {
    using rpc_serde_exempt = std::true_type;
    int16_t x = 1;
    int32_t y = 2;
    int64_t z = 3;
};

inline constexpr size_t pod_bytes() {
    return sizeof(int16_t)    // pod::x
           + sizeof(int32_t)  // pod::y
           + sizeof(int64_t); // pod::z
}

struct complex_custom {
    pod pit;
    iobuf oi;
};

inline constexpr size_t complex_custom_bytes() {
    return pod_bytes() + sizeof(int32_t); // blob of iobuf
}

struct pod_with_vector {
    pod pit;
    std::vector<int> v{1, 2, 3};
};

inline constexpr size_t pod_with_vector_bytes() {
    return pod_bytes()
           + (sizeof(int) * 3) // Size of each int times size of vector
           + sizeof(int32_t);  // blob for vector
}

struct pod_with_array {
    pod pit;
    std::array<int, 3> v{1, 2, 3};
};

inline constexpr size_t pod_with_arr_bytes() {
    return pod_bytes()
           + sizeof(int) * 3; // Size of each int times the number of elements
}

struct kv {
    ss::sstring k;
    iobuf v;
};
struct test_rpc_header {
    int32_t size = 42;
    uint64_t checksum = 66;
    std::vector<kv> hdrs;
};
