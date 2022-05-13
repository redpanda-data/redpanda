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
#include "seastarx.h"

#include <seastar/core/bitops.hh>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

namespace details {
class io_allocation_size {
public:
    static constexpr size_t max_chunk_size = 128 * 1024;
    static constexpr size_t default_chunk_size = 512;

    // the largest size handled by seastar's small object pool
    static constexpr size_t ss_max_small_allocation = 16384;

public:
    // >>> x=512
    // >>> while x < int((1024*128)):
    // ...     print(x)
    // ...     x=int(((x*3)+1)/2)
    // ...     x=int(min(1024*128,x))
    // print(1024*128)
    static constexpr auto alloc_table = std::to_array<uint32_t>(
      // computed from a python script above
      {512,
       768,
       1152,
       1728,
       2592,
       3888,
       5832,
       8748,
       13122,
       19683,
       29525,
       44288,
       66432,
       99648,
       131072});
    static size_t next_allocation_size(size_t data_size);

    // Pick next allocation size for when the total remaining data size is
    // known, e.g. in an iobuf copy operation.
    // - If the size falls into the range of seastar's small allocator, allow a
    //   full allocation.
    // - Otherwise, allocate on lower bound power of 2 size which aligns with a
    //   span bucket in seastar's large allocation pool.
    // - Clamp the resulting pow2 size to max_chunk_size
    static size_t ss_next_allocation_size(size_t size) {
        if (size <= ss_max_small_allocation) {
            return size;
        }
        return std::min(
          static_cast<size_t>(1) << ss::log2floor(size), max_chunk_size);
    }
};

// Pick next allocation size when the ultimate size of the buffer is
// not known, e.g. for append/reserve operations on an iobuf.
// - try to not exceed max_chunk_size
// - must be enough for data_size
// - uses folly::vector of 1.5 growth without using double conversions
inline size_t io_allocation_size::next_allocation_size(size_t data_size) {
    // size_t next_size = ((_next_alloc_sz * 3) + 1) / 2;
    if (data_size > alloc_table.back()) {
        return alloc_table.back();
    }
    for (auto x : alloc_table) {
        // should be <, because we want forward progress in the allocator
        // chunks. so if you call the allocator with 512, you'll get 768, and if
        // you call the allocator with 768, you'll get 1152, and so on.
        if (data_size < x) {
            return x;
        }
    }
    return alloc_table.back();
}

} // namespace details
