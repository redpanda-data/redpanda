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
#include "base/seastarx.h"

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
    // This script computes the table immediately below, which are "ideal"
    // allocation sizes: rounded up to the next true size supported by
    // the seastar allocator. At <= 16K we apply the small pool indexing
    // logic, and above we use powers of 2 since we are in the buddy allocator
    // which only supports power-of-two sizes.
    //
    // We scale the target size by 1.47, i.e., 1.5 tweaked slightly to ensure
    // we hit 16K at the small<->big pool boundary.
    //
    // def lg2(size: int):
    //     return size.bit_length() - 1
    // def p(v: object):
    //     print(f"{v},")
    // s = 512
    // fb = 2 # idx_frac_bits
    // while s <= 2**14:
    //     # The size calculation below is doing idx_to_size(size_to_idx(s)),
    //     # i.e., figuring out which small point index the allocation falls in
    //     # then seeing what the size of that small pool is, i.e., the size of
    //     # the smallest small pool that can fix this allocation.
    //     # See the corresponding routines in src/core/memory.cc:
    //     #https://github.com/scylladb/seastar/blob/f840b860432e7e716e3cfc004690897b50dc122c/src/core/memory.cc#L478-L499
    //     idx = ((lg2(s) << fb) - ((1 << fb) - 1)) + ((s - 1) >> (lg2(s) - fb))
    //     p((((1 << fb) | (idx & ((1 << fb) - 1))) << (idx >> fb)) >> fb)
    //     s = int(s * 1.47)
    // for e in [15, 16, 17]:
    //     p(2**e)
    static constexpr auto alloc_table = std::to_array<uint32_t>(
      {512,
       768,
       1280,
       1792,
       2560,
       3584,
       6144,
       8192,
       12288,
       16384,
       32768,
       65536,
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
