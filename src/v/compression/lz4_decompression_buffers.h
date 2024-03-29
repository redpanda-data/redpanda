/*
 * Copyright 2024 Redpanda Data, Inc.
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

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/semaphore.hh>

#define LZ4F_STATIC_LINKING_ONLY

#include <lz4frame.h>

namespace compression {

class lz4_decompression_buffers {
public:
    explicit lz4_decompression_buffers(
      size_t buffer_size, size_t min_alloc_threshold, bool disabled = false);

    // LZ4 decompression requires two buffers during a single decompression
    // operation. This struct carries the buffers and associated book-keeping
    // state of allocation.
    struct alloc_ctx {
        // A typical transition cycle for this set of buffers is:
        // no_buffers_allocated -> input_buffer_allocated ->
        // output_buffer_allocated -> both_buffers_allocated
        // During deallocation/free the reverse states are expected.
        enum class allocation_state : uint8_t {
            // No buffers have been allocated to the LZ4 decompression routine.
            // The buffers are effectively not in use.
            no_buffers_allocated,
            // The input buffer has been allocated out to LZ4 decompression
            // routine.
            input_buffer_allocated,
            // The output buffer has also been allocated. Note that output
            // buffer will never be allocated alone.
            output_buffer_allocated,
            // Both buffers are allocated to decompression routine.
            both_buffers_allocated,
        };

        std::unique_ptr<char[], ss::free_deleter> input_buffer;
        std::unique_ptr<char[], ss::free_deleter> output_buffer;
        allocation_state state;

        // Checks if the address belongs to one of the two managed buffers. This
        // address check is used when freeing an address. If the address is
        // not managed by this context, then we fall back to `free()`.
        [[nodiscard]] bool is_managed_address(const void* const address) const;
    };

    // Returns a reference to allocated buffer pair. The buffers must have been
    // reserved before this call.
    [[nodiscard]] alloc_ctx& buffers();

    // Returns the minimum allocation threshold, allocation requests below this
    // size are passed through to `malloc()`.
    [[nodiscard]] size_t min_alloc_threshold() const;

    struct stats {
        size_t allocs{0};
        size_t deallocs{0};
        size_t pass_through_allocs{0};
        size_t pass_through_deallocs{0};
    };

    void allocated() { _allocation_stats.allocs += 1; }

    void deallocated() { _allocation_stats.deallocs += 1; }

    void pass_through_allocated() {
        _allocation_stats.pass_through_allocs += 1;
    }

    void pass_through_deallocated() {
        _allocation_stats.pass_through_deallocs += 1;
    }

    stats allocation_stats() const { return _allocation_stats; }

    void reset_stats() { _allocation_stats = {}; }

private:
    size_t _min_alloc_threshold;
    bool _disabled{false};

    alloc_ctx _buffers;
    stats _allocation_stats;
};

std::ostream& operator<<(
  std::ostream&, lz4_decompression_buffers::alloc_ctx::allocation_state);

} // namespace compression
