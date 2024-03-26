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

#include "utils/object_pool.h"

#include <seastar/core/aligned_buffer.hh>

#define LZ4F_STATIC_LINKING_ONLY

#include <lz4frame.h>
#include <memory>

extern "C" {

// Allocates buffers for decompression. Accepts `lz4_decompression_buffers` as
// the state pointer. The buffers must first have been reserved for use via
// `lz4_decompression_buffers::reserve_buffers`. Tracks the buffer to return
// using the allocation_state enum. May also be called for objects which will
// not be allocated out of the static pool, in which case it falls back to
// malloc.
void* allocate_decompression_buffer(void* state, size_t size);

// Manages returning the buffers used for decompression to the static pool. This
// function may also be called for objects not allocated out of the static pool,
// in which case it falls back to using the free function on the pointer.
void free_decompression_object(void* state, void* address);
}

namespace compression {

class lz4_decompression_buffers {
public:
    explicit lz4_decompression_buffers(size_t buffer_size);

    [[nodiscard]] bool has_reserved_buffers() const;

    // Handles releasing buffers using RAII, modelled after
    // `object_pool::scoped_object` without storing the actual reserved object.
    struct units {
        explicit units(lz4_decompression_buffers& parent)
          : _parent{parent} {}

        units(units&& other) noexcept
          : _parent{other._parent} {
            other._parent = std::nullopt;
        }

        units(const units&) = delete;
        units& operator=(const units&) = delete;
        units& operator=(units&&) = delete;

        ~units() {
            if (_parent.has_value()) {
                _parent.value().get().release_buffers();
            }
        }

        std::optional<std::reference_wrapper<lz4_decompression_buffers>>
          _parent;
    };

    // Reserves buffers for decompression by moving them out of the pool and
    // into the reserved buffers field. Returns a future which resolves when
    // wait for buffers is complete. The buffers are available until they are
    // released back into the pool. This method does not return buffers because
    // the call site of waiting for available buffers and the call site of using
    // them may be several function calls apart. Instead of returning buffers
    // here and expecting the caller to thread the reference through
    // intermediate functions, we allow the caller to reserve capacity here and
    // call the `buffers()` method later to access the buffers.
    ss::future<units> reserve_buffers();

    // Releases buffers from the available field back to the pool. The next
    // decompression operation will need to wait on the pool to get buffers for
    // use. Can be called multiple times safely.
    void release_buffers();

    // LZ4 decompression requires two buffers during a single decompression
    // operation. This struct carries the buffers and associated book-keeping
    // state of allocation.
    struct alloc_ctx {
        enum class allocation_state : uint8_t {
            no_buffers_allocated,
            input_buffer_allocated,
            output_buffer_allocated,
            both_buffers_allocated,
        };

        std::unique_ptr<char[], ss::free_deleter> input_buffer;
        std::unique_ptr<char[], ss::free_deleter> output_buffer;
        allocation_state state;

        [[nodiscard]] bool is_managed_address(const void* const address) const;
    };

    [[nodiscard]] alloc_ctx& buffers();

    [[nodiscard]] LZ4F_CustomMem custom_mem_alloc();

    [[nodiscard]] size_t min_allocation_size() const;

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
    // Buffers taken out of pool and placed into this field are available for an
    // imminent decompression.
    std::optional<alloc_ctx> _reserved_buffers;

    // Buffers in this pool must be reserved and moved to the _reserved_buffers
    // field before a decompression operation.
    object_pool<alloc_ctx> _decompression_buffers;

    stats _allocation_stats;
};

void init_lz4_decompression_buffers(size_t buffer_size);

lz4_decompression_buffers& lz4_decompression_buffers_instance();

std::ostream& operator<<(
  std::ostream&, lz4_decompression_buffers::alloc_ctx::allocation_state);

inline lz4_decompression_buffers::alloc_ctx::allocation_state operator|(
  lz4_decompression_buffers::alloc_ctx::allocation_state a,
  lz4_decompression_buffers::alloc_ctx::allocation_state b) {
    using t = std::underlying_type_t<
      lz4_decompression_buffers::alloc_ctx::allocation_state>;
    return lz4_decompression_buffers::alloc_ctx::allocation_state(t(a) | t(b));
}

inline void operator|=(
  lz4_decompression_buffers::alloc_ctx::allocation_state& a,
  lz4_decompression_buffers::alloc_ctx::allocation_state b) {
    a = (a | b);
}

inline lz4_decompression_buffers::alloc_ctx::allocation_state operator&(
  lz4_decompression_buffers::alloc_ctx::allocation_state a,
  lz4_decompression_buffers::alloc_ctx::allocation_state b) {
    using t = std::underlying_type_t<
      lz4_decompression_buffers::alloc_ctx::allocation_state>;
    return lz4_decompression_buffers::alloc_ctx::allocation_state(t(a) & t(b));
}

inline void operator&=(
  lz4_decompression_buffers::alloc_ctx::allocation_state& a,
  lz4_decompression_buffers::alloc_ctx::allocation_state b) {
    a = (a & b);
}

inline lz4_decompression_buffers::alloc_ctx::allocation_state
operator~(lz4_decompression_buffers::alloc_ctx::allocation_state a) {
    return lz4_decompression_buffers::alloc_ctx::allocation_state(
      ~std::underlying_type_t<
        lz4_decompression_buffers::alloc_ctx::allocation_state>(a));
}

} // namespace compression
