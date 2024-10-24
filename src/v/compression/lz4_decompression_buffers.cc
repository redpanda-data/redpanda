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

#include "compression/lz4_decompression_buffers.h"

#include "base/units.h"
#include "base/vassert.h"

#include <seastar/coroutine/all.hh>

namespace compression {

std::ostream& operator<<(
  std::ostream& os, lz4_decompression_buffers::alloc_ctx::allocation_state st) {
    switch (st) {
        using enum compression::lz4_decompression_buffers::alloc_ctx::
          allocation_state;
    case no_buffers_allocated:
        return os << "no buffers allocated";
    case input_buffer_allocated:
        return os << "input buffer allocated";
    case output_buffer_allocated:
        return os << "output buffer allocated";
    case both_buffers_allocated:
        return os << "both buffers allocated";
    }
}

lz4_decompression_buffers::lz4_decompression_buffers(
  size_t buffer_size, size_t min_alloc_threshold, bool disabled)
  : _buffer_size{buffer_size}
  , _min_alloc_threshold{min_alloc_threshold}
  , _disabled{disabled} {
    if (!_disabled) {
        _buffers = {
          .input_buffer = ss::allocate_aligned_buffer<char>(buffer_size, 8),
          .output_buffer = ss::allocate_aligned_buffer<char>(buffer_size, 8),
          .state = alloc_ctx::allocation_state::no_buffers_allocated,
        };
    }
}

bool lz4_decompression_buffers::alloc_ctx::is_managed_address(
  const void* const address) const {
    return address == input_buffer.get() || address == output_buffer.get();
}

lz4_decompression_buffers::alloc_ctx& lz4_decompression_buffers::buffers() {
    return _buffers;
}

size_t lz4_decompression_buffers::min_alloc_threshold() const {
    return _min_alloc_threshold;
}

LZ4F_CustomMem lz4_decompression_buffers::custom_mem_alloc() {
    // If custom allocation is disabled, setting all alloc functions to null
    // makes lz4 fall back to malloc, calloc and free.
    if (_disabled) {
        return {
          .customAlloc = nullptr,
          .customCalloc = nullptr,
          .customFree = nullptr,
          .opaqueState = nullptr};
    }

    return {
      .customAlloc = alloc_lz4_obj,
      .customCalloc = nullptr,
      .customFree = free_lz4_obj,
      .opaqueState = this};
}

static thread_local std::unique_ptr<lz4_decompression_buffers>
  _buffers_instance;

void init_lz4_decompression_buffers(
  size_t buffer_size, size_t min_alloc_threshold, bool prealloc_disabled) {
    if (!_buffers_instance) {
        _buffers_instance = std::make_unique<lz4_decompression_buffers>(
          buffer_size, min_alloc_threshold, prealloc_disabled);
    }
}

void reset_lz4_decompression_buffers() {
    if (_buffers_instance) {
        _buffers_instance.reset();
    }
}

lz4_decompression_buffers& lz4_decompression_buffers_instance() {
    if (unlikely(!_buffers_instance)) {
        init_lz4_decompression_buffers(
          lz4_decompression_buffers::bufsize,
          lz4_decompression_buffers::min_threshold);
    }

    return *_buffers_instance;
}

} // namespace compression

namespace {

using alloc_st
  = compression::lz4_decompression_buffers::alloc_ctx::allocation_state;
using t = std::underlying_type_t<alloc_st>;

alloc_st operator|(alloc_st a, alloc_st b) { return alloc_st(t(a) | t(b)); }

void operator|=(alloc_st& a, alloc_st b) { a = (a | b); }

alloc_st operator&(alloc_st a, alloc_st b) { return alloc_st(t(a) & t(b)); }

void operator&=(alloc_st& a, alloc_st b) { a = (a & b); }

alloc_st operator~(alloc_st a) { return alloc_st(~t(a)); }

} // namespace

// During a typical lz4 decompression operation the following LZ4F_malloc calls
// will be processed via this alloc function:
// 1. Allocation for the tmp input buffer: this can be a maximum of 4MiB + 4
// bytes
// 2. Allocation for the tmp output buffer: this can be a maximum of 4MiB +
// 128KiB
// These two calls will typically happen once per decompression context, and are
// preceded by calls to LZ4F_free to first free up the two buffers.
void* alloc_lz4_obj(void* state, size_t size) {
    auto* st = static_cast<compression::lz4_decompression_buffers*>(state);
    vassert(
      size <= st->buffer_size(),
      "Request to allocate {} bytes which is more than max buffer size "
      "available: {} bytes",
      size,
      st->buffer_size());

    if (size < st->min_alloc_threshold()) {
        st->pass_through_allocated();
        return malloc(size);
    }

    auto& bufs = st->buffers();

    switch (bufs.state) {
        using enum compression::lz4_decompression_buffers::alloc_ctx::
          allocation_state;
    case no_buffers_allocated:
        bufs.state |= input_buffer_allocated;
        st->allocated();
        return bufs.input_buffer.get();
    case input_buffer_allocated:
        bufs.state |= output_buffer_allocated;
        st->allocated();
        return bufs.output_buffer.get();
    case both_buffers_allocated:
    case output_buffer_allocated:
        vassert(
          false, "invalid allocation request when both buffers allocated");
    }
}

// During a decompression operation this function is called via the LZ4F_free
// wrapper. The function is typically called in the following sequence:
// 1. When freeing the decompression context:
//    a. free the tmp out buffer
//    b. free the tmp in buffer
//    c. free the decompression context
// 2. When initializing the decompression context, this function will be called
// on the two buffer addresses.
// In all cases we either pass the address straight through to `free()` or if
// the address is managed, we update the state. The state update ensures that
// the next decompression operation starts with the correct state (no buffers
// allocated)
void free_lz4_obj(void* state, void* address) {
    auto* st = static_cast<compression::lz4_decompression_buffers*>(state);

    auto& bufs = st->buffers();

    // If the address being freed does not match one of the static addresses we
    // manage, fall back to free. This can happen because:
    //
    // 1. LZ4 frees memory before performing each allocation, resulting in
    // interspersed calls to free/malloc where the freed address was not
    // allocated from our pool.
    //
    // 2. The allocation was not done via this allocator, eg for blocks
    // small enough that they should not be managed by custom allocator.
    //
    // In both cases these memory addresses will not match our managed buffers.
    if (!bufs.is_managed_address(address)) {
        st->pass_through_deallocated();
        free(address);
        return;
    }

    // Buffers are released by lz4 in the order: input buffer, output buffer,
    // decompression ctx. The first two calls update the state here. The third
    // call is passed through to free because we do not allocate memory for the
    // decompression ctx.
    switch (bufs.state) {
        using enum compression::lz4_decompression_buffers::alloc_ctx::
          allocation_state;
    case no_buffers_allocated:
    case input_buffer_allocated:
        vassert(
          false, "unexpected buffer state {} during deallocation", bufs.state);
    case output_buffer_allocated:
        st->deallocated();
        bufs.state &= (~output_buffer_allocated);
        return;
    case both_buffers_allocated:
        st->deallocated();
        bufs.state &= (~input_buffer_allocated);
        return;
    }
}
