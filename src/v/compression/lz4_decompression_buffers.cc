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

void* allocate_decompression_buffer(void* state, size_t size) {
    auto* st = static_cast<compression::lz4_decompression_buffers*>(state);
    if (size < st->min_allocation_size()) {
        st->pass_through_allocated();
        return malloc(size);
    }

    vassert(
      st->has_reserved_buffers(), "no available buffers in scratch space");

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

void free_decompression_object(void* state, void* address) {
    auto* st = static_cast<compression::lz4_decompression_buffers*>(state);

    // decompression buffers have been returned to the pool. The free request is
    // for the decompression c-struct. Fall back to free to release memory since
    // we do not manage it.
    if (!st->has_reserved_buffers()) {
        st->pass_through_deallocated();
        free(address);
        return;
    }

    auto& bufs = st->buffers();

    // If buffers are currently in use but the address being freed does not
    // match one of the static addresses we manage, fall back to free. This can
    // happen because:
    //
    // 1. LZ4 frees memory before performing each allocation, resulting in
    // interspersed calls to free/malloc where the freed address was not
    // allocated from our pool.
    //
    // 2. The allocation was not done via this allocator, eg for blocks
    // small enough that they should not be managed by custom allocator.
    //
    // Either of these memory addresses will not match our managed buffers.
    // Since the address is passed in as void* we do not have other means of
    // checking the type of the object being freed, we rely on address matching.
    if (!bufs.is_managed_address(address)) {
        st->pass_through_deallocated();
        free(address);
        return;
    }

    // Buffers are released by lz4 in the order: input buffer, output buffer,
    // ctx. The first call results in buffer state update. The second call
    // results in releasing the buffers back to the pool. The third and final
    // call should not reach here because we update the state, and is handled by
    // the check at the beginning of this function.
    switch (bufs.state) {
        using enum compression::lz4_decompression_buffers::alloc_ctx::
          allocation_state;
    case no_buffers_allocated:
    case output_buffer_allocated:
        vassert(
          false, "unexpected buffer state {} during deallocation", bufs.state);
    case input_buffer_allocated:
        st->deallocated();
        bufs.state &= (~input_buffer_allocated);
        vassert(
          bufs.state == no_buffers_allocated,
          "unexpected buffer state after freeing input buffer: {}",
          bufs.state);
        st->release_buffers();
        return;
    case both_buffers_allocated:
        st->deallocated();
        bufs.state &= (~output_buffer_allocated);
        vassert(
          bufs.state == input_buffer_allocated,
          "unexpected buffer state after freeing output buffer: {}",
          bufs.state);
        return;
    }
}

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

lz4_decompression_buffers::lz4_decompression_buffers(size_t buffer_size) {
    _decompression_buffers.release_object({
      .input_buffer = ss::allocate_aligned_buffer<char>(buffer_size, 8),
      .output_buffer = ss::allocate_aligned_buffer<char>(buffer_size, 8),
      .state = alloc_ctx::allocation_state::no_buffers_allocated,
    });
}

bool lz4_decompression_buffers::has_reserved_buffers() const {
    return _reserved_buffers.has_value();
}

ss::future<lz4_decompression_buffers::units>
lz4_decompression_buffers::reserve_buffers() {
    ss::abort_source as;
    auto buffers = co_await _decompression_buffers.allocate_object(as);
    _reserved_buffers.emplace(std::move(buffers));
    co_return units{*this};
}

void lz4_decompression_buffers::release_buffers() {
    if (!has_reserved_buffers()) {
        return;
    }

    _reserved_buffers->state
      = alloc_ctx::allocation_state::no_buffers_allocated;
    _decompression_buffers.release_object(std::move(_reserved_buffers.value()));
    _reserved_buffers.reset();
}

bool lz4_decompression_buffers::alloc_ctx::is_managed_address(
  const void* const address) const {
    return address == input_buffer.get() || address == output_buffer.get();
}

lz4_decompression_buffers::alloc_ctx& lz4_decompression_buffers::buffers() {
    vassert(
      has_reserved_buffers(), "attempt to access buffers when none reserved");
    return _reserved_buffers.value();
}

size_t lz4_decompression_buffers::min_allocation_size() const { return 4_MiB; }

LZ4F_CustomMem lz4_decompression_buffers::custom_mem_alloc() {
    return {
      .customAlloc = allocate_decompression_buffer,
      .customCalloc = nullptr,
      .customFree = free_decompression_object,
      .opaqueState = this};
}

static thread_local std::unique_ptr<lz4_decompression_buffers> _buffers;
static constexpr auto default_buf_size = 4_MiB + 128_KiB;

void init_lz4_decompression_buffers(size_t buffer_size) {
    if (!_buffers) {
        _buffers = std::make_unique<lz4_decompression_buffers>(buffer_size);
    }
}

lz4_decompression_buffers& lz4_decompression_buffers_instance() {
    if (unlikely(!_buffers)) {
        init_lz4_decompression_buffers(default_buf_size);
    }

    return *_buffers;
}

} // namespace compression
