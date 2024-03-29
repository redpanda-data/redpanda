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
  : _min_alloc_threshold{min_alloc_threshold}
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

} // namespace compression
