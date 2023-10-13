/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "wasm/allocator.h"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/print.hh>

#include <stdexcept>
#include <unistd.h>

namespace wasm {

heap_allocator::heap_allocator(config c) {
    size_t page_size = ::getpagesize();
    size_t alloc_size = ss::align_up(c.heap_memory_size, page_size);
    for (size_t i = 0; i < c.num_heaps; ++i) {
        auto buffer = ss::allocate_aligned_buffer<uint8_t>(
          alloc_size, page_size);
        _memory_pool.emplace_back(std::move(buffer), alloc_size);
    }
}

std::optional<heap_memory> heap_allocator::allocate(request req) {
    if (_memory_pool.empty()) {
        return std::nullopt;
    }
    size_t size = _memory_pool.front().size;
    if (size < req.minimum || size > req.maximum) {
        return std::nullopt;
    }
    heap_memory front = std::move(_memory_pool.front());
    _memory_pool.pop_front();
    return front;
}

void heap_allocator::deallocate(heap_memory m) {
    _memory_pool.push_back(std::move(m));
}

} // namespace wasm
