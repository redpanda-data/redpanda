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

#include "vassert.h"
#include "vlog.h"
#include "wasm/logger.h"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/print.hh>

#include <sys/mman.h>

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

stack_memory::stack_memory(stack_bounds bounds, allocated_memory data)
  : _bounds(bounds)
  , _data(std::move(data)) {}

stack_memory::~stack_memory() {
    if (!_data) {
        // This can happen if the memory was moved.
        return;
    }
    int r = ::mprotect(
      _data.get(), _bounds.bottom - _data.get(), PROT_READ | PROT_WRITE);
    vassert(r == 0, "stack memory must be able to unprotect on destruction");
}

size_t stack_memory::size() const { return _bounds.top - _bounds.bottom; }

stack_bounds stack_memory::bounds() const { return _bounds; }

stack_allocator::stack_allocator(config c)
  : _tracking_enabled(c.tracking_enabled)
  , _page_size(::getpagesize()) {}

stack_memory stack_allocator::allocate(size_t size) {
    size = ss::align_up(size, _page_size);
    stack_memory mem;
    // Reuse a page
    if (!_memory_pool.empty() && _memory_pool.front().size() == size) {
        mem = std::move(_memory_pool.front());
        _memory_pool.pop_front();
    } else {
        // Create a stack with a guard page and aligned to a page.
        auto buffer = ss::allocate_aligned_buffer<uint8_t>(
          size + _page_size, _page_size);
        // Protect the guard page by making it read only.
        ::mprotect(buffer.get(), _page_size, PROT_READ);
        uint8_t* bottom = buffer.get() + _page_size;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        stack_bounds bounds{.top = bottom + size, .bottom = bottom};
        mem = {bounds, std::move(buffer)};
    }
    if (_tracking_enabled) {
        _live_stacks.emplace(mem.bounds());
    }
    return mem;
}

bool stack_allocator::tracking_enabled() const { return _tracking_enabled; }

std::optional<stack_bounds>
stack_allocator::stack_bounds_for_address(uint8_t* address) const {
    auto it = _live_stacks.lower_bound({.top = address, .bottom = nullptr});
    if (it == _live_stacks.end() || address < it->bottom) {
        return std::nullopt;
    }
    return *it;
}

void stack_allocator::deallocate(stack_memory mem) {
    if (_tracking_enabled) {
        _live_stacks.erase(mem.bounds());
    }
    // Return this stack back to the pool so that it can be reused.
    _memory_pool.push_back(std::move(mem));
}

std::ostream& operator<<(std::ostream& os, const stack_bounds& bounds) {
    return os << ss::format(
             "{{.top = {}, .bottom = {}}}",
             fmt::ptr(bounds.top),
             fmt::ptr(bounds.bottom));
}

} // namespace wasm
