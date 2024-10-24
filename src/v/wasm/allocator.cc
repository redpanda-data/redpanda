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
#include "allocator.h"

#include "base/vassert.h"

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/print.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/later.hh>

#include <sys/mman.h>

#include <unistd.h>

namespace wasm {

heap_allocator::heap_allocator(config c)
  : _memset_chunk_size(c.memset_chunk_size) {
    size_t page_size = ::getpagesize();
    _size = ss::align_up(c.heap_memory_size, page_size);
    for (size_t i = 0; i < c.num_heaps; ++i) {
        auto buffer = ss::allocate_aligned_buffer<uint8_t>(_size, page_size);
        _memory_pool.push_back(
          async_zero_memory({std::move(buffer), _size}, _size));
    }
}

ss::future<> heap_allocator::stop() {
    auto pool = std::exchange(_memory_pool, {});
    co_await ss::when_all_succeed(pool.begin(), pool.end());
}

ss::future<std::optional<heap_memory>> heap_allocator::allocate(request req) {
    if (_memory_pool.empty()) {
        co_return std::nullopt;
    }
    if (_size < req.minimum || _size > req.maximum) {
        co_return std::nullopt;
    }
    ss::future<heap_memory> front = std::move(_memory_pool.front());
    _memory_pool.pop_front();
    co_return co_await std::move(front);
}

void heap_allocator::deallocate(heap_memory m, size_t used_amount) {
    _memory_pool.push_back(async_zero_memory(std::move(m), used_amount));
}

ss::future<heap_memory>
heap_allocator::async_zero_memory(heap_memory m, size_t used_amount) {
    uint8_t* data = m.data.get();
    size_t remaining = used_amount;
    while (true) {
        if (remaining <= _memset_chunk_size) {
            std::memset(data, 0, remaining);
            co_return m;
        }
        std::memset(data, 0, _memset_chunk_size);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        data += _memset_chunk_size;
        remaining -= _memset_chunk_size;
        co_await ss::coroutine::maybe_yield();
    }
}

size_t heap_allocator::max_size() const { return _size; }

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
    std::memset(mem.bounds().bottom, 0, mem.size());
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
