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
#pragma once

#include "base/seastarx.h"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <absl/container/btree_set.h>

namespace wasm {

/**
 * An owned bit of heap memory, which is aligned to page size and it's size is a
 * multiple of the page size.
 */
struct heap_memory {
    // NOLINTNEXTLINE(*-avoid-c-arrays)
    std::unique_ptr<uint8_t[], ss::free_deleter> data;
    size_t size;
};

// The allocator for "heap" memory within a Wasm VM.
//
// This "heap" memory is known as LinearMemory in WebAssembly parlance and must
// be a contiguous range of memory. Since these memories can grow beyond the
// maximum amount of memory we recommend allocating at once in Redpanda (due to
// fragmentation in long running processes), we allocate a pool at startup when
// there are copious amounts of larger contiguous chunks.
//
// Additionally memory must be page-aligned and a multiple of page size to
// enable performance optimizations within the Wasm VMs.
//
// Currently the allocation model requires a fixed sized heap at startup and
// then those are handed out and reused as VM instances spin up and down.
//
// Future work may allow variable sized heaps for VMs, but then we must deal
// with memory fragmentation and support spinning down VMs to defragment our
// pool of memory.
//
// Instance on every core.
class heap_allocator {
public:
    struct config {
        // The size of a single allocated heap memory.
        size_t heap_memory_size;
        // The total number of heaps allocated per core.
        size_t num_heaps;
        // The amount of memory we zero out at once.
        size_t memset_chunk_size;
    };

    explicit heap_allocator(config);

    /**
     * Stop this allocator, waiting for any asynchronous zero'ing of bytes to
     * finish.
     */
    ss::future<> stop();

    /**
     * A request of heap memory based on the following bounds.
     */
    struct request {
        size_t minimum;
        size_t maximum;
    };

    /**
     * Allocate heap memory by taking a memory instance from the pool, returns
     * std::nullopt if the memory request cannot be fulfilled either because the
     * request does not fix within our bounds or because all memory is currently
     * allocated.
     *
     * Memory returned from this method will be zero-filled.
     */
    ss::future<std::optional<heap_memory>> allocate(request);

    /**
     * Deallocate heap memory by returing a memory instance to the pool.
     *
     * used_amount is to zero out the used memory from the heap, this is
     * required so that if only a portion of a large memory space was used we
     * don't have to touch all the bytes. If the used_amount is over some
     * threshold then we make zero'ing out the bytes an asynchronous task.
     */
    void deallocate(heap_memory, size_t used_amount);

    /**
     * The maximum size of a heap_memory that can be allocated.
     */
    size_t max_size() const;

private:
    ss::future<heap_memory> async_zero_memory(heap_memory, size_t used_amount);

    size_t _memset_chunk_size;
    size_t _size;
    // We expect this list to be small, so override the chunk to be smaller too.
    static constexpr size_t items_per_chunk = 16;
    ss::chunked_fifo<ss::future<heap_memory>, items_per_chunk> _memory_pool;
};

/**
 * The useable (i.e. excluding guard pages) bounds of an allocated stack.
 */
struct stack_bounds {
    uint8_t* top;
    uint8_t* bottom;

    auto operator<=>(const stack_bounds&) const = default;
    friend std::ostream& operator<<(std::ostream&, const stack_bounds&);
};

/**
 * An owned bit of stack memory, which is aligned to page size and it's size is
 * a multiple of the page size.
 *
 * Note that there is a guard page of protected memory "below" the stack to
 * protect against stack overflow.
 */
class stack_memory {
    // NOLINTNEXTLINE(*-avoid-c-arrays)
    using allocated_memory = std::unique_ptr<uint8_t[], ss::free_deleter>;

public:
    stack_memory() = default;
    stack_memory(stack_bounds bounds, allocated_memory data);
    stack_memory(const stack_memory&) = delete;
    stack_memory& operator=(const stack_memory&) = delete;
    stack_memory(stack_memory&&) = default;
    stack_memory& operator=(stack_memory&&) = default;
    ~stack_memory();

    stack_bounds bounds() const;
    size_t size() const;

private:
    friend class stack_allocator;
    stack_bounds _bounds{};
    allocated_memory _data;
};

// The allocator for stack memory within a Wasm VM.
//
// We execute WebAssembly on a seperate stack because WebAssembly doesn't
// have a notion of "async" and we need to be able to both pause the VM and
// support host functions that need to execute asynchronously. We can pause the
// VM to let this async operations happen by switching from the WebAssembly
// stack back to the host stack to do other work.
//
// Stack memory must be page-aligned and a multiple of page size.
// Some architectures require this alignment for stacks. Additionally, we
// always allocate a guard page at the bottom, which is unprotected when memory
// is "deallocated". Lastly, memory returned from this allocator is always
// zero-filled as assumed by the Wasm VM.
//
// The allocator also supports the ability to query if the current stack being
// used has been allocated from this allocator and returns the bounds. This is
// used in tests to support ensuring that we won't ever overflow the stack by
// adjusting our host functions to run as if Wasm has taken up the maximum
// amount stack space the VM allows.
//
// There are penalties for allocating the guard page on these stacks - the
// kernel will have to break up transparent huge pages (THP) when just a single
// 4KiB page is allocated. To mitigate this we cache stacks that are allocated.
// Currently these are never freed. However there can still be THPs that are
// broken up due to these stacks being scattered around in memory. A future
// optimization may help with this by allocating larger chunks and protecting
// neighboring (small) pages so that it's less likely THPs need to be broken up.
//
// Instance on every core.
class stack_allocator {
public:
    struct config {
        // If true, enable tracking, otherwise `current_stack_bounds` always
        // returns `std::nullopt`.
        bool tracking_enabled;
    };

    explicit stack_allocator(config);

    /**
     * Allocate stack memory. This size will be allocated plus a single guard
     * page at the "bottom" of the stack.
     */
    stack_memory allocate(size_t size);

    /**
     * If tracking was enabled for this allocator.
     */
    bool tracking_enabled() const;

    /**
     * Return the stack bounds if the stack that contains the address has been
     * created by this allocator.
     *
     * Example usage:
     * ```
     * uint8_t* dummy = 0;
     * auto my_stack = stack_allocator.stack_bounds_for_address(&dummy);
     * // do something with my_stack
     * ```
     */
    std::optional<stack_bounds> stack_bounds_for_address(uint8_t*) const;

    /**
     * Deallocate stack memory.
     */
    void deallocate(stack_memory);

private:
    bool _tracking_enabled;
    size_t _page_size;
    absl::btree_set<stack_bounds> _live_stacks;
    // We expect this list to be small, so override the chunk to be smaller too.
    static constexpr size_t items_per_chunk = 16;
    ss::chunked_fifo<stack_memory, items_per_chunk> _memory_pool;
};

} // namespace wasm
