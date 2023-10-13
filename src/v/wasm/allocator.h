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

#include "seastarx.h"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/util/optimized_optional.hh>

#include <type_traits>

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
    };

    explicit heap_allocator(config);

    /**
     * A request of heap memory based on the following bounds.
     */
    struct request {
        size_t minimum;
        size_t maximum;
    };

    /**
     * Allocate heap memory by taking a memory instance from the pool.
     */
    std::optional<heap_memory> allocate(request);

    /**
     * Deallocate heap memory by returing a memory instance to the pool.
     */
    void deallocate(heap_memory);

private:
    // We expect this list to be small, so override the chunk to be smaller too.
    static constexpr size_t items_per_chunk = 16;
    ss::chunked_fifo<heap_memory, items_per_chunk> _memory_pool;
};

} // namespace wasm
