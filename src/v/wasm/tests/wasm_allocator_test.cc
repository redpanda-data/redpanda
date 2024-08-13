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

#include "base/units.h"
#include "gmock/gmock.h"
#include "wasm/allocator.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <span>
#include <stdexcept>
#include <unistd.h>

namespace wasm {

constexpr static auto default_memset_chunk_size = 10_MiB;

using ::testing::Optional;

TEST(HeapAllocatorParamsTest, SizeIsAligned) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size + 3,
      .num_heaps = 1,
      .memset_chunk_size = default_memset_chunk_size,
    });
    auto mem = allocator
                 .allocate(
                   {.minimum = 0,
                    .maximum = std::numeric_limits<size_t>::max()})
                 .get();
    ASSERT_TRUE(mem.has_value());
    EXPECT_EQ(mem->size, page_size * 2);
}

TEST(HeapAllocatorTest, CanAllocateOne) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
      .memset_chunk_size = default_memset_chunk_size,
    });
    auto mem
      = allocator.allocate({.minimum = page_size, .maximum = page_size}).get();
    ASSERT_TRUE(mem.has_value());
    EXPECT_EQ(mem->size, page_size);
}

TEST(HeapAllocatorTest, MustAllocateWithinBounds) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
      .memset_chunk_size = default_memset_chunk_size,
    });
    // minimum too large
    auto mem = allocator
                 .allocate({.minimum = page_size * 2, .maximum = page_size * 3})
                 .get();
    EXPECT_FALSE(mem.has_value());
    // maximum too small
    mem = allocator
            .allocate({.minimum = page_size / 2, .maximum = page_size - 1})
            .get();
    EXPECT_FALSE(mem.has_value());
}

TEST(HeapAllocatorTest, Exhaustion) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
      .memset_chunk_size = default_memset_chunk_size,
    });
    auto mem
      = allocator.allocate({.minimum = page_size, .maximum = page_size}).get();
    EXPECT_TRUE(mem.has_value());
    mem
      = allocator.allocate({.minimum = page_size, .maximum = page_size}).get();
    EXPECT_FALSE(mem.has_value());
}

TEST(HeapAllocatorTest, CanReturnMemoryToThePool) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 3,
      .memset_chunk_size = default_memset_chunk_size,
    });
    heap_allocator::request req{.minimum = page_size, .maximum = page_size};
    std::vector<heap_memory> allocated;
    for (int i = 0; i < 3; ++i) {
        auto mem = allocator.allocate(req).get();
        ASSERT_TRUE(mem.has_value());
        allocated.push_back(std::move(*mem));
    }
    auto mem = allocator.allocate(req).get();
    EXPECT_FALSE(mem.has_value());
    mem = std::move(allocated.back());
    allocated.pop_back();
    allocator.deallocate(std::move(*mem), /*used_amount=*/0);
    mem = allocator.allocate(req).get();
    EXPECT_TRUE(mem.has_value());
    mem = allocator.allocate(req).get();
    EXPECT_FALSE(mem.has_value());
}

// We want to test a specific scenario where the deallocation happens
// asynchronously, however, release mode continuations can be "inlined" into the
// current executing task, so we can't enforce the scenario we want to test, so
// this test only runs in debug mode, which forces as many scheduling points as
// possible and the zeroing task always happens asynchronously.
#ifdef SEASTAR_DEBUG

using ::testing::_;

TEST(HeapAllocatorTest, AsyncDeallocationOnlyOneAwakened) {
    size_t page_size = ::getpagesize();
    // force deallocations to be asynchronous.
    size_t test_chunk_size = page_size / 4;
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 2,
      .memset_chunk_size = test_chunk_size,
    });
    heap_allocator::request req{.minimum = page_size, .maximum = page_size};
    // Start on deallocation in the background.
    allocator.deallocate(allocator.allocate(req).get().value(), page_size);
    // Start another deallocation in the background so there is no memory left.
    allocator.deallocate(allocator.allocate(req).get().value(), page_size);
    auto waiter1 = allocator.allocate(req);
    auto waiter2 = allocator.allocate(req);
    // There should not be memory available, so both our requests for
    // memory should be waiting.
    EXPECT_FALSE(waiter1.available());
    EXPECT_FALSE(waiter2.available());
    // waiter1 should be notified first there is memory available, but waiter2
    // should not be notified yet as there is a deallocation in flight.
    // waiter2 may or may not be ready depending on task execution order (which
    // is randomized in debug mode), so we cannot assert that it's not
    // completed.
    EXPECT_THAT(waiter1.get(), Optional(_));
    // The second waiter completes and we can allocate memory
    EXPECT_THAT(waiter2.get(), Optional(_));
}

TEST(HeapAllocatorTest, AsyncDeallocationNotEnoughMemory) {
    size_t page_size = ::getpagesize();
    // force deallocations to be asynchronous.
    size_t test_chunk_size = page_size / 4;
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
      .memset_chunk_size = test_chunk_size,
    });
    heap_allocator::request req{.minimum = page_size, .maximum = page_size};
    allocator.deallocate(allocator.allocate(req).get().value(), page_size);

    // The first request succeeded and is waiting for the deallocation to finish
    auto waiter1 = allocator.allocate(req);
    EXPECT_FALSE(waiter1.available());

    // Future requests fail immediately, no memory is available
    auto waiter2 = allocator.allocate(req);
    EXPECT_TRUE(waiter2.available());

    EXPECT_THAT(waiter1.get(), Optional(_));
    EXPECT_EQ(waiter2.get(), std::nullopt);
}
#endif

MATCHER(HeapIsZeroed, "is zeroed") {
    std::span<uint8_t> d = {arg.data.get(), arg.size};
    for (auto e : d) {
        if (e != 0) {
            return false;
        }
    }
    return true;
}

TEST(HeapAllocatorTest, MemoryIsZeroFilled) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
      .memset_chunk_size = default_memset_chunk_size,
    });
    heap_allocator::request req{.minimum = page_size, .maximum = page_size};
    auto allocated = allocator.allocate(req).get();
    ASSERT_TRUE(allocated.has_value());
    EXPECT_THAT(allocated, Optional(HeapIsZeroed()));
    std::fill_n(allocated->data.get(), 4, 1);
    allocator.deallocate(*std::move(allocated), 4);

    allocated = allocator.allocate(req).get();
    ASSERT_TRUE(allocated.has_value());
    EXPECT_THAT(allocated, Optional(HeapIsZeroed()));
}

TEST(StackAllocatorParamsTest, TrackingCanBeEnabled) {
    stack_allocator allocator(stack_allocator::config{
      .tracking_enabled = true,
    });
    EXPECT_TRUE(allocator.tracking_enabled());
    allocator = stack_allocator(stack_allocator::config{
      .tracking_enabled = false,
    });
    EXPECT_FALSE(allocator.tracking_enabled());
}

TEST(StackAllocatorTest, CanAllocateOne) {
    stack_allocator allocator(stack_allocator::config{
      .tracking_enabled = true,
    });
    size_t page_size = ::getpagesize();
    auto stack = allocator.allocate(page_size * 4);
    EXPECT_EQ(stack.size(), page_size * 4);
    EXPECT_EQ(stack.bounds().top - stack.bounds().bottom, page_size * 4);
}

TEST(StackAllocatorTest, CanLookupMemory) {
    stack_allocator allocator(stack_allocator::config{
      .tracking_enabled = true,
    });
    size_t page_size = ::getpagesize();
    stack_memory stack = allocator.allocate(page_size * 4);
    stack_bounds bounds = stack.bounds();

    // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    EXPECT_EQ(
      allocator.stack_bounds_for_address(bounds.bottom - 1), std::nullopt);
    EXPECT_THAT(
      allocator.stack_bounds_for_address(bounds.bottom), Optional(bounds));
    EXPECT_THAT(
      allocator.stack_bounds_for_address(bounds.bottom + 1), Optional(bounds));
    EXPECT_THAT(
      allocator.stack_bounds_for_address(bounds.top - 1), Optional(bounds));
    EXPECT_THAT(
      allocator.stack_bounds_for_address(bounds.top), Optional(bounds));
    EXPECT_EQ(allocator.stack_bounds_for_address(bounds.top + 1), std::nullopt);

    allocator.deallocate(std::move(stack));

    EXPECT_EQ(allocator.stack_bounds_for_address(bounds.top), std::nullopt);
    EXPECT_EQ(allocator.stack_bounds_for_address(bounds.top - 1), std::nullopt);
    EXPECT_EQ(allocator.stack_bounds_for_address(bounds.bottom), std::nullopt);
    EXPECT_EQ(
      allocator.stack_bounds_for_address(bounds.bottom + 1), std::nullopt);
    // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

TEST(StackAllocatorTest, CanReturnMemoryToThePool) {
    stack_allocator allocator(stack_allocator::config{
      .tracking_enabled = true,
    });
    size_t page_size = ::getpagesize();
    auto stack = allocator.allocate(page_size * 4);
    auto bounds = stack.bounds();
    allocator.deallocate(std::move(stack));
    // Getting that stack back out of the allocator will reuse the previous
    // stack.
    stack = allocator.allocate(page_size * 4);
    EXPECT_EQ(stack.bounds(), bounds);
}

MATCHER(StackIsZeroed, "is zeroed") {
    std::span<uint8_t> d = {arg.bounds().bottom, arg.size()};
    for (auto e : d) {
        if (e != 0) {
            return false;
        }
    }
    return true;
}

TEST(StackAllocatorTest, MemoryIsZeroFilled) {
    stack_allocator allocator(stack_allocator::config{
      .tracking_enabled = true,
    });
    size_t page_size = ::getpagesize();
    auto stack = allocator.allocate(page_size * 4);
    EXPECT_THAT(stack, StackIsZeroed());
    std::fill_n(stack.bounds().bottom, 4, 1);
    allocator.deallocate(std::move(stack));
    stack = allocator.allocate(page_size * 4);
    EXPECT_THAT(stack, StackIsZeroed());
}

} // namespace wasm
