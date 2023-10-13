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

#include "gmock/gmock.h"
#include "wasm/allocator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>
#include <unistd.h>

namespace wasm {

TEST(HeapAllocatorParamsTest, SizeIsAligned) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size + 3,
      .num_heaps = 1,
    });
    auto mem = allocator.allocate(
      {.minimum = 0, .maximum = std::numeric_limits<size_t>::max()});
    ASSERT_TRUE(mem.has_value());
    EXPECT_EQ(mem->size, page_size * 2);
}

TEST(HeapAllocatorTest, CanAllocateOne) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
    });
    auto mem = allocator.allocate({.minimum = page_size, .maximum = page_size});
    ASSERT_TRUE(mem.has_value());
    EXPECT_EQ(mem->size, page_size);
}

TEST(HeapAllocatorTest, MustAllocateWithinBounds) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
    });
    // minimum too large
    auto mem = allocator.allocate(
      {.minimum = page_size * 2, .maximum = page_size * 3});
    EXPECT_FALSE(mem.has_value());
    // maximum too small
    mem = allocator.allocate(
      {.minimum = page_size / 2, .maximum = page_size - 1});
    EXPECT_FALSE(mem.has_value());
}

TEST(HeapAllocatorTest, Exhaustion) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 1,
    });
    auto mem = allocator.allocate({.minimum = page_size, .maximum = page_size});
    EXPECT_TRUE(mem.has_value());
    mem = allocator.allocate({.minimum = page_size, .maximum = page_size});
    EXPECT_FALSE(mem.has_value());
}

TEST(HeapAllocatorTest, CanReturnMemoryToThePool) {
    size_t page_size = ::getpagesize();
    heap_allocator allocator(heap_allocator::config{
      .heap_memory_size = page_size,
      .num_heaps = 3,
    });
    heap_allocator::request req{.minimum = page_size, .maximum = page_size};
    std::vector<heap_memory> allocated;
    for (int i = 0; i < 3; ++i) {
        auto mem = allocator.allocate(req);
        ASSERT_TRUE(mem.has_value());
        allocated.push_back(std::move(*mem));
    }
    auto mem = allocator.allocate(req);
    EXPECT_FALSE(mem.has_value());
    mem = std::move(allocated.back());
    allocated.pop_back();
    allocator.deallocate(std::move(*mem));
    mem = allocator.allocate(req);
    EXPECT_TRUE(mem.has_value());
    mem = allocator.allocate(req);
    EXPECT_FALSE(mem.has_value());
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

using ::testing::Optional;

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

} // namespace wasm
