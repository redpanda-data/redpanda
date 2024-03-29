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

#include "base/units.h"
#include "compression/internal/lz4_frame_compressor.h"
#include "compression/lz4_decompression_buffers.h"
#include "random/generators.h"

#include <gmock/gmock.h>

#include <lz4.h>

using enum compression::lz4_decompression_buffers::alloc_ctx::allocation_state;

TEST(AllocateBuffers, StateTransitions) {
    auto b = compression::lz4_decompression_buffers{4_MiB, 128_KiB + 1};
    const auto& buffers = b.buffers();
    EXPECT_EQ(buffers.state, no_buffers_allocated);
    auto allocator = b.custom_mem_alloc();

    auto* input = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    EXPECT_EQ(buffers.state, input_buffer_allocated);
    EXPECT_EQ(input, buffers.input_buffer.get());

    auto* output = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    EXPECT_EQ(buffers.state, both_buffers_allocated);
    EXPECT_EQ(output, buffers.output_buffer.get());

    allocator.customFree(allocator.opaqueState, input);
    EXPECT_EQ(buffers.state, output_buffer_allocated);

    allocator.customFree(allocator.opaqueState, output);
    EXPECT_EQ(buffers.state, no_buffers_allocated);
}

TEST(FallbackForSmallAllocs, CustomAllocator) {
    auto b = compression::lz4_decompression_buffers{4_MiB, 128_KiB + 1};
    auto allocator = b.custom_mem_alloc();
    auto* allocated = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold() - 1);
    EXPECT_NE(allocated, nullptr);
    allocator.customFree(allocator.opaqueState, allocated);
}

TEST(MixedAllocations, CustomAllocator) {
    auto b = compression::lz4_decompression_buffers{4_MiB, 128_KiB + 1};
    const auto& buffers = b.buffers();
    auto allocator = b.custom_mem_alloc();

    auto* input = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    EXPECT_EQ(input, buffers.input_buffer.get());
    EXPECT_EQ(buffers.state, input_buffer_allocated);

    auto* random_alloc = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold() - 1);
    EXPECT_FALSE(buffers.is_managed_address(random_alloc));
    EXPECT_EQ(buffers.state, input_buffer_allocated);

    auto* output = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    EXPECT_EQ(output, buffers.output_buffer.get());
    EXPECT_EQ(buffers.state, both_buffers_allocated);

    allocator.customFree(allocator.opaqueState, input);
    EXPECT_EQ(buffers.state, output_buffer_allocated);

    allocator.customFree(allocator.opaqueState, random_alloc);
    EXPECT_EQ(buffers.state, output_buffer_allocated);

    allocator.customFree(allocator.opaqueState, output);
    EXPECT_EQ(buffers.state, no_buffers_allocated);
}

TEST(MaxBufSizeDeathTest, CustomAllocator) {
    auto b = compression::lz4_decompression_buffers{4_MiB, 128_KiB + 1};
    auto allocator = b.custom_mem_alloc();
    ASSERT_DEATH(
      { allocator.customAlloc(allocator.opaqueState, b.buffer_size() + 1); },
      "Request to allocate 4194305 bytes which is more than max buffer size "
      "available: 4194304 bytes");
}

TEST(CustomAllocDisabled, Configuration) {
    compression::reset_lz4_decompression_buffers();
    compression::init_lz4_decompression_buffers(4_MiB, 128_KiB + 1, true);
    const auto data = random_generators::gen_alphanum_string(512);
    iobuf input;
    input.append(data.data(), data.size());

    using compression::internal::lz4_frame_compressor;
    auto& instance = compression::lz4_decompression_buffers_instance();
    auto compressed = lz4_frame_compressor::compress(input);
    auto uncompressed = lz4_frame_compressor::uncompress(compressed);
    auto stats = instance.allocation_stats();
    EXPECT_EQ(stats.allocs, 0);
    EXPECT_EQ(stats.deallocs, 0);
    EXPECT_EQ(stats.pass_through_allocs, 0);
    EXPECT_EQ(stats.pass_through_deallocs, 0);
    instance.reset_stats();
}
