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
#include "bytes/iobuf.h"
#include "compression/compression.h"
#include "compression/internal/lz4_frame_compressor.h"
#include "compression/lz4_decompression_buffers.h"
#include "model/compression.h"
#include "random/generators.h"

#include <gmock/gmock.h>

#include <lz4.h>

using enum compression::lz4_decompression_buffers::alloc_ctx::allocation_state;

TEST(ReserveAndRelease, StateTransitions) {
    auto b = compression::lz4_decompression_buffers{4_MiB};
    EXPECT_FALSE(b.has_reserved_buffers());
    auto units = b.reserve_buffers().get();
    EXPECT_TRUE(b.has_reserved_buffers());
    b.release_buffers();
    EXPECT_FALSE(b.has_reserved_buffers());
}

TEST(RAIIRelease, StateTransitions) {
    auto b = compression::lz4_decompression_buffers{4_MiB};
    EXPECT_FALSE(b.has_reserved_buffers());
    {
        auto units = b.reserve_buffers().get();
        EXPECT_TRUE(b.has_reserved_buffers());
    }
    EXPECT_FALSE(b.has_reserved_buffers());
}

TEST(AccessWithoutReserveDeathTest, StateTransitions) {
    auto b = compression::lz4_decompression_buffers{4_MiB};
    ASSERT_DEATH(
      { [[maybe_unused]] auto& buffers = b.buffers(); },
      "attempt to access buffers when none reserved");
}

TEST(AllocateBuffers, StateTransitions) {
    auto b = compression::lz4_decompression_buffers{4_MiB};
    auto units = b.reserve_buffers().get();
    const auto& buffers = b.buffers();
    EXPECT_EQ(buffers.state, no_buffers_allocated);
    auto allocator = b.custom_mem_alloc();

    auto* input = allocate_decompression_buffer(
      allocator.opaqueState, b.min_allocation_size() + 1);
    EXPECT_EQ(buffers.state, input_buffer_allocated);
    EXPECT_EQ(input, buffers.input_buffer.get());

    auto* output = allocate_decompression_buffer(
      allocator.opaqueState, b.min_allocation_size() + 1);
    EXPECT_EQ(buffers.state, both_buffers_allocated);
    EXPECT_EQ(output, buffers.output_buffer.get());

    free_decompression_object(allocator.opaqueState, output);
    EXPECT_EQ(buffers.state, input_buffer_allocated);

    free_decompression_object(allocator.opaqueState, input);
    EXPECT_EQ(buffers.state, no_buffers_allocated);

    EXPECT_FALSE(b.has_reserved_buffers());
}

TEST(FallbackForSmallAllocs, PassThroughAlloc) {
    auto b = compression::lz4_decompression_buffers{4_MiB};
    // We do not need to reserve buffers for passthrough behaviour
    auto allocator = b.custom_mem_alloc();
    // Should be passed through to malloc
    auto* allocated = allocate_decompression_buffer(
      allocator.opaqueState, b.min_allocation_size() - 1);
    EXPECT_NE(allocated, nullptr);
    free_decompression_object(allocator.opaqueState, allocated);
}

TEST(MixedAllocations, PassThroughAlloc) {
    auto b = compression::lz4_decompression_buffers{4_MiB};
    auto units = b.reserve_buffers().get();

    const auto& buffers = b.buffers();
    auto allocator = b.custom_mem_alloc();

    auto* input = allocate_decompression_buffer(
      allocator.opaqueState, b.min_allocation_size() + 1);
    EXPECT_EQ(input, buffers.input_buffer.get());
    EXPECT_EQ(buffers.state, input_buffer_allocated);

    auto* random_alloc = allocate_decompression_buffer(
      allocator.opaqueState, b.min_allocation_size() - 1);
    EXPECT_FALSE(buffers.is_managed_address(random_alloc));
    EXPECT_EQ(buffers.state, input_buffer_allocated);

    free_decompression_object(allocator.opaqueState, input);
    EXPECT_EQ(buffers.state, no_buffers_allocated);

    free_decompression_object(allocator.opaqueState, random_alloc);
    EXPECT_EQ(buffers.state, no_buffers_allocated);
}

TEST(DecompressLargeBlocks, Decompression) {
    const auto data = random_generators::gen_alphanum_string(512);

    iobuf input;
    input.append(data.data(), data.size());

    using compression::internal::lz4_frame_compressor;
    auto& instance = compression::lz4_decompression_buffers_instance();
    auto compressed = lz4_frame_compressor::compress(input, LZ4F_max4MB);
    auto units = instance.reserve_buffers().get();
    auto uncompressed = lz4_frame_compressor::uncompress(compressed);
    auto stats = instance.allocation_stats();
    EXPECT_EQ(stats.allocs, 2);
    EXPECT_EQ(stats.deallocs, 2);
    EXPECT_EQ(stats.pass_through_allocs, 1);
    EXPECT_EQ(stats.pass_through_deallocs, 3);
    instance.reset_stats();
}

TEST(DecompressPassThroughBlocks, Decompression) {
    const auto data = random_generators::gen_alphanum_string(512);

    iobuf input;
    input.append(data.data(), data.size());

    using compression::internal::lz4_frame_compressor;
    auto& instance = compression::lz4_decompression_buffers_instance();
    auto compressed = lz4_frame_compressor::compress(input);
    auto units = instance.reserve_buffers().get();
    auto uncompressed = lz4_frame_compressor::uncompress(compressed);
    auto stats = instance.allocation_stats();
    EXPECT_EQ(stats.allocs, 0);
    EXPECT_EQ(stats.deallocs, 0);
    EXPECT_EQ(stats.pass_through_allocs, 3);
    EXPECT_EQ(stats.pass_through_deallocs, 5);
    instance.reset_stats();
}
