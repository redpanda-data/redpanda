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
#include "thirdparty/lz4/lz4.h"

#include <gmock/gmock.h>

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

class StaticInstanceTest : public ::testing::Test {
public:
    void SetUp() override { compression::reset_lz4_decompression_buffers(); }
    void TearDown() override {
        compression::lz4_decompression_buffers_instance().reset_stats();
    }
};

void test_decompression_calls(
  compression::lz4_decompression_buffers::stats expected,
  bool disable_prealloc = false,
  std::optional<LZ4F_blockSizeID_t> blocksize = std::nullopt) {
    if (disable_prealloc) {
        compression::init_lz4_decompression_buffers(
          4_MiB, 128_KiB + 1, disable_prealloc);
    }

    const auto data = random_generators::gen_alphanum_string(512);

    iobuf input;
    input.append(data.data(), data.size());

    using compression::internal::lz4_frame_compressor;
    auto& instance = compression::lz4_decompression_buffers_instance();
    auto compressed = blocksize.has_value()
                        ? lz4_frame_compressor::compress_with_block_size(
                            input, blocksize.value())
                        : lz4_frame_compressor::compress(input);
    auto uncompressed = lz4_frame_compressor::uncompress(compressed);
    EXPECT_EQ(instance.allocation_stats(), expected);
}

TEST_F(StaticInstanceTest, DecompressLargeBlocks) {
    test_decompression_calls(
      {.allocs = 2,
       .deallocs = 2,
       .pass_through_allocs = 1,
       .pass_through_deallocs = 3},
      false,
      LZ4F_max4MB);
}

TEST_F(StaticInstanceTest, DecompressPassThroughBlocks) {
    test_decompression_calls(
      {.allocs = 0,
       .deallocs = 0,
       .pass_through_allocs = 3,
       .pass_through_deallocs = 5});
}

TEST_F(StaticInstanceTest, CustomAllocDisabled) {
    test_decompression_calls(
      {.allocs = 0,
       .deallocs = 0,
       .pass_through_allocs = 0,
       .pass_through_deallocs = 0},
      true);
}
