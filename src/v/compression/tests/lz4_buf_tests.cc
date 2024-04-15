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

#include "compression/internal/lz4_frame_compressor.h"
#include "compression/lz4_decompression_buffers.h"
#include "random/generators.h"
#include "units.h"

#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <lz4.h>

using enum compression::lz4_decompression_buffers::alloc_ctx::allocation_state;

BOOST_AUTO_TEST_CASE(state_transitions) {
    auto b = compression::lz4_decompression_buffers{4_MiB, 128_KiB + 1};
    const auto& buffers = b.buffers();
    BOOST_REQUIRE_EQUAL(buffers.state, no_buffers_allocated);
    auto allocator = b.custom_mem_alloc();

    auto* input = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    BOOST_REQUIRE_EQUAL(buffers.state, input_buffer_allocated);
    BOOST_REQUIRE_EQUAL(input, buffers.input_buffer.get());

    auto* output = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    BOOST_REQUIRE_EQUAL(buffers.state, both_buffers_allocated);
    BOOST_REQUIRE_EQUAL(output, buffers.output_buffer.get());

    allocator.customFree(allocator.opaqueState, input);
    BOOST_REQUIRE_EQUAL(buffers.state, output_buffer_allocated);

    allocator.customFree(allocator.opaqueState, output);
    BOOST_REQUIRE_EQUAL(buffers.state, no_buffers_allocated);
}

BOOST_AUTO_TEST_CASE(fallback_small_allocs) {
    auto b = compression::lz4_decompression_buffers{4_MiB, 128_KiB + 1};
    auto allocator = b.custom_mem_alloc();
    auto* allocated = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold() - 1);
    BOOST_REQUIRE_NE(allocated, nullptr);
    allocator.customFree(allocator.opaqueState, allocated);
}

BOOST_AUTO_TEST_CASE(mixed_allocs) {
    auto b = compression::lz4_decompression_buffers{4_MiB, 128_KiB + 1};
    const auto& buffers = b.buffers();
    auto allocator = b.custom_mem_alloc();

    auto* input = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    BOOST_REQUIRE_EQUAL(input, buffers.input_buffer.get());
    BOOST_REQUIRE_EQUAL(buffers.state, input_buffer_allocated);

    auto* random_alloc = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold() - 1);
    BOOST_REQUIRE(!buffers.is_managed_address(random_alloc));
    BOOST_REQUIRE_EQUAL(buffers.state, input_buffer_allocated);

    auto* output = allocator.customAlloc(
      allocator.opaqueState, b.min_alloc_threshold());
    BOOST_REQUIRE_EQUAL(output, buffers.output_buffer.get());
    BOOST_REQUIRE_EQUAL(buffers.state, both_buffers_allocated);

    allocator.customFree(allocator.opaqueState, input);
    BOOST_REQUIRE_EQUAL(buffers.state, output_buffer_allocated);

    allocator.customFree(allocator.opaqueState, random_alloc);
    BOOST_REQUIRE_EQUAL(buffers.state, output_buffer_allocated);

    allocator.customFree(allocator.opaqueState, output);
    BOOST_REQUIRE_EQUAL(buffers.state, no_buffers_allocated);
}

void test_decompression_calls(
  compression::lz4_decompression_buffers::stats expected,
  bool disable_prealloc = false,
  std::optional<LZ4F_blockSizeID_t> blocksize = std::nullopt) {
    compression::reset_lz4_decompression_buffers();
    auto deferred = ss::defer(
      [] { compression::lz4_decompression_buffers_instance().reset_stats(); });

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
    BOOST_REQUIRE(instance.allocation_stats() == expected);
}

BOOST_AUTO_TEST_CASE(decompress_large_blocks) {
    test_decompression_calls(
      {.allocs = 2,
       .deallocs = 2,
       .pass_through_allocs = 1,
       .pass_through_deallocs = 3},
      false,
      LZ4F_max4MB);
}

BOOST_AUTO_TEST_CASE(decompresss_passthrough_blocks) {
    test_decompression_calls(
      {.allocs = 0,
       .deallocs = 0,
       .pass_through_allocs = 3,
       .pass_through_deallocs = 5});
}

BOOST_AUTO_TEST_CASE(custom_alloc_disabled) {
    test_decompression_calls(
      {.allocs = 0,
       .deallocs = 0,
       .pass_through_allocs = 0,
       .pass_through_deallocs = 0},
      true);
}
