/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_chunk.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_chunk_ordering) {
    std::vector<cloud_storage::segment_chunk> chunks{};

    // Primary ordering is on readers waiting, higher values are more important.
    chunks.push_back(
      {.required_by_readers_in_future = 10, .required_after_n_chunks = 1});
    chunks.push_back(
      {.required_by_readers_in_future = 11, .required_after_n_chunks = 100});
    chunks.push_back(
      {.required_by_readers_in_future = 12, .required_after_n_chunks = 121});
    chunks.push_back(
      {.required_by_readers_in_future = 13, .required_after_n_chunks = 1221});

    std::sort(chunks.begin(), chunks.end());
    auto expected = 10;
    for (const auto& chunk : chunks) {
        BOOST_REQUIRE_EQUAL(expected, chunk.required_by_readers_in_future);
        expected += 1;
    }

    chunks.clear();

    // Secondary ordering is on required_after_n_chunks, lower values are more
    // important. A lower required_after_n_chunks means that a chunk is required
    // soon.
    chunks.push_back(
      {.required_by_readers_in_future = 10, .required_after_n_chunks = 1});
    chunks.push_back(
      {.required_by_readers_in_future = 10, .required_after_n_chunks = 2});
    chunks.push_back(
      {.required_by_readers_in_future = 10, .required_after_n_chunks = 3});
    chunks.push_back(
      {.required_by_readers_in_future = 10, .required_after_n_chunks = 4});
    // Chunk with 0 value of required_after_n_chunks will appear first in
    // ascending sort. Since 0 is the default, a value of 0 means that a chunk
    // is not required by any readers. A value of 1 means that the chunks is
    // next to be read for some data source.
    chunks.push_back(
      {.required_by_readers_in_future = 10, .required_after_n_chunks = 0});

    std::sort(chunks.begin(), chunks.end());
    BOOST_REQUIRE_EQUAL(chunks.begin()->required_after_n_chunks, 0);
    chunks.erase(chunks.begin());

    expected = 4;
    for (const auto& chunk : chunks) {
        BOOST_REQUIRE_EQUAL(expected, chunk.required_after_n_chunks);
        expected -= 1;
    }
}
