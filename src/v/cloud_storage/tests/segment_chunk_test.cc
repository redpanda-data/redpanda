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
#include "cloud_storage/segment_chunk_api.h"

#include <seastar/testing/thread_test_case.hh>

using namespace cloud_storage;

namespace {
ss::logger test_log("test");
}

SEASTAR_THREAD_TEST_CASE(test_chunk_ordering) {
    std::vector<segment_chunk> chunks{};

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

auto make_chunks(size_t n = 10) {
    segment_chunks::chunk_map_t chunks;
    auto handle = ss::make_lw_shared(ss::file{});
    for (size_t i = 0; i < n; ++i) {
        chunks.insert({i, segment_chunk{.handle = handle}});
    }
    return chunks;
}

SEASTAR_THREAD_TEST_CASE(test_eager_chunk_eviction) {
    class eager_chunk_eviction_strategy_test final
      : public eager_chunk_eviction_strategy {
    public:
        size_t evicted;

    protected:
        ss::future<> close_files(
          std::vector<ss::lw_shared_ptr<ss::file>> files_to_close,
          retry_chain_logger&) override {
            evicted = files_to_close.size();
            co_return;
        }
    };

    auto chunks = make_chunks();
    std::vector<segment_chunks::chunk_map_t::iterator> to_evict;
    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
        to_evict.push_back(it);
    }

    auto st = eager_chunk_eviction_strategy_test{};
    ss::abort_source as;
    retry_chain_node rtc{as};
    retry_chain_logger l{test_log, rtc};
    st.evict(to_evict, l).get();
    BOOST_REQUIRE_EQUAL(st.evicted, chunks.size());
}

SEASTAR_THREAD_TEST_CASE(test_capped_chunk_eviction) {
    class capped_chunk_eviction_strategy_test final
      : public capped_chunk_eviction_strategy {
    public:
        capped_chunk_eviction_strategy_test(
          uint64_t max_chunks, uint64_t hydrated)
          : capped_chunk_eviction_strategy{max_chunks, hydrated} {}
        size_t evicted;

    protected:
        ss::future<> close_files(
          std::vector<ss::lw_shared_ptr<ss::file>> files_to_close,
          retry_chain_logger&) override {
            evicted = files_to_close.size();
            co_return;
        }
    };

    auto chunks = make_chunks();
    std::vector<segment_chunks::chunk_map_t::iterator> to_evict;
    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
        to_evict.push_back(it);
    }

    size_t max = 5;
    size_t hydrated = 9;
    auto st = capped_chunk_eviction_strategy_test{max, hydrated};
    ss::abort_source as;
    retry_chain_node rtc{as};
    retry_chain_logger l{test_log, rtc};
    st.evict(to_evict, l).get();
    BOOST_REQUIRE_EQUAL(st.evicted, hydrated - max);
}

SEASTAR_THREAD_TEST_CASE(test_predictive_chunk_eviction) {
    class predictive_chunk_eviction_strategy_test final
      : public predictive_chunk_eviction_strategy {
    public:
        predictive_chunk_eviction_strategy_test(
          uint64_t max_chunks, uint64_t hydrated)
          : predictive_chunk_eviction_strategy{max_chunks, hydrated} {}
        size_t evicted;

    protected:
        ss::future<> close_files(
          std::vector<ss::lw_shared_ptr<ss::file>> files_to_close,
          retry_chain_logger&) override {
            evicted = files_to_close.size();
            co_return;
        }
    };

    segment_chunks::chunk_map_t chunks;
    auto handle = ss::make_lw_shared(ss::file{});
    chunks.insert(
      {0,
       segment_chunk{
         .current_state = chunk_state::hydrated,
         .handle = handle,
         .required_by_readers_in_future = 0,
         .required_after_n_chunks = 0}});
    chunks.insert(
      {1,
       segment_chunk{
         .current_state = chunk_state::hydrated,
         .handle = handle,
         .required_by_readers_in_future = 1,
         .required_after_n_chunks = 0}});
    chunks.insert(
      {2,
       segment_chunk{
         .current_state = chunk_state::hydrated,
         .handle = handle,
         .required_by_readers_in_future = 2,
         .required_after_n_chunks = 0}});

    std::vector<segment_chunks::chunk_map_t::iterator> to_evict;
    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
        to_evict.push_back(it);
    }

    size_t max = 1;
    size_t hydrated = 3;
    auto st = predictive_chunk_eviction_strategy_test{max, hydrated};
    ss::abort_source as;
    retry_chain_node rtc{as};
    retry_chain_logger l{test_log, rtc};

    st.evict(to_evict, l).get();

    BOOST_REQUIRE(!chunks[0].handle.has_value());
    BOOST_REQUIRE(chunks[0].current_state == chunk_state::not_available);

    BOOST_REQUIRE(!chunks[1].handle.has_value());
    BOOST_REQUIRE(chunks[1].current_state == chunk_state::not_available);

    BOOST_REQUIRE(chunks[2].handle.has_value());
    BOOST_REQUIRE(chunks[2].current_state == chunk_state::hydrated);
}
