// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record.h"
#include "storage/batch_cache.h"
#include "storage/record_batch_builder.h"
#include "test_utils/fixture.h"

#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>

static storage::batch_cache::reclaim_options opts = {
  .growth_window = std::chrono::milliseconds(3000),
  .stable_window = std::chrono::milliseconds(10000),
  .min_size = 128 << 10,
  .max_size = 4 << 20,
  .min_free_memory = 1};

using is_dirty_entry = storage::batch_cache::is_dirty_entry;

model::record_batch make_batch(size_t size) {
    static model::offset base_offset{0};
    iobuf value;
    value.append(ss::temporary_buffer<char>(size));
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, base_offset);
    builder.add_raw_kv(iobuf{}, std::move(value));
    auto batch = std::move(builder).build();
    base_offset += model::offset(batch.record_count());
    return batch;
}

class fixture {};

FIXTURE_TEST(reclaim, fixture) {
    using namespace std::chrono_literals;

    storage::batch_cache cache(opts);
    storage::batch_cache_index index(cache);
    std::vector<storage::batch_cache::entry> cache_entries;
    cache_entries.reserve(30);

    auto stats = ss::memory::stats();

    BOOST_TEST(stats.reclaims() == 0);
    BOOST_REQUIRE(stats.free_memory() > ss::memory::min_free_memory());
    size_t bytes_until_reclaim = stats.free_memory()
                                 - ss::memory::min_free_memory();

    info(
      "memory stats (kb) total {} free {} min_free {} until_reclaim {} "
      "reclaims {}",
      stats.total_memory() / 1024,
      stats.free_memory() / 1024,
      ss::memory::min_free_memory() / 1024,
      bytes_until_reclaim / 1024,
      stats.reclaims());

    size_t pages_until_reclaim = bytes_until_reclaim / ss::memory::page_size;

    // ensure there is some wiggle room. otherwise the test might be a bit
    // unreliable operating on the edge of reclaim
    BOOST_TEST_REQUIRE(pages_until_reclaim > 20, "please run with more memory");

    // insert batches into the cache up to roughly have the amount needed to
    // trigger reclaim
    for (size_t i = 0; i < (pages_until_reclaim / 2); i++) {
        size_t buf_size = ss::memory::page_size - sizeof(model::record_batch);
        auto batch = make_batch(buf_size);
        cache_entries.push_back(cache.put(index, batch, is_dirty_entry::no));
    }

    // cache uses an async reclaimer. give it a chance to run
    ss::thread::yield();

    // all of the cache entries should be valid
    BOOST_CHECK(std::all_of(
      cache_entries.begin(),
      cache_entries.end(),
      [](storage::batch_cache::entry& e) { return (bool)e.range(); }));

    stats = ss::memory::stats();
    BOOST_TEST(stats.reclaims() == 0);

    // now allocate past what should cause relcaims to trigger
    for (size_t i = 0; i < pages_until_reclaim; i++) {
        size_t buf_size = ss::memory::page_size - sizeof(model::record_batch);
        auto batch = make_batch(buf_size);
        auto e = cache.put(index, std::move(batch), is_dirty_entry::no);
        BOOST_REQUIRE((bool)e.range());
        cache_entries.emplace_back(std::move(e));
    }

    // cache uses an async reclaimer. give it a chance to run
    ss::thread::yield();

    // now some of the cache entries should have been reclaimed
    BOOST_CHECK(std::any_of(
      cache_entries.begin(),
      cache_entries.end(),
      [](storage::batch_cache::entry& e) { return !e.range(); }));

    stats = ss::memory::stats();
    BOOST_TEST(stats.reclaims() > 0);

    info(
      "memory stats (kb) total {} free {} min_free {} until_reclaim {} "
      "reclaims {}",
      stats.total_memory() / 1024,
      stats.free_memory() / 1024,
      ss::memory::min_free_memory() / 1024,
      bytes_until_reclaim / 1024,
      stats.reclaims());
    cache.stop().get();
}
