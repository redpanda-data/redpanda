#include "cluster/simple_batch_builder.h"
#include "model/record.h"
#include "storage/batch_cache.h"
#include "test_utils/fixture.h"

#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <algorithm>

class fixture {};

FIXTURE_TEST(reclaim, fixture) {
    using namespace std::chrono_literals;

    storage::batch_cache cache;
    std::vector<storage::batch_cache::entry_ptr> cache_entries;
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
    for (auto i = 0; i < (pages_until_reclaim / 2); i++) {
        size_t buf_size = ss::memory::page_size - sizeof(model::record_batch);
        iobuf buf;
        buf.append(ss::temporary_buffer<char>(buf_size));
        model::record_batch batch(
          model::record_batch_header{},
          model::record_batch::compressed_records(buf_size, std::move(buf)));
        auto e = cache.put(std::move(batch));
        cache_entries.emplace_back(std::move(e));
    }

    // cache uses an async reclaimer. give it a chance to run
    ss::thread::yield();

    // all of the cache entries should be valid
    BOOST_CHECK(std::all_of(
      cache_entries.begin(),
      cache_entries.end(),
      [](storage::batch_cache::entry_ptr& e) { return (bool)e; }));

    stats = ss::memory::stats();
    BOOST_TEST(stats.reclaims() == 0);

    // now allocate past what should cause relcaims to trigger
    for (auto i = 0; i < pages_until_reclaim; i++) {
        size_t buf_size = ss::memory::page_size - sizeof(model::record_batch);
        iobuf buf;
        buf.append(ss::temporary_buffer<char>(buf_size));
        model::record_batch batch(
          model::record_batch_header{},
          model::record_batch::compressed_records(buf_size, std::move(buf)));
        auto e = cache.put(std::move(batch));
        BOOST_REQUIRE((bool)e);
        cache_entries.emplace_back(std::move(e));
    }

    // cache uses an async reclaimer. give it a chance to run
    ss::thread::yield();

    // now some of the cache entries should have been reclaimed
    BOOST_CHECK(std::any_of(
      cache_entries.begin(),
      cache_entries.end(),
      [](storage::batch_cache::entry_ptr& e) { return !e; }));

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
}
