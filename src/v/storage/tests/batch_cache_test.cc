// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/random.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "random/generators.h"
#include "storage/batch_cache.h"
#include "storage/record_batch_builder.h"
#include "test_utils/fixture.h"

#include <seastar/core/sharded.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>

static storage::batch_cache::reclaim_options opts = {
  .growth_window = std::chrono::milliseconds(3000),
  .stable_window = std::chrono::milliseconds(10000),
  .min_size = 128 << 10,
  .max_size = 4 << 20,
};

using is_dirty_entry = storage::batch_cache::is_dirty_entry;

static model::record_batch
make_batch(size_t size = 10, model::offset offset = model::offset(0)) {
    storage::record_batch_builder b(model::record_batch_type(1), offset);
    for (size_t i = 0; i < size; i++) {
        b.add_raw_kv(iobuf::from("key"), iobuf::from("value"));
    }
    return std::move(b).build();
}

static model::record_batch make_random_batch(
  size_t max_size = 10, model::offset offset = model::offset(0)) {
    storage::record_batch_builder b(model::record_batch_type(1), offset);
    b.add_raw_kv(
      iobuf{}, bytes_to_iobuf(random_generators::get_bytes(max_size)));

    return std::move(b).build();
}

class batch_cache_test_fixture {
public:
    batch_cache_test_fixture()
      : cache(opts) {}

    auto& get_lru() { return cache._lru; };
    ~batch_cache_test_fixture() { cache.stop().get(); }

    storage::batch_cache cache;
};

FIXTURE_TEST(initially_empty, batch_cache_test_fixture) {
    BOOST_CHECK(cache.empty());
}

FIXTURE_TEST(evict, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    auto b = make_batch(100);
    auto w = cache.put(index, std::move(b), is_dirty_entry::no);
    BOOST_CHECK(!cache.empty());
    cache.evict(std::move(w.range()));
    BOOST_CHECK(cache.empty());
}

FIXTURE_TEST(reclaim_rounds_up, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    auto b = make_batch(5);
    auto b_size = b.memory_usage();
    std::cout << b_size << std::endl;
    cache.put(index, std::move(b), is_dirty_entry::no);
    BOOST_CHECK(!cache.empty());

    auto size = cache.reclaim(1);
    // reclaims rounds up to the range size for small batches
    BOOST_REQUIRE_EQUAL(size, storage::batch_cache::range::range_size);
    BOOST_CHECK(cache.empty());
}

FIXTURE_TEST(reclaim_removes_multiple, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    auto b = make_batch(100);
    auto b_size = b.memory_usage();

    cache.put(index, b.share(), is_dirty_entry::no);
    cache.put(index, b.share(), is_dirty_entry::no);
    cache.put(index, b.share(), is_dirty_entry::no);
    cache.put(index, b.share(), is_dirty_entry::no);
    cache.put(index, b.share(), is_dirty_entry::no);
    cache.put(index, b.share(), is_dirty_entry::no);
    BOOST_CHECK(!cache.empty());

    auto size = cache.reclaim(b_size + 1);
    BOOST_CHECK(size > (2 * b_size));
    BOOST_CHECK(cache.empty());
}

FIXTURE_TEST(weakness, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    auto b0 = cache.put(index, make_batch(10), is_dirty_entry::no);
    auto b1 = cache.put(index, make_batch(10), is_dirty_entry::no);
    auto b2 = cache.put(index, make_batch(10), is_dirty_entry::no);

    BOOST_CHECK(!cache.empty());

    BOOST_CHECK(b0.range());
    BOOST_CHECK(b1.range());
    BOOST_CHECK(b2.range());

    cache.reclaim(1);
    BOOST_CHECK(!b0.range());
    BOOST_CHECK(!b1.range());
    BOOST_CHECK(!b2.range());
}

SEASTAR_THREAD_TEST_CASE(touch) {
    static storage::batch_cache::reclaim_options opts = {
      .growth_window = std::chrono::milliseconds(3000),
      .stable_window = std::chrono::milliseconds(10000),
      .min_size = 1,
      .max_size = 1,
    };

    {
        std::unique_ptr<storage::batch_cache_index> index_1;
        std::unique_ptr<storage::batch_cache_index> index_2;

        storage::batch_cache cache(opts);
        index_1 = std::make_unique<storage::batch_cache_index>(cache);
        index_2 = std::make_unique<storage::batch_cache_index>(cache);
        auto b0 = cache.put(*index_1, make_batch(10), is_dirty_entry::no);
        auto b1 = cache.put(*index_2, make_batch(10), is_dirty_entry::no);

        // first one is invalid, second one still valid
        cache.reclaim(1);
        BOOST_CHECK(!b0.range());
        BOOST_CHECK(b1.range());
        cache.stop().get();
    }

    {
        std::unique_ptr<storage::batch_cache_index> index_1;
        std::unique_ptr<storage::batch_cache_index> index_2;

        // build the cache the same way
        storage::batch_cache cache(opts);
        index_1 = std::make_unique<storage::batch_cache_index>(cache);
        index_2 = std::make_unique<storage::batch_cache_index>(cache);
        auto b0 = cache.put(*index_1, make_batch(10), is_dirty_entry::no);
        auto b1 = cache.put(*index_2, make_batch(10), is_dirty_entry::no);

        // the first one moves to the head
        cache.touch(b0.range());
        // so reclaiming now frees the second
        cache.reclaim(1);
        BOOST_CHECK(b0.range());
        BOOST_CHECK(!b1.range());
        cache.stop().get();
    }
}

FIXTURE_TEST(index_get_empty, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    BOOST_CHECK(index.empty());
    BOOST_CHECK(!index.get(model::offset(0)));
    BOOST_CHECK(!index.get(model::offset(10)));
    BOOST_CHECK(index.empty());
}

FIXTURE_TEST(index_get, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);
    storage::batch_cache_index index2(cache);

    // [10][11:20][21:30]
    index.put(make_batch(1, model::offset(10)), is_dirty_entry::no);
    index.put(make_batch(10, model::offset(11)), is_dirty_entry::no);
    index.put(make_batch(10, model::offset(21)), is_dirty_entry::no);

    // before first
    BOOST_CHECK(!index.get(model::offset(0)));
    BOOST_CHECK(!index.get(model::offset(9)));

    // macro makes line numbers and printed values work for error messages
#define checker(o, base, last)                                                 \
    do {                                                                       \
        BOOST_REQUIRE(index.get(model::offset(o)));                            \
        BOOST_CHECK(                                                           \
          index.get(model::offset(o))->base_offset() == model::offset(base));  \
        BOOST_CHECK(                                                           \
          index.get(model::offset(o))->last_offset() == model::offset(last));  \
    } while (0)

    // at first
    checker(10, 10, 10);

    // begin, mid, end of each batch
    checker(11, 11, 20);
    checker(13, 11, 20);
    checker(20, 11, 20);

    checker(21, 21, 30);
    checker(24, 21, 30);
    checker(30, 21, 30);

#undef checker

    // after last
    BOOST_CHECK(!index.get(model::offset(31)));
    BOOST_CHECK(!index.get(model::offset(40)));

    // [11:20]   [41:50]
    index2.put(make_batch(10, model::offset(11)), is_dirty_entry::no);
    index2.put(make_batch(10, model::offset(41)), is_dirty_entry::no);

    // in the gap
    BOOST_CHECK(!index2.get(model::offset(21)));
    BOOST_CHECK(!index2.get(model::offset(25)));
    BOOST_CHECK(!index2.get(model::offset(40)));
}

FIXTURE_TEST(index_truncate_smoke, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    // add batches of increasing size
    model::offset base(0);
    for (size_t size = 1; size < 10; size++) {
        index.put(make_batch(size, base), is_dirty_entry::no);
        base += model::offset(size);
    }

    for (auto i = 0; i < base(); i++) {
        BOOST_CHECK(index.get(model::offset(i)));
    }

    base += model::offset(10);
    for (auto trunc_at = base(); trunc_at-- > 0;) {
        index.truncate(model::offset(trunc_at));
        for (auto i = trunc_at; i < base(); i++) {
            BOOST_CHECK(!index.get(model::offset(i)));
        }
    }
}

FIXTURE_TEST(index_truncate_hole, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    // [10][11:20]  [41:50]
    index.put(make_batch(1, model::offset(10)), is_dirty_entry::no);
    index.put(make_batch(10, model::offset(11)), is_dirty_entry::no);
    index.put(make_batch(10, model::offset(41)), is_dirty_entry::no);

    index.truncate(model::offset(25));
    // all batches belong to the same range, all of them will be evicted
    BOOST_CHECK(!index.get(model::offset(10)));
    BOOST_CHECK(!index.get(model::offset(11)));
    BOOST_CHECK(!index.get(model::offset(41)));
}

FIXTURE_TEST(index_truncate_hole_missing_prev, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    // [10][11:20]  [41:50]
    index.put(make_batch(1, model::offset(10)), is_dirty_entry::no);
    index.put(make_batch(10, model::offset(11)), is_dirty_entry::no);
    index.put(make_batch(10, model::offset(41)), is_dirty_entry::no);

    index.testing_evict_from_cache(model::offset(11));
    BOOST_CHECK(index.testing_exists_in_index(model::offset(11)));
    BOOST_CHECK(!index.get(model::offset(10)));

    index.truncate(model::offset(25));
    BOOST_CHECK(!index.get(model::offset(10)));
    BOOST_CHECK(!index.testing_exists_in_index(model::offset(11)));
    BOOST_CHECK(!index.get(model::offset(11)));
    BOOST_CHECK(!index.get(model::offset(41)));
}

FIXTURE_TEST(test_random_batch_sizes, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);
    std::vector<model::record_batch> batches;
    for (int i = 0; i < 1000; ++i) {
        auto batch = make_random_batch(
          random_generators::get_int<size_t>(10, 16_KiB), model::offset(i));
        index.put(batch, is_dirty_entry::no);
        batches.push_back(std::move(batch));
    }

    for (auto& b : batches) {
        auto from_cache = index.get(b.base_offset());
        BOOST_REQUIRE(from_cache.has_value());
        BOOST_REQUIRE_EQUAL(from_cache->header(), b.header());
        BOOST_REQUIRE_EQUAL(from_cache->data(), b.data());
    }
    double max_waste = ((double)storage::batch_cache::range::max_waste_bytes
                        / storage::batch_cache::range::range_size)
                       * 100.0;

    // assert waste, we have to skip last range
    for (auto& r : boost::make_iterator_range(
           get_lru().begin(), std::prev(get_lru().end()))) {
        BOOST_REQUIRE_LE(r.waste(), max_waste);
    }
}

FIXTURE_TEST(test_mark_clean_empty, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);
    index.mark_clean(model::offset(1000));
}

/// Test marking a single batch as clean to catch any potential off-by-one
/// errors or incorrect use of `<=` vs `<` comparison operators.
FIXTURE_TEST(test_mark_clean_min_edge, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    index.put(make_batch(1, model::offset(10)), is_dirty_entry::yes);
    index.mark_clean(model::offset(10));

    cache.clear();
    BOOST_CHECK(index.empty());
    BOOST_CHECK(cache.empty());
}

FIXTURE_TEST(test_mark_clean_edge, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    // Small batches which will go in the same range.
    index.put(make_batch(5, model::offset(20)), is_dirty_entry::no);
    index.put(make_batch(5, model::offset(30)), is_dirty_entry::yes);
    index.put(make_batch(5, model::offset(40)), is_dirty_entry::yes);
    index.put(make_batch(5, model::offset(10)), is_dirty_entry::no);

    // Reclaim will not happen due to a dirty batch in the range.
    cache.clear();
    BOOST_CHECK(index.get(model::offset(20)).has_value());
    BOOST_CHECK(index.get(model::offset(30)).has_value());
    BOOST_CHECK(index.get(model::offset(40)).has_value());
    BOOST_CHECK(index.get(model::offset(10)).has_value());

    // No reclaim as there is a dirty batch with a higher offset.
    index.mark_clean(model::offset(43));
    BOOST_CHECK(index.get(model::offset(20)).has_value());
    BOOST_CHECK(index.get(model::offset(30)).has_value());
    BOOST_CHECK(index.get(model::offset(40)).has_value());
    BOOST_CHECK(index.get(model::offset(10)).has_value());

    // Reclaim will happen here and evict the whole range.
    index.mark_clean(model::offset(44));
    cache.clear();
    BOOST_CHECK(!index.get(model::offset(20)));
    BOOST_CHECK(!index.get(model::offset(30)));
    BOOST_CHECK(!index.get(model::offset(40)));
    BOOST_CHECK(!index.get(model::offset(10)));
}

/// Regression test for a buggy iteration order after the introduction of the
/// dirty min-max optimization. In particular, that we are not trying to iterate
/// from the "min dirty offset=10" to the "mark clean up_to=1".
/// The assertions are implicit: not hitting any assertions in implementation
/// and not crashing.
FIXTURE_TEST(test_mark_clean_valid_iterating, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    index.put(make_batch(1, model::offset(1)), is_dirty_entry::no);
    index.put(make_batch(1, model::offset(2)), is_dirty_entry::no);
    index.put(make_batch(10, model::offset(10)), is_dirty_entry::yes);

    index.mark_clean(model::offset(1));
    index.mark_clean(model::offset(19));
}

FIXTURE_TEST(test_add_dirty_seq, batch_cache_test_fixture) {
    storage::batch_cache_index index(cache);

    index.put(make_batch(10, model::offset(10)), is_dirty_entry::yes);
    index.put(make_batch(10, model::offset(19)), is_dirty_entry::yes);

    index.mark_clean(model::offset(28));
}
