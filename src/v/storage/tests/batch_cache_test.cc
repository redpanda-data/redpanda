#include "cluster/simple_batch_builder.h"
#include "model/record.h"
#include "storage/batch_cache.h"

#include <seastar/testing/thread_test_case.hh>

static storage::batch_cache::reclaim_options opts = {
  .growth_window = std::chrono::milliseconds(3000),
  .stable_window = std::chrono::milliseconds(10000),
  .min_size = 128 << 10,
  .max_size = 4 << 20,
};

static model::record_batch
make_batch(size_t size = 10, model::offset offset = model::offset(0)) {
    cluster::simple_batch_builder b(model::record_batch_type(1), offset);
    for (size_t i = 0; i < size; i++) {
        b.add_kv("key", "value");
    }
    return std::move(b).build();
}

SEASTAR_THREAD_TEST_CASE(initially_empty) {
    storage::batch_cache c(opts);
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(evict) {
    storage::batch_cache c(opts);

    auto b = make_batch(100);
    auto w = c.put(std::move(b));
    BOOST_CHECK(!c.empty());
    c.evict(std::move(w));
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(reclaim_rounds_up) {
    storage::batch_cache c(opts);

    auto b = make_batch(100);
    auto b_size = b.memory_usage();

    c.put(std::move(b));
    BOOST_CHECK(!c.empty());

    auto size = c.reclaim(1);
    BOOST_CHECK(size == b_size);
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(reclaim_removes_multiple) {
    storage::batch_cache c(opts);

    auto b = make_batch(100);
    auto b_size = b.memory_usage();

    c.put(b.share());
    c.put(b.share());
    c.put(b.share());
    c.put(b.share());
    c.put(b.share());
    c.put(b.share());
    BOOST_CHECK(!c.empty());

    auto size = c.reclaim(b_size + 1);
    BOOST_CHECK(size > (2 * b_size));
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(weakness) {
    storage::batch_cache c(opts);

    auto b0 = c.put(make_batch(10));
    auto b1 = c.put(make_batch(10));
    auto b2 = c.put(make_batch(10));

    BOOST_CHECK(!c.empty());

    BOOST_CHECK(b0);
    BOOST_CHECK(b1);
    BOOST_CHECK(b2);

    c.reclaim(1);
    BOOST_CHECK(!b0);
    BOOST_CHECK(!b1);
    BOOST_CHECK(!b2);
}

SEASTAR_THREAD_TEST_CASE(touch) {
    static storage::batch_cache::reclaim_options opts = {
      .growth_window = std::chrono::milliseconds(3000),
      .stable_window = std::chrono::milliseconds(10000),
      .min_size = 1,
      .max_size = 1,
    };

    {
        storage::batch_cache c(opts);
        auto b0 = c.put(make_batch(10));
        auto b1 = c.put(make_batch(10));

        // first one is invalid, second one still valid
        c.reclaim(1);
        BOOST_CHECK(!b0);
        BOOST_CHECK(b1);
    }

    {
        // build the cache the same way
        storage::batch_cache c(opts);
        auto b0 = c.put(make_batch(10));
        auto b1 = c.put(make_batch(10));

        // the first one moves to the head
        c.touch(b0);
        // so reclaiming now frees the second
        c.reclaim(1);
        BOOST_CHECK(b0);
        BOOST_CHECK(!b1);
    }
}

SEASTAR_THREAD_TEST_CASE(index_get_empty) {
    storage::batch_cache cache(opts);
    storage::batch_cache_index index(cache);

    BOOST_CHECK(index.empty());
    BOOST_CHECK(!index.get(model::offset(0)));
    BOOST_CHECK(!index.get(model::offset(10)));
    BOOST_CHECK(index.empty());
}

SEASTAR_THREAD_TEST_CASE(index_get) {
    storage::batch_cache cache(opts);
    storage::batch_cache_index index(cache);
    storage::batch_cache_index index2(cache);

    // [10][11:20][21:30]
    index.put(make_batch(1, model::offset(10)));
    index.put(make_batch(10, model::offset(11)));
    index.put(make_batch(10, model::offset(21)));

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
    index2.put(make_batch(10, model::offset(11)));
    index2.put(make_batch(10, model::offset(41)));

    // in the gap
    BOOST_CHECK(!index2.get(model::offset(21)));
    BOOST_CHECK(!index2.get(model::offset(25)));
    BOOST_CHECK(!index2.get(model::offset(40)));
}

SEASTAR_THREAD_TEST_CASE(index_truncate_smoke) {
    storage::batch_cache cache(opts);
    storage::batch_cache_index index(cache);

    // add batches of increasing size
    model::offset base(0);
    for (size_t size = 1; size < 10; size++) {
        index.put(make_batch(size, base));
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

SEASTAR_THREAD_TEST_CASE(index_truncate_hole) {
    storage::batch_cache cache(opts);
    storage::batch_cache_index index(cache);

    // [10][11:20]  [41:50]
    index.put(make_batch(1, model::offset(10)));
    index.put(make_batch(10, model::offset(11)));
    index.put(make_batch(10, model::offset(41)));

    index.truncate(model::offset(25));
    BOOST_CHECK(index.get(model::offset(10)));
    BOOST_CHECK(index.get(model::offset(11)));
    BOOST_CHECK(!index.get(model::offset(41)));
}

SEASTAR_THREAD_TEST_CASE(index_truncate_hole_missing_prev) {
    storage::batch_cache cache(opts);
    storage::batch_cache_index index(cache);

    // [10][11:20]  [41:50]
    index.put(make_batch(1, model::offset(10)));
    index.put(make_batch(10, model::offset(11)));
    index.put(make_batch(10, model::offset(41)));

    index.testing_evict_from_cache(model::offset(11));
    BOOST_CHECK(index.testing_exists_in_index(model::offset(11)));
    BOOST_CHECK(index.get(model::offset(10)));

    index.truncate(model::offset(25));
    BOOST_CHECK(index.get(model::offset(10)));
    BOOST_CHECK(!index.testing_exists_in_index(model::offset(11)));
    BOOST_CHECK(!index.get(model::offset(11)));
    BOOST_CHECK(!index.get(model::offset(41)));
}
