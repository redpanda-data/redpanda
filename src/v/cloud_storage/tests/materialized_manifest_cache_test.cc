/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/tests/util.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/testing/seastar_test.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <iterator>
#include <numeric>

using namespace cloud_storage;

static ss::logger test_log("async_manifest_view_log");
static const model::initial_revision_id manifest_rev(111);

static spillover_manifest make_manifest(model::offset base) {
    spillover_manifest manifest(manifest_ntp, manifest_rev);
    segment_meta meta{
      .size_bytes = 1024,
      .base_offset = base,
      .committed_offset = model::next_offset(base),
    };
    manifest.add(meta);
    return manifest;
}

manifest_cache_key make_key(int64_t off) {
    return std::make_tuple(manifest_ntp, model::offset{off});
}

manifest_cache_key make_key(model::offset off) {
    return std::make_tuple(manifest_ntp, off);
}

// Add elements to an empty cache and verify that they are added correctly.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_empty) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(10);
    cache.start().get();

    auto fut = cache.prepare(10, ctxlog);
    BOOST_REQUIRE(fut.available());
    const auto expected_so = model::offset(34);
    const auto key = make_key(expected_so);
    cache.put(std::move(fut.get()), make_manifest(expected_so), ctxlog);

    auto res = cache.get(key, ctxlog);
    BOOST_REQUIRE(res != nullptr);
    auto actual_so = res->manifest.get_start_offset();
    BOOST_REQUIRE(actual_so.has_value());
    BOOST_REQUIRE(actual_so.has_value() && actual_so.value() == expected_so);
    BOOST_REQUIRE(cache.size() == 1);
    BOOST_REQUIRE(cache.size_bytes() == 10);
}

// Add elements to a non-empty cache and verify that the cache size increases
// and the new elements are added correctly.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_non_empty) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(100);
    cache.start().get();

    auto fut0 = cache.prepare(20, ctxlog);
    BOOST_REQUIRE(fut0.available());
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)), ctxlog);

    auto fut1 = cache.prepare(20, ctxlog);
    BOOST_REQUIRE(fut1.available());
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)), ctxlog);

    auto fut2 = cache.prepare(20, ctxlog);
    BOOST_REQUIRE(fut2.available());
    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)), ctxlog);

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    auto check_segment = [&](model::offset expected_so) {
        const auto key = make_key(expected_so);
        auto res = cache.get(key, ctxlog);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    check_segment(model::offset(0));
    check_segment(model::offset(1));
    check_segment(model::offset(2));
}

// Add elements beyond the capacity of the cache and verify that the least
// recently used elements are removed to make room for new elements.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_evict) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50);
    cache.start().get();

    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)), ctxlog);

    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)), ctxlog);

    auto fut2 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)), ctxlog);

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto check_segment = [&](model::offset expected_so) {
        const auto key = make_key(expected_so);
        auto res = cache.get(key, ctxlog);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    // First manifest should be missing at this point
    const auto key = make_key(model::offset{0});
    auto res = cache.get(key, ctxlog);
    BOOST_REQUIRE(res == nullptr);
    check_segment(model::offset(1));
    check_segment(model::offset(2));
}

// Add elements beyond the capacity of the cache and verify that the least
// recently used elements are removed to make room for new elements. Hold
// the reference to the least used element to postpone eviction. Check that
// the eviction happens after the referenced element is deleted.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_wait_evict) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50);
    cache.start().get();

    auto m0 = model::offset(0);
    auto k0 = make_key(m0);
    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(m0), ctxlog);
    auto p0 = cache.get(k0, ctxlog);
    BOOST_REQUIRE(p0);

    auto m1 = model::offset(1);
    auto k1 = make_key(m1);
    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(m1), ctxlog);
    auto p1 = cache.get(k1, ctxlog);
    BOOST_REQUIRE(p1);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20, ctxlog);
    // The future can't become available yet because the
    // m0 manifest is referenced through p0 shared pointer.
    ss::sleep(100ms).get();
    BOOST_REQUIRE(!fut2.available());
    // This should unstuck the 'prepare' future
    p0 = nullptr;
    cache.put(std::move(fut2.get()), make_manifest(m2), ctxlog);

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto check_segment = [&](model::offset expected_so) {
        auto key = make_key(expected_so);
        auto res = cache.get(key, ctxlog);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    check_segment(m1);
    check_segment(m2);
}

// Add elements beyond the capacity of the cache and verify that the least
// recently used elements are removed to make room for new elements. Hold
// the reference to the least used element to postpone eviction. Check that
// the prepare method throws when timeout expires.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_wait_evict_timeout) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50);
    cache.start().get();

    auto check_segment = [&](model::offset expected_so) {
        auto key = make_key(expected_so);
        auto res = cache.get(key, ctxlog);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    auto m0 = model::offset(0);
    auto k0 = make_key(m0);
    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(m0), ctxlog);
    auto p0 = cache.get(k0, ctxlog);
    BOOST_REQUIRE(p0);

    auto m1 = model::offset(1);
    auto k1 = make_key(m1);
    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(m1), ctxlog);
    auto p1 = cache.get(k1, ctxlog);
    BOOST_REQUIRE(p1);

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20, ctxlog, 100ms);

    // The eviction candidate should be accessible through the
    // '_eviction_rollback' list. The 'size' and 'size_bytes' should also give
    // consistent results. The manifests are moved into the eviction list before
    // scheduling point.
    check_segment(m0);
    check_segment(m1);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    BOOST_REQUIRE_THROW(
      cache.put(std::move(fut2.get()), make_manifest(m2), ctxlog),
      ss::timed_out_error);

    // After the failed attempt to put new manifest the state should stay the
    // same.
    check_segment(m0);
    check_segment(m1);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

// Fill the cache to its capacity and access elements to verify that the least
// recently used elements are evicted correctly.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_get) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50);
    cache.start().get();

    auto m0 = model::offset(0);
    auto k0 = make_key(m0);
    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(m0), ctxlog);

    auto m1 = model::offset(1);
    auto k1 = make_key(m1);
    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(m1), ctxlog);
    auto p0 = cache.get(k0, ctxlog);
    BOOST_REQUIRE(p0);
    p0 = nullptr;

    auto check_segment = [&](model::offset expected_so) {
        auto key = make_key(expected_so);
        auto res = cache.get(key, ctxlog);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20, ctxlog);

    cache.put(std::move(fut2.get()), make_manifest(m2), ctxlog);

    // Element 1 should be evicted
    check_segment(m0);
    check_segment(m2);
    auto p1 = cache.get(k1, ctxlog);
    BOOST_REQUIRE(p1 == nullptr);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

// Fill the cache to its capacity and access elements to verify that the least
// recently used elements are evicted correctly. Use 'promote' method instead of
// 'get'.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_promote) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50);
    cache.start().get();

    auto m0 = model::offset(0);
    auto k0 = make_key(m0);
    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(m0), ctxlog);

    auto m1 = model::offset(1);
    auto k1 = make_key(m1);
    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(m1), ctxlog);
    cache.promote(k0);

    auto check_segment = [&](model::offset expected_so) {
        auto key = make_key(expected_so);
        auto res = cache.get(key, ctxlog);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20, ctxlog);

    cache.put(std::move(fut2.get()), make_manifest(m2), ctxlog);

    // Element 1 should be evicted
    check_segment(m0);
    check_segment(m2);
    auto p1 = cache.get(k1, ctxlog);
    BOOST_REQUIRE(p1 == nullptr);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_remove) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(60);
    cache.start().get();

    auto m0 = model::offset(0);
    auto k0 = make_key(m0);
    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(m0), ctxlog);

    auto m1 = model::offset(1);
    auto k1 = make_key(m1);
    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(m1), ctxlog);

    auto m2 = model::offset(2);
    auto k2 = make_key(m2);
    auto fut2 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut2.get()), make_manifest(m2), ctxlog);

    auto p1 = cache.get(k1, ctxlog);
    BOOST_REQUIRE(p1 != nullptr);

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    cache.remove(k1, ctxlog);

    p1 = cache.get(k1, ctxlog);
    BOOST_REQUIRE(p1 == nullptr);
    auto p2 = cache.get(k2, ctxlog);
    BOOST_REQUIRE(p2 != nullptr);
    auto p0 = cache.get(k0, ctxlog);
    BOOST_REQUIRE(p0 != nullptr);

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

// Add elements to fill cache capacity and then shrink the cache.
// Check that the element is evicted from it.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_shrink) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(60);
    cache.start().get();

    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)), ctxlog);

    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)), ctxlog);

    auto fut2 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)), ctxlog);

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    auto check_segment = [&](model::offset expected_so, bool null_expected) {
        auto key = make_key(expected_so);
        auto res = cache.get(key, ctxlog);
        if (null_expected) {
            BOOST_REQUIRE(res == nullptr);
        } else {
            BOOST_REQUIRE(res != nullptr);
            auto actual_so = res->manifest.get_start_offset();
            BOOST_REQUIRE(actual_so.has_value());
            BOOST_REQUIRE(
              actual_so.has_value() && actual_so.value() == expected_so);
        }
    };

    check_segment(model::offset(0), false);
    check_segment(model::offset(1), false);
    check_segment(model::offset(2), false);

    cache.set_capacity(20).get();

    check_segment(model::offset(0), true);
    check_segment(model::offset(1), true);
    check_segment(model::offset(2), false);
}

// Add elements to fill cache capacity and then grow the cache.
// Check that the 'prepare' operation which was waiting for eviction
// succeeded.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_grow) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(40);
    cache.start().get();

    auto fut0 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)), ctxlog);
    auto p0 = cache.get(make_key(0), ctxlog);

    auto fut1 = cache.prepare(20, ctxlog);
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)), ctxlog);
    auto p1 = cache.get(make_key(1), ctxlog);

    // Cache is full at this point

    auto fut2 = cache.prepare(20, ctxlog);
    ss::sleep(100ms).get();
    BOOST_REQUIRE(!fut2.available());
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    // Increase capacity and unblock 'fut2'
    cache.set_capacity(60).get();

    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)), ctxlog);

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(make_key(expected_so), ctxlog);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    check_segment(model::offset(0));
    check_segment(model::offset(1));
    check_segment(model::offset(2));

    // Element 0 is still being evicted by last 'prepare' call which doesn't
    // know about the fact that cache grow bigger. This is a side effect which
    // shouldn't cause any problems.
    p0 = nullptr;
    p1 = nullptr;
    ss::sleep(100ms).get();
    p0 = cache.get(make_key(0), ctxlog);
    BOOST_REQUIRE(p0 == nullptr);
    p1 = cache.get(make_key(1), ctxlog);
    BOOST_REQUIRE(p1 != nullptr);
}
