/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "cloud_topics/level_zero/reader/materialized_extent_reader.h"
#include "cloud_topics/level_zero/reader/tests/materialized_extent_fixture.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

inline ss::logger test_log("materialized_extent_reader_gtest");

using namespace cloud_storage;
using namespace std::chrono_literals;

namespace cloud_topics = experimental::cloud_topics;

static chunked_vector<cloud_topics::extent_meta>
convert_placeholders(const chunked_vector<model::record_batch>& batches) {
    chunked_vector<cloud_topics::extent_meta> res;
    for (const auto& b : batches) {
        auto mext = materialized_extent_fixture::make_materialized_extent(
          b.copy());
        res.push_back(mext.meta);
    }
    return res;
}

TEST_F_CORO(materialized_extent_fixture, full_scan_test) {
    const int num_batches = 10;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);
    auto underlying = convert_placeholders(make_underlying());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");
    auto actual = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      rtc,
      logger);
    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
}

// Same as 'full_scan_test' but the range can be arbitrary
ss::future<> test_aggregated_log_partial_scan(
  materialized_extent_fixture* fx, int num_batches, int begin, int end) {
    co_await fx->add_random_batches(num_batches);
    fx->produce_placeholders(true, 1, {}, begin, end);

    // Copy batches that we expect to read
    model::offset base;
    model::offset last;
    chunked_vector<model::record_batch> expected_view;
    for (int i = begin; i <= end; i++) {
        const auto& hdr = fx->expected[i];
        if (base == model::offset{}) {
            base = hdr.base_offset();
        }
        last = hdr.last_offset();
        expected_view.push_back(fx->expected[i].copy());
    }
    vlog(
      test_log.info,
      "Fetching offsets {} to {}, expect to read {} batches",
      base,
      last,
      expected_view.size());
    for (const auto& b : expected_view) {
        vlog(
          test_log.info,
          "Expect fetching {}:{}",
          b.base_offset(),
          b.last_offset());
    }

    auto underlying = convert_placeholders(fx->make_underlying());

    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    retry_chain_logger logger(test_log, rtc, "materialized_extent_reader_test");

    auto actual = co_await cloud_topics::l0::materialize_placeholders(
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      fx->remote,
      fx->cache,
      rtc,
      logger);

    ASSERT_EQ_CORO(actual.size(), expected_view.size());
    ASSERT_TRUE_CORO(actual == expected_view);
}

TEST_F_CORO(materialized_extent_fixture, scan_range) {
    co_await test_aggregated_log_partial_scan(this, 10, 0, 0);
}
