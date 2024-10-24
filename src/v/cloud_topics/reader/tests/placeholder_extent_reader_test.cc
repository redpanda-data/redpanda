// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_topics/reader/placeholder_extent_reader.h"
#include "cloud_topics/reader/tests/placeholder_extent_fixture.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <exception>
#include <memory>
#include <system_error>

inline ss::logger test_log("placeholder_extent_reader_gtest");

using namespace cloud_storage;
using namespace std::chrono_literals;

namespace cloud_topics = experimental::cloud_topics;

struct fragmented_vector_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
        target->push_back(std::move(rb));
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}

    fragmented_vector<model::record_batch>* target;
};

TEST_F_CORO(placeholder_extent_fixture, full_scan_test) {
    const int num_batches = 10;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);
    auto underlying = make_log_reader();
    storage::log_reader_config config(
      model::offset(0),
      get_expected_committed_offset(),
      ss::default_priority_class());
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);
    auto reader = cloud_topics::make_placeholder_extent_reader(
      config,
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache,
      rtc);
    fragmented_vector<model::record_batch> actual;
    fragmented_vector_consumer consumer{
      .target = &actual,
    };
    co_await reader.consume(consumer, model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
}

// Same as 'full_scan_test' but the range can be arbitrary
ss::future<> test_aggregated_log_partial_scan(
  placeholder_extent_fixture* fx, int num_batches, int begin, int end) {
    co_await fx->add_random_batches(num_batches);
    fx->produce_placeholders(true, 1, {}, begin, end);

    // Copy batches that we expect to read
    model::offset base;
    model::offset last;
    fragmented_vector<model::record_batch> expected_view;
    for (int i = begin; i <= end; i++) {
        const auto& hdr = fx->expected.at(i);
        if (base == model::offset{}) {
            base = hdr.base_offset();
        }
        last = hdr.last_offset();
        expected_view.push_back(fx->expected.at(i).copy());
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

    auto underlying = fx->make_log_reader();
    storage::log_reader_config config(base, last, ss::default_priority_class());

    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 100ms);

    auto reader = cloud_topics::make_placeholder_extent_reader(
      config,
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      fx->remote,
      fx->cache,
      rtc);

    fragmented_vector<model::record_batch> actual;
    fragmented_vector_consumer consumer{
      .target = &actual,
    };

    co_await reader.consume(consumer, model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(actual.size(), expected_view.size());
    ASSERT_TRUE_CORO(actual == expected_view);
}

TEST_F_CORO(placeholder_extent_fixture, scan_range) {
    co_await test_aggregated_log_partial_scan(this, 10, 0, 0);
}
