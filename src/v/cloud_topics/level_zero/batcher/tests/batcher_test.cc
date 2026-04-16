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
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_topics/level_zero/batcher/batcher.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/object_utils.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "remote_mock.h"
#include "storage/record_batch_builder.h"
#include "test_utils/random_bytes.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <exception>
#include <iterator>
#include <memory>
#include <system_error>

inline ss::logger test_log("aggregated_uploader_gtest");

static cloud_topics::cluster_epoch min_epoch{3840};

struct reader_with_content {
    chunked_vector<bytes> keys;
    chunked_vector<bytes> records;
    chunked_vector<model::record_batch> batches;
};

reader_with_content
get_random_batches(int num_batches, int num_records) { // NOLINT
    chunked_vector<bytes> keys;
    chunked_vector<bytes> records;
    chunked_vector<model::record_batch> batches;
    model::offset offset{0};
    for (int i = 0; i < num_batches; i++) {
        // Build a record batch
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(offset));

        for (int r = 0; r < num_records; r++) {
            auto k = tests::random_iobuf(32);
            auto v = tests::random_iobuf(256);
            keys.push_back(iobuf_to_bytes(k.copy()));
            records.push_back(iobuf_to_bytes(v.copy()));
            builder.add_raw_kv(std::move(k), std::move(v));
            offset += 1;
        }
        batches.push_back(std::move(builder).build());
    }
    return {
      .keys = std::move(keys),
      .records = std::move(records),
      .batches = std::move(batches),
    };
}

class static_cluster_services : public cloud_topics::cluster_services {
public:
    static_cluster_services() = default;
    seastar::future<cloud_topics::cluster_epoch>
    current_epoch(seastar::abort_source*) override {
        return seastar::make_ready_future<cloud_topics::cluster_epoch>(0);
    }

    seastar::future<>
    invalidate_epoch_below(cloud_topics::cluster_epoch) override {
        return ss::now();
    }
};

namespace cloud_topics::l0 {
struct batcher_accessor {
    ss::future<std::expected<std::monostate, errc>> run_once() noexcept {
        constexpr static size_t lim = 10_MiB;
        auto list = batcher->_stage.pull_write_requests(lim);
        return batcher->run_once(std::move(list));
    }

    cloud_topics::l0::batcher<ss::manual_clock>* batcher;
};
} // namespace cloud_topics::l0

namespace cloud_topics::l0 {
struct write_pipeline_accessor {
    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) {
        return pipeline->get_pending().size() == n;
    }

    cloud_topics::l0::write_pipeline<ss::manual_clock>* pipeline;
};
} // namespace cloud_topics::l0

ss::future<> sleep(std::chrono::milliseconds delta, int retry_limit = 100) {
    ss::manual_clock::advance(delta);
    for (int i = 0; i < retry_limit; i++) {
        co_await ss::yield();
    }
}

// Simulate sleep of certain duration and wait until the condition is met
template<class Fn>
ss::future<>
sleep_until(std::chrono::milliseconds delta, Fn&& fn, int retry_limit = 100) {
    ss::manual_clock::advance(delta);
    for (int i = 0; i < retry_limit; i++) {
        co_await ss::yield();
        if (fn()) {
            co_return;
        }
    }
    GTEST_MESSAGE_("Test stalled", ::testing::TestPartResult::kFatalFailure);
}

TEST_CORO(batcher_test, single_write_request) {
    remote_mock mock;
    cloud_storage_clients::bucket_name bucket("foo");
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    static_cluster_services cluster_services;
    cloud_topics::l0::batcher<ss::manual_clock> batcher(
      pipeline.register_write_pipeline_stage(),
      bucket,
      mock,
      &cluster_services);
    cloud_topics::l0::batcher_accessor batcher_accessor{
      .batcher = &batcher,
    };
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };
    int num_batches = 10;
    auto [_, records, reader] = get_random_batches(num_batches, 10);
    // Expect single upload to be made
    mock.expect_upload_object(records);

    const auto timeout = 1s;
    auto deadline = ss::manual_clock::now() + timeout;
    auto fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(reader), deadline);
    // Make sure the write request is in the _pending list
    co_await sleep_until(
      10ms, [&] { return pipeline_accessor.write_requests_pending(1); });

    auto res = co_await batcher_accessor.run_once();

    ASSERT_TRUE_CORO(res.has_value());

    auto write_res = co_await std::move(fut);
    ASSERT_TRUE_CORO(write_res.has_value());

    // Expect single L0 upload
    ASSERT_EQ_CORO(mock.keys.size(), 1);
    auto id = mock.keys.back();

    // Check that uuid in the placeholder can be used to
    // access the data in S3.
    auto placeholder_batches = std::move(write_res.value());
    ASSERT_EQ_CORO(placeholder_batches.extents.size(), num_batches);
    for (const cloud_topics::extent_meta& ext : placeholder_batches.extents) {
        auto sid = cloud_topics::object_path_factory::level_zero_path(ext.id);
        ASSERT_EQ_CORO(sid, id);
    }
}

TEST_CORO(batcher_test, many_write_requests) {
    remote_mock mock;
    cloud_storage_clients::bucket_name bucket("foo");
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    static_cluster_services cluster_services;
    cloud_topics::l0::batcher<ss::manual_clock> batcher(
      pipeline.register_write_pipeline_stage(),
      bucket,
      mock,
      &cluster_services);
    cloud_topics::l0::batcher_accessor batcher_accessor{
      .batcher = &batcher,
    };
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };

    std::vector<size_t> expected_num_batches = {10, 20, 10};
    auto [_1, records1, reader1] = get_random_batches(10, 10);
    auto [_2, records2, reader2] = get_random_batches(20, 10);
    auto [_3, records3, reader3] = get_random_batches(10, 20);

    chunked_vector<bytes> all_records;
    std::copy(
      std::make_move_iterator(records1.begin()),
      std::make_move_iterator(records1.end()),
      std::back_inserter(all_records));
    std::copy(
      std::make_move_iterator(records2.begin()),
      std::make_move_iterator(records2.end()),
      std::back_inserter(all_records));
    std::copy(
      std::make_move_iterator(records3.begin()),
      std::make_move_iterator(records3.end()),
      std::back_inserter(all_records));

    // Expect single upload to be made
    mock.expect_upload_object(all_records);

    const auto timeout = 1s;
    auto deadline = ss::manual_clock::now() + timeout;
    std::vector<
      ss::future<std::expected<cloud_topics::upload_meta, std::error_code>>>
      futures;
    futures.push_back(pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(reader1), deadline));
    futures.push_back(pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(reader2), deadline));
    futures.push_back(pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(reader3), deadline));

    // Make sure that all write requests are in the _pending list
    co_await sleep_until(
      10ms, [&] { return pipeline_accessor.write_requests_pending(3); });

    // Single L0 object that contains data from all write requests
    // should be "uploaded".
    auto res = co_await batcher_accessor.run_once();

    // Expect single L0 upload
    ASSERT_EQ_CORO(mock.keys.size(), 1);
    auto id = mock.keys.back();

    ASSERT_TRUE_CORO(res.has_value());

    auto results = co_await ss::when_all_succeed(std::move(futures));
    size_t ix = 0;
    for (auto& write_res : results) {
        ASSERT_TRUE_CORO(write_res.has_value());
        // Check that uuid in the placeholder can be used to
        // access the data in S3. All placeholders should share the same
        // uuid.
        auto placeholder_batches = std::move(write_res.value());
        ASSERT_EQ_CORO(
          placeholder_batches.extents.size(), expected_num_batches.at(ix));
        for (const cloud_topics::extent_meta& ext :
             placeholder_batches.extents) {
            auto sid = cloud_topics::object_path_factory::level_zero_path(
              ext.id);
            ASSERT_EQ_CORO(sid, id);
        }
        ix++;
    }
}

TEST_CORO(batcher_test, expired_write_request) {
    // The test starts two write request but one of which is expected to
    // timeout. The expectation is that uploaded L0 object will not contain any
    // data from the expired write request.
    remote_mock mock;
    cloud_storage_clients::bucket_name bucket("foo");
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    static_cluster_services cluster_services;
    cloud_topics::l0::batcher<ss::manual_clock> batcher(
      pipeline.register_write_pipeline_stage(),
      bucket,
      mock,
      &cluster_services);
    cloud_topics::l0::batcher_accessor batcher_accessor{
      .batcher = &batcher,
    };
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };

    int expected_num_batches = 33;
    int expected_num_records = 33;
    auto [_1, included_records, included_batches] = get_random_batches(
      expected_num_batches, expected_num_records);
    auto [_2, timedout_records, timedout_batches] = get_random_batches(1, 1);

    chunked_vector<bytes> all_records;
    std::copy(
      std::make_move_iterator(included_records.begin()),
      std::make_move_iterator(included_records.end()),
      std::back_inserter(all_records));
    std::copy(
      std::make_move_iterator(timedout_records.begin()),
      std::make_move_iterator(timedout_records.end()),
      std::back_inserter(all_records));

    // Expect single upload to be made
    mock.expect_upload_object(all_records);

    const auto timeout = 1s;
    auto deadline = ss::manual_clock::now() + timeout;
    auto expect_fail_fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(timedout_batches), deadline);

    // Let time pass to invalidate the first enqueued write request
    co_await sleep_until(
      10ms, [&] { return pipeline_accessor.write_requests_pending(1); });
    ss::manual_clock::advance(timeout);

    deadline = ss::manual_clock::now() + timeout;
    auto expect_pass_fut = pipeline.write_and_debounce(
      model::controller_ntp, min_epoch, std::move(included_batches), deadline);

    // Make sure that both write requests are pending
    co_await sleep_until(
      10ms, [&] { return pipeline_accessor.write_requests_pending(2); });

    // Single L0 object that contains data from all write requests
    // should be "uploaded".
    auto res = co_await batcher_accessor.run_once();

    // Expect single L0 upload
    ASSERT_EQ_CORO(mock.keys.size(), 1);
    auto id = mock.keys.back();

    ASSERT_TRUE_CORO(res.has_value());

    auto [pass_result, fail_result] = co_await ss::when_all_succeed(
      std::move(expect_pass_fut), std::move(expect_fail_fut));

    ASSERT_TRUE_CORO(!fail_result.has_value());

    ASSERT_TRUE_CORO(pass_result.has_value());
    auto placeholder_batches = std::move(pass_result.value());

    ASSERT_EQ_CORO(placeholder_batches.extents.size(), expected_num_batches);
    for (const cloud_topics::extent_meta& ext : placeholder_batches.extents) {
        auto sid = cloud_topics::object_path_factory::level_zero_path(ext.id);
        ASSERT_EQ_CORO(sid, id);
    }
}

TEST_CORO(batcher_test, chunk_splitting_balances_upload_sizes) {
    scoped_config cfg;
    // Use a small threshold so test data splits into multiple chunks.
    cfg.get("cloud_topics_produce_batching_size_threshold")
      .set_value(size_t{4096});

    remote_mock mock;
    mock.expect_upload_object_repeatedly();

    cloud_storage_clients::bucket_name bucket("foo");
    cloud_topics::l0::write_pipeline<ss::manual_clock> pipeline;
    static_cluster_services cluster_services;
    cloud_topics::l0::batcher<ss::manual_clock> batcher(
      pipeline.register_write_pipeline_stage(),
      bucket,
      mock,
      &cluster_services);
    cloud_topics::l0::write_pipeline_accessor pipeline_accessor{
      .pipeline = &pipeline,
    };

    // Push several write requests. Each has 1 batch with 10 records
    // (~3KB serialized), so 6 requests total ~18KB. With threshold=4096
    // this should produce multiple balanced chunks.
    const int num_requests = 6;
    std::vector<
      ss::future<std::expected<cloud_topics::upload_meta, std::error_code>>>
      futures;

    const auto timeout = 10s;
    auto deadline = ss::manual_clock::now() + timeout;

    for (int i = 0; i < num_requests; i++) {
        auto [_, records, batches] = get_random_batches(1, 10);
        futures.push_back(pipeline.write_and_debounce(
          model::controller_ntp, min_epoch, std::move(batches), deadline));
    }

    // Wait for all write requests to be staged in the pipeline
    // before starting the batcher. subscribe() checks pre-existing
    // pending data, so bg_controller_loop's wait_next will return
    // immediately seeing all requests at once.
    co_await sleep_until(10ms, [&] {
        return pipeline_accessor.write_requests_pending(num_requests);
    });

    // Start the batcher — bg_controller_loop will pull all 6 requests
    // in one batch and split them into balanced chunks.
    co_await batcher.start();

    // Wait for all write request futures to resolve (the batcher
    // loop processes chunks via spawn_with_gate, which sets the
    // promises on each write request).
    auto results = co_await ss::when_all_succeed(std::move(futures));
    for (auto& res : results) {
        ASSERT_TRUE_CORO(res.has_value());
    }

    co_await batcher.stop();

    // Multiple uploads should happen since total data exceeds threshold.
    ASSERT_GT_CORO(mock.payloads.size(), size_t{1});

    // Verify uploads are balanced: no upload should be excessively small
    // compared to the average.
    size_t total_payload = 0;
    for (const auto& p : mock.payloads) {
        total_payload += p.size();
    }
    size_t avg_size = total_payload / mock.payloads.size();
    for (size_t i = 0; i < mock.payloads.size(); i++) {
        EXPECT_GE(mock.payloads[i].size(), avg_size / 3)
          << "Upload " << i << " size " << mock.payloads[i].size()
          << " is too small relative to average " << avg_size;
    }
}
