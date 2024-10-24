// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "bytes/random.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_topics/batcher/batcher.h"
#include "cloud_topics/dl_placeholder.h"
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
#include "storage/record_batch_utils.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
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

namespace cloud_topics = experimental::cloud_topics;

struct reader_with_content {
    chunked_vector<bytes> keys;
    chunked_vector<bytes> records;
    model::record_batch_reader reader;
};

reader_with_content
get_random_reader(int num_batches, int num_records) { // NOLINT
    chunked_vector<bytes> keys;
    chunked_vector<bytes> records;
    ss::chunked_fifo<model::record_batch> fifo;
    model::offset offset{0};
    for (int i = 0; i < num_batches; i++) {
        // Build a record batch
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(offset));

        for (int r = 0; r < num_records; r++) {
            auto k = random_generators::make_iobuf(32);
            auto v = random_generators::make_iobuf(256);
            keys.push_back(iobuf_to_bytes(k.copy()));
            records.push_back(iobuf_to_bytes(v.copy()));
            builder.add_raw_kv(std::move(k), std::move(v));
            offset += 1;
        }
        fifo.push_back(std::move(builder).build());
    }
    auto reader = model::make_fragmented_memory_record_batch_reader(
      std::move(fifo));
    return {
      .keys = std::move(keys),
      .records = std::move(records),
      .reader = std::move(reader),
    };
}

namespace experimental::cloud_topics {
struct batcher_accessor {
    ss::future<result<bool>> run_once() noexcept { return batcher->run_once(); }

    // Returns true if the write request is in the `_pending` collection
    bool write_requests_pending(size_t n) {
        return batcher->_pending.size() == n;
    }

    cloud_topics::batcher<ss::manual_clock>* batcher;
};
} // namespace experimental::cloud_topics

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

inline cloud_topics::dl_placeholder
parse_placeholder_batch(model::record_batch batch) {
    iobuf payload = std::move(batch).release_data();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    return serde::from_iobuf<cloud_topics::dl_placeholder>(std::move(value));
}

TEST_CORO(batcher_test, single_write_request) {
    remote_mock mock;
    cloud_storage_clients::bucket_name bucket("foo");
    cloud_topics::batcher<ss::manual_clock> batcher(bucket, mock);
    cloud_topics::batcher_accessor accessor{
      .batcher = &batcher,
    };
    int num_batches = 10;
    auto [_, records, reader] = get_random_reader(num_batches, 10);
    // Expect single upload to be made
    mock.expect_upload_object(records);

    const auto timeout = 1s;
    auto fut = batcher.write_and_debounce(
      model::controller_ntp, std::move(reader), timeout);

    // Make sure the write request is in the _pending list
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });

    auto res = co_await accessor.run_once();

    ASSERT_TRUE_CORO(res.has_value());

    auto write_res = co_await std::move(fut);
    ASSERT_TRUE_CORO(write_res.has_value());

    // Expect single L0 upload
    ASSERT_EQ_CORO(mock.keys.size(), 1);
    auto id = mock.keys.back();

    // Check that uuid in the placeholder can be used to
    // access the data in S3.
    auto placeholder_batches = co_await model::consume_reader_to_memory(
      std::move(write_res.value()), model::no_timeout);
    ASSERT_EQ_CORO(placeholder_batches.size(), num_batches);
    for (const model::record_batch& batch : placeholder_batches) {
        auto placeholder = parse_placeholder_batch(batch.copy());
        // TODO: revisit this code when the object path format will change
        auto sid = ssx::sformat("{}", placeholder.id);
        ASSERT_EQ_CORO(sid, id());
    }
}

TEST_CORO(batcher_test, many_write_requests) {
    remote_mock mock;
    cloud_storage_clients::bucket_name bucket("foo");
    cloud_topics::batcher<ss::manual_clock> batcher(bucket, mock);
    cloud_topics::batcher_accessor accessor{
      .batcher = &batcher,
    };

    std::vector<size_t> expected_num_batches = {10, 20, 10};
    auto [_1, records1, reader1] = get_random_reader(10, 10);
    auto [_2, records2, reader2] = get_random_reader(20, 10);
    auto [_3, records3, reader3] = get_random_reader(10, 20);

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
    std::vector<ss::future<result<model::record_batch_reader>>> futures;
    futures.push_back(batcher.write_and_debounce(
      model::controller_ntp, std::move(reader1), timeout));
    futures.push_back(batcher.write_and_debounce(
      model::controller_ntp, std::move(reader2), timeout));
    futures.push_back(batcher.write_and_debounce(
      model::controller_ntp, std::move(reader3), timeout));

    // Make sure that all write requests are in the _pending list
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(3); });

    // Single L0 object that contains data from all write requests
    // should be "uploaded".
    auto res = co_await accessor.run_once();

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
        auto placeholder_batches = co_await model::consume_reader_to_memory(
          std::move(write_res.value()), model::no_timeout);
        ASSERT_EQ_CORO(placeholder_batches.size(), expected_num_batches.at(ix));
        for (const model::record_batch& batch : placeholder_batches) {
            auto placeholder = parse_placeholder_batch(batch.copy());
            // TODO: revisit this code when the object path format will change
            auto sid = ssx::sformat("{}", placeholder.id);
            ASSERT_EQ_CORO(sid, id());
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
    cloud_topics::batcher<ss::manual_clock> batcher(bucket, mock);
    cloud_topics::batcher_accessor accessor{
      .batcher = &batcher,
    };

    int expected_num_batches = 33;
    int expected_num_records = 33;
    auto [_1, included_records, included_reader] = get_random_reader(
      expected_num_batches, expected_num_records);
    auto [_2, timedout_records, timedout_reader] = get_random_reader(1, 1);

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
    auto expect_fail_fut = batcher.write_and_debounce(
      model::controller_ntp, std::move(timedout_reader), timeout);

    // Let time pass to invalidate the first enqueued write request
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(1); });
    ss::manual_clock::advance(timeout);

    auto expect_pass_fut = batcher.write_and_debounce(
      model::controller_ntp, std::move(included_reader), timeout);

    // Make sure that both write requests are pending
    co_await sleep_until(
      10ms, [&] { return accessor.write_requests_pending(2); });

    // Single L0 object that contains data from all write requests
    // should be "uploaded".
    auto res = co_await accessor.run_once();

    // Expect single L0 upload
    ASSERT_EQ_CORO(mock.keys.size(), 1);
    auto id = mock.keys.back();

    ASSERT_TRUE_CORO(res.has_value());

    auto [pass_result, fail_result] = co_await ss::when_all_succeed(
      std::move(expect_pass_fut), std::move(expect_fail_fut));

    ASSERT_TRUE_CORO(fail_result.has_error());

    ASSERT_TRUE_CORO(pass_result.has_value());
    auto placeholder_batches = co_await model::consume_reader_to_memory(
      std::move(pass_result.value()), model::no_timeout);

    ASSERT_EQ_CORO(placeholder_batches.size(), expected_num_batches);
    for (const model::record_batch& batch : placeholder_batches) {
        auto placeholder = parse_placeholder_batch(batch.copy());
        // TODO: revisit this code when the object path format will change
        auto sid = ssx::sformat("{}", placeholder.id);
        ASSERT_EQ_CORO(sid, id());
    }
}

// TODO: add more tests
// - behaviour in case if pending write request sizes exceed L0 object size
// limit
