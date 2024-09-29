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
#include "cloud_topics/batcher/batcher.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "remote_mock.h"
#include "storage/record_batch_builder.h"
#include "storage/record_batch_utils.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
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

TEST_CORO(batcher_test, single_write_request) {
    remote_mock<> mock;
    cloud_storage_clients::bucket_name bucket("foo");
    cloud_topics::batcher<ss::manual_clock> batcher(bucket, mock);

    // TODO: control the bg loop manually in the test
    co_await batcher.start();
    ss::manual_clock::advance(500ms);

    auto [keys, records, reader] = get_random_reader(10, 10);

    const auto timeout = 1s;
    auto fut = batcher.write_and_debounce(
      model::controller_ntp, std::move(reader), timeout);
    while (!fut.available()) {
        ss::manual_clock::advance(100ms);
        co_await ss::yield();
    }
    auto res = co_await std::move(fut);
    co_await batcher.stop();
}

// TODO: add more tests
