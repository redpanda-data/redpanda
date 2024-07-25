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
#include "cloud_data/aggregated_uploader.h"
#include "cloud_io/io_result.h"
#include "cloud_storage_api_mocks.h"
#include "cloud_storage_clients/client_pool.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "storage/record_batch_utils.h"
#include "test_utils/scoped_config.h"
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

using namespace cloud_storage;
using namespace std::chrono_literals;
namespace csc = cloud_storage_clients;

namespace cloud_data {
using uploader_t = aggregated_uploader<ss::manual_clock>;
using upload_result_src_t = ss::noncopyable_function<cloud_io::upload_result()>;

struct aggregated_uploader_fixture : seastar_test {
    aggregated_uploader_fixture() {
        sc_cfg.get("cloud_storage_aggregated_log_upload_interval_ms")
          .set_value(std::chrono::milliseconds(1000));
    }
    ss::future<> SetUpAsync() override {
        co_await remote.start();

        co_await uploader.start(test_bucket, ss::sharded_parameter([this] {
                                    return std::ref(remote.local());
                                }));

        co_await uploader.invoke_on_all(
          [](uploader_t& s) { return s.start(); });

        co_return;
    }

    ss::future<> TearDownAsync() override {
        /*TODO: remove*/ vlog(test_log.info, "NEEDLE about to stop uploader");
        co_await uploader.stop();
        co_await remote.stop();
        /*TODO: remove*/ vlog(test_log.info, "NEEDLE about to stop remote");
        co_return;
    }

    size_t calculate_batches_size(
      const ss::circular_buffer<model::record_batch>& batches) {
        size_t result = 0;
        for (const auto& b : batches) {
            result += model::packed_record_batch_header_size;
            result += b.data().size_bytes();
        }
        return result;
    }

    ss::future<std::tuple<size_t, model::record_batch_reader>>
    make_batches(int count) {
        auto batches = co_await model::test::make_random_batches(
          model::offset(0), count);
        auto batches_size = calculate_batches_size(batches);
        auto rdr = model::make_memory_record_batch_reader(std::move(batches));
        co_return std::make_tuple(batches_size, std::move(rdr));
    }

    ss::future<>
    set_upload_object_result(unsigned shard, cloud_io::upload_result result) {
        using namespace ::testing;
        co_await ss::smp::submit_to(shard, [result, this] {
            remote.local().set_next_upload_result(result);
        });
    }

    csc::bucket_name test_bucket{"best_tucket"};
    ss::sharded<remote_mock<ss::manual_clock>> remote;
    ss::sharded<uploader_t> uploader;
    scoped_config sc_cfg;
};

// Advance manual clock and yield few times so background fibers
// have a chance to work
inline ss::future<> simulate_sleep(ss::manual_clock::duration duration) {
    ss::manual_clock::advance(duration);
    // Need this to make sure other fibers can have some progress
    co_await ss::sleep(1ms);
}

static ss::future<> write_and_debounce_random_batches(
  aggregated_uploader_fixture* fx, model::ntp expected_ntp, int num_batches) {
    auto [batches_size, rdr] = co_await fx->make_batches(num_batches);
    auto fut = fx->uploader.local().write_and_debounce(
      expected_ntp, std::move(rdr), 10s);
    vlog(test_log.info, "available: {}", fut.available());
    ASSERT_FALSE_CORO(fut.available());

    co_await simulate_sleep(100ms);
    vlog(test_log.info, "available: {}", fut.available());
    ASSERT_FALSE_CORO(fut.available());

    int limit = 1000;
    while (!fut.available() && limit-- > 0) {
        co_await simulate_sleep(100ms);
    }
    vlog(test_log.info, "available: {}, limit: {}", fut.available(), limit);
    ASSERT_TRUE_CORO(fut.available());
    ASSERT_TRUE_CORO(!fut.failed());
}

TEST_F_CORO(aggregated_uploader_fixture, test_uploads_from_one_shard) {
    model::ntp expected_ntp(
      model::kafka_namespace, model::topic("optic"), model::partition_id(0));

    int num_uploads = ss::smp::count * 2;
    // All uploads are successful by default so no need to setup the
    // mock with any additional information.

    for (int i = 0; i < num_uploads; i++) {
        vlog(test_log.info, "Produce request {}", i);
        co_await write_and_debounce_random_batches(this, expected_ntp, 10);
    }
    co_return;
}

TEST_F_CORO(
  aggregated_uploader_fixture, test_uploads_from_different_shards_sync) {
    model::ntp expected_ntp(
      model::kafka_namespace, model::topic("optic"), model::partition_id(0));

    int num_uploads = ss::smp::count * 2;
    // All uploads are successful by default so no need to setup the
    // mock with any additional information.

    for (int i = 0; i < num_uploads; i++) {
        auto shard = i % ss::smp::count;
        vlog(
          test_log.info, "Produce request: {}, producer shard: {}", i, shard);
        co_await ss::smp::submit_to(shard, [&] {
            return write_and_debounce_random_batches(this, expected_ntp, 10);
        });
    }
    co_return;
}

TEST_F_CORO(
  aggregated_uploader_fixture, test_uploads_from_different_shards_parallel) {
    int num_produce_requests = 10;
    // All uploads are successful by default so no need to setup the
    // mock with any additional information.

    std::vector<ss::future<>> futures;

    for (unsigned i = 0; i < ss::smp::count; i++) {
        auto shard = i % ss::smp::count;
        vlog(
          test_log.info,
          "Produce {} request on shard {}",
          num_produce_requests,
          shard);

        // Every shard produces 'num_produce_requests' batches in parallel. Each
        // batch is validated individually (the corresponding future should
        // become available for every produced batch).
        for (int u = 0; u < num_produce_requests; u++) {
            auto fut = ss::smp::submit_to(shard, [this, i] {
                model::ntp expected_ntp(
                  model::kafka_namespace,
                  model::topic("optic"),
                  model::partition_id(i));
                return write_and_debounce_random_batches(
                  this, expected_ntp, 10);
            });
            futures.push_back(std::move(fut));
        }
    }
    // This shouldn't throw given that we didn't inject any failures
    co_await ss::when_all_succeed(std::move(futures));
    co_return;
}

// TODO: check timeout handling (also implement timeout handling)

} // namespace cloud_data
