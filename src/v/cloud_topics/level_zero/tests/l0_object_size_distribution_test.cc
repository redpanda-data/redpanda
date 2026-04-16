/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/remote_api.h"
#include "cloud_topics/level_zero/batcher/batcher.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/write_request_scheduler/write_request_scheduler.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/coroutine/as_future.hh>

#include <gmock/gmock.h>

#include <limits>

using namespace std::chrono_literals;
using namespace cloud_topics;

static cloud_topics::cluster_epoch min_epoch{3840};
static const model::topic_namespace
  test_topic(model::kafka_namespace, model::topic("tapioca"));
static const model::ntp
  test_ntp0(test_topic.ns, test_topic.tp, model::partition_id(0));

namespace cloud_topics::l0 {
struct write_request_balancer_accessor {
    static void
    disable_background_loop(write_request_scheduler<seastar::manual_clock>* s) {
        s->_test_only_disable_background_loop = true;
    }
};
} // namespace cloud_topics::l0

class remote_mock final : public cloud_io::remote_api<ss::manual_clock> {
public:
    using reset_input_stream
      = cloud_io::remote_api<ss::manual_clock>::reset_input_stream;

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      download_object,
      (cloud_io::basic_download_request<ss::manual_clock>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      object_exists,
      (const cloud_storage_clients::bucket_name&,
       const cloud_storage_clients::object_key&,
       basic_retry_chain_node<ss::manual_clock>&,
       std::string_view),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_object,
      (cloud_io::basic_upload_request<ss::manual_clock>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::upload_result>,
      upload_stream,
      (cloud_io::basic_transfer_details<ss::manual_clock>,
       uint64_t,
       const reset_input_stream&,
       lazy_abort_source&,
       const std::string_view,
       std::optional<size_t>),
      (override));

    MOCK_METHOD(
      ss::future<cloud_io::download_result>,
      download_stream,
      (cloud_io::basic_transfer_details<ss::manual_clock>,
       const cloud_io::try_consume_stream&,
       const std::string_view,
       bool,
       std::optional<cloud_storage_clients::http_byte_range>,
       std::function<void(size_t)>),
      (override));
};

class static_cluster_services : public cloud_topics::cluster_services {
public:
    static_cluster_services() = default;
    seastar::future<cloud_topics::cluster_epoch>
    current_epoch(seastar::abort_source*) override {
        return seastar::make_ready_future<cloud_topics::cluster_epoch>(0);
    }
    seastar::future<> invalidate_epoch_below(cluster_epoch) override {
        return ss::now();
    }
};

class L0ObjectSizeDistFixture : public seastar_test {
public:
    ss::future<> start(bool disable_bg_loop) {
        co_await pipeline.start();

        /*
         * setup the write request scheduler
         */
        co_await scheduler.start(seastar::sharded_parameter([this] {
            return pipeline.local().register_write_pipeline_stage();
        }));

        co_await scheduler.invoke_on_all([&](auto& sched) {
            if (disable_bg_loop) {
                l0::write_request_balancer_accessor::disable_background_loop(
                  &sched);
            }
        });

        /*
         * setup the remote mock which will track the size of uploaded objects
         */
        co_await remote.start();
        co_await uploads.start();
        co_await remote.invoke_on_all([this](remote_mock& remote) {
            EXPECT_CALL(remote, upload_object(::testing::_))
              .WillRepeatedly([this](auto req) {
                  uploads.local().push_back(req.payload.size_bytes());
                  return seastar::make_ready_future<cloud_io::upload_result>(
                    cloud_io::upload_result::success);
              });
        });

        /*
         * start the services
         */
        co_await batcher.start(
          seastar::sharded_parameter([this] {
              return pipeline.local().register_write_pipeline_stage();
          }),
          cloud_storage_clients::bucket_name{"bucket0"},
          ss::sharded_parameter([this] { return std::ref(remote.local()); }),
          &cluster_services);

        co_await scheduler.invoke_on_all(
          [](auto& sched) { return sched.start(); });

        co_await batcher.invoke_on_all(
          [](auto& batcher) { return batcher.start(); });
    }

    ss::future<> stop() {
        co_await batcher.stop();
        co_await scheduler.stop();
        co_await remote.stop();
        co_await uploads.stop();
        co_await pipeline.stop();
    }

    ss::future<size_t> num_uploads() {
        return uploads.map_reduce0(
          [](const auto& uploads) { return uploads.size(); },
          size_t{0},
          std::plus<size_t>{});
    }

    ss::future<size_t> num_upload_bytes() {
        return uploads.map_reduce0(
          [](const auto& uploads) {
              return std::reduce(uploads.begin(), uploads.end());
          },
          size_t{0},
          std::plus<size_t>{});
    }

    static_cluster_services cluster_services;
    ss::sharded<remote_mock> remote;
    ss::sharded<l0::write_pipeline<seastar::manual_clock>> pipeline;
    ss::sharded<l0::write_request_scheduler<seastar::manual_clock>> scheduler;
    ss::sharded<l0::batcher<seastar::manual_clock>> batcher;
    ss::sharded<std::vector<size_t>> uploads;
};

TEST_F(L0ObjectSizeDistFixture, ThreeToOne) {
    /*
     * This test writes batches into the write pipeline from three different
     * cores and then expects that these are eventually grouped together and
     * uploaded as a single object by the scheduler/batcher.
     */
    ASSERT_EQ(seastar::smp::count, 3);
    start(false).get();

    const auto timeout = 1s;
    auto deadline = ss::manual_clock::now() + timeout;

    // build batches to upload to each core
    size_t total_size{0};
    std::vector<chunked_vector<model::record_batch>> batches;
    for (unsigned i = 0; i < seastar::smp::count; ++i) {
        batches.emplace_back();
        auto buf = model::test::make_random_batches().get();
        for (auto& b : buf) {
            total_size += b.size_bytes();
            batches.back().push_back(std::move(b));
        }
    }

    // upload the built batches from each core
    auto write_fut = pipeline.invoke_on_all([&](auto& p) {
        auto data = std::move(batches[seastar::this_shard_id()]);
        return p
          .write_and_debounce(test_ntp0, min_epoch, std::move(data), deadline)
          .discard_result();
    });

    // Allow requests to be propagated to the scheduler
    ss::sleep(5ms).get();

    // Advance time to trigger the scheduler
    ss::manual_clock::advance(300ms);

    // Give up the shard to allow the background fiber of the scheduler to run
    ss::sleep(1ms).get();

    // Wait for all write operations to complete
    std::move(write_fut).get();

    // we expect there to have only been one object uploaded for all the data
    ASSERT_EQ(num_uploads().get(), 1);

    // we expect the size of the object uploaded is at least as big as the
    // batches we submitted
    ASSERT_GE(num_upload_bytes().get(), total_size);

    stop().get();
}
