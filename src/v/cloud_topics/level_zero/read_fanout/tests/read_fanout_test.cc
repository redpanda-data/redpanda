// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/read_fanout/read_fanout.h"
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>
#include <limits>

using namespace std::chrono_literals;

static ss::logger test_log("L0_read_fanout_test");

namespace cloud_topics {

// Special object id that triggers failure
static auto id_to_fail = object_id{
  .epoch = cluster_epoch{0},
  .name = uuid_t::create(),
};

struct fake_fetch_handler {
    explicit fake_fetch_handler(l0::read_pipeline<>& p)
      : stage(p.register_read_pipeline_stage()) {
        vlog(stage.logger().info, "fake fetch handler created");
    }

    ss::future<> start() {
        ssx::background = bg_run();
        return ss::now();
    }

    ss::future<> stop() {
        vlog(stage.logger().info, "fake fetch handler stop");
        co_await _gate.close();
    }

    ss::future<> bg_run() {
        auto h = _gate.hold();
        while (!stage.stopped()) {
            vlog(stage.logger().debug, "fake_fetch_handler subscribe");

            auto result = co_await stage.pull_fetch_requests(
              std::numeric_limits<size_t>::max());

            if (!result.has_value()) {
                // Expected during shutdown
                co_return;
            }

            vlog(
              stage.logger().debug,
              "fake_fetch_handler got {} requests",
              result.value().requests.size());

            for (auto& r : result.value().requests) {
                // The expectation is that all extents in this request
                // will share the same object id.
                object_id id = r.query.meta.front().id;
                chunked_vector<model::record_batch> batches;
                for (size_t i = 0; i < r.query.meta.size(); i++) {
                    if (r.query.meta.at(i).id != id) {
                        throw std::runtime_error(
                          "request targets more than one object");
                    }
                    model::test::record_batch_spec spec{};
                    spec.offset = _next_offset;
                    spec.count = 1;
                    spec.records = 1;
                    auto rb = model::test::make_random_batch(spec);
                    batches.push_back(std::move(rb));
                    _next_offset = model::next_offset(_next_offset);
                }
                if (id_to_fail == id) {
                    // The failure was injected
                    r.set_value(errc::timeout);
                } else {
                    r.set_value({{std::move(batches)}});
                }
            }
        }
    }

    model::offset _next_offset{0};
    l0::read_pipeline<>::stage stage;
    ss::gate _gate;
};

} // namespace cloud_topics

using namespace cloud_topics;

class read_fanout_fixture : public seastar_test {
public:
    ss::future<> start() {
        co_await pipeline.start();

        co_await fanout.start(ss::sharded_parameter([this] {
            return pipeline.local().register_read_pipeline_stage();
        }));

        co_await fanout.invoke_on_all(
          [](l0::read_fanout& s) { return s.start(); });

        co_await fetch_handler.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }));

        co_await fetch_handler.invoke_on_all(
          [](fake_fetch_handler& handler) { return handler.start(); });
    }

    ss::future<> stop() {
        co_await pipeline.invoke_on_all([](auto& s) { return s.shutdown(); });
        co_await fetch_handler.stop();
        co_await fanout.stop();
        co_await pipeline.stop();
    }

    ss::sharded<l0::read_pipeline<>> pipeline;
    ss::sharded<l0::read_fanout> fanout;
    ss::sharded<fake_fetch_handler> fetch_handler;
};

static const model::topic_namespace
  test_topic(model::kafka_namespace, model::topic("tapioca"));

static const model::ntp
  test_ntp0(test_topic.ns, test_topic.tp, model::partition_id(0));

static const model::ntp
  test_ntp1(test_topic.ns, test_topic.tp, model::partition_id(1));

TEST_F_CORO(read_fanout_fixture, test_bypass) {
    // Check that the read request is bypassed when it has single extent
    co_await start();
    l0::dataplane_query query;
    query.output_size_estimate = 1_MiB;
    query.meta.push_back(
      extent_meta{.byte_range_size = byte_range_size_t{1_MiB}});
    auto result = co_await pipeline.local().make_reader(
      test_ntp0, std::move(query), ss::lowres_clock::now() + 10s);
    ASSERT_TRUE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.value().results.size(), 1);
    ASSERT_EQ_CORO(
      result.value().results.front().header().base_offset, model::offset{0});
    auto [in, out, fail] = this->fanout.local().get_stats();
    ASSERT_EQ_CORO(in, out);
    ASSERT_EQ_CORO(in, 1);
    ASSERT_EQ_CORO(fail, 0);
    co_await stop();
}

TEST_F_CORO(read_fanout_fixture, test_scatter_gather) {
    // Check that the read request that targets several objects
    // is split into several smaller ones. Check that number of
    // requests matches number of objects.
    co_await start();

    auto obj1_uuid = uuid_t::create();
    auto obj2_uuid = uuid_t::create();
    auto obj3_uuid = uuid_t::create();

    l0::dataplane_query query;
    query.output_size_estimate = 4_MiB;
    // Obj1, one extent
    query.meta.push_back(
      extent_meta{
        .id = object_id{.epoch = cluster_epoch{0}, .name = obj1_uuid},
        .byte_range_size = byte_range_size_t{1_MiB}});
    // Obj2, two extents
    query.meta.push_back(
      extent_meta{
        .id = object_id{.epoch = cluster_epoch{0}, .name = obj2_uuid},
        .byte_range_size = byte_range_size_t{1_MiB}});
    query.meta.push_back(
      extent_meta{
        .id = object_id{.epoch = cluster_epoch{0}, .name = obj2_uuid},
        .byte_range_size = byte_range_size_t{1_MiB}});
    // Obj3, one extent
    query.meta.push_back(
      extent_meta{
        .id = object_id{.epoch = cluster_epoch{0}, .name = obj3_uuid},
        .byte_range_size = byte_range_size_t{1_MiB}});

    auto result = co_await pipeline.local().make_reader(
      test_ntp0, std::move(query), ss::lowres_clock::now() + 10s);

    ASSERT_TRUE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.value().results.size(), 4);
    ASSERT_EQ_CORO(
      result.value().results.front().header().base_offset, model::offset{0});
    ASSERT_EQ_CORO(
      result.value().results.back().header().base_offset, model::offset{3});

    auto [in, out, fail] = this->fanout.local().get_stats();
    ASSERT_NE_CORO(in, out);
    ASSERT_EQ_CORO(in, 1);
    ASSERT_EQ_CORO(out, 3);
    ASSERT_EQ_CORO(fail, 0);
    co_await stop();
}

TEST_F_CORO(read_fanout_fixture, test_failure) {
    // Check that the read request only fails as a whole.
    // No partial failures are allowed for now.
    co_await start();

    auto obj1_uuid = uuid_t::create();
    auto obj2_uuid = uuid_t::create();

    l0::dataplane_query query;
    query.output_size_estimate = 3_MiB;

    // Obj1, one extent
    query.meta.push_back(
      extent_meta{
        .id = object_id{.epoch = cluster_epoch{0}, .name = obj1_uuid},
        .byte_range_size = byte_range_size_t{1_MiB}});

    // Obj2, two extents (one injects failure)
    query.meta.push_back(
      extent_meta{
        .id = object_id{.epoch = cluster_epoch{0}, .name = obj2_uuid},
        .byte_range_size = byte_range_size_t{1_MiB}});
    query.meta.push_back(
      extent_meta{
        .id = id_to_fail, .byte_range_size = byte_range_size_t{1_MiB}});

    auto result = co_await pipeline.local().make_reader(
      test_ntp0, std::move(query), ss::lowres_clock::now() + 10s);

    ASSERT_TRUE_CORO(!result.has_value());

    auto [in, out, fail] = this->fanout.local().get_stats();
    ASSERT_NE_CORO(in, out);
    ASSERT_EQ_CORO(in, 1);
    ASSERT_EQ_CORO(out, 0);
    // Shows number of failed proxy requests
    ASSERT_EQ_CORO(fail, 3);
    co_await stop();
}
