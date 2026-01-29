// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/read_request_scheduler/read_request_scheduler.h"
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

static ss::logger test_log("L0_read_scheduler_test");

namespace cloud_topics {

struct fetch_handler {
    explicit fetch_handler(l0::read_pipeline<>& p)
      : stage(p.register_read_pipeline_stage()) {}

    ss::future<> start() {
        ssx::background = bg_run();
        return ss::now();
    }

    ss::future<> stop() { co_await _gate.close(); }

    ss::future<> bg_run() {
        auto h = _gate.hold();
        while (!stage.stopped()) {
            auto result = co_await stage.pull_fetch_requests(
              std::numeric_limits<size_t>::max());
            if (!result.has_value()) {
                co_return;
            }

            // OK to iterate because there are no scheduling points below
            for (auto& r : result.value().requests) {
                requests_cnt++;
                auto it = injected_errors.find(r.query.meta.front().id.name);
                if (it != injected_errors.end()) {
                    r.set_value(it->second);
                } else {
                    chunked_vector<model::record_batch> batches;
                    for (size_t i = 0; i < r.query.meta.size(); i++) {
                        model::test::record_batch_spec spec{};
                        spec.count = 1;
                        spec.records = 1;
                        auto rb = model::test::make_random_batch(spec);
                        batches.push_back(std::move(rb));
                    }
                    r.set_value({{std::move(batches)}});
                }
            }
        }
    }

    l0::read_pipeline<>::stage stage;
    ss::gate _gate;
    size_t requests_cnt{0};
    std::map<uuid_t, errc> injected_errors;
};

} // namespace cloud_topics

namespace ct = cloud_topics;
namespace l0 = ct::l0;

class read_request_scheduler_fixture : public seastar_test {
public:
    ss::future<> start() {
        co_await pipeline.start();

        co_await scheduler.start(ss::sharded_parameter([this] {
            return pipeline.local().register_read_pipeline_stage();
        }));

        co_await scheduler.invoke_on_all(
          [](l0::read_request_scheduler& s) { return s.start(); });

        co_await fetch_handler.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }));

        co_await fetch_handler.invoke_on_all(
          [](ct::fetch_handler& handler) { return handler.start(); });
    }

    ss::future<> stop() {
        co_await pipeline.invoke_on_all([](auto& s) { return s.shutdown(); });
        co_await fetch_handler.stop();
        co_await scheduler.stop();
        co_await pipeline.stop();
    }

    ss::future<size_t> total_requests_cnt() {
        auto counters = co_await fetch_handler.map(
          [](ct::fetch_handler& h) { return h.requests_cnt; });
        size_t res = 0;
        for (auto i : counters) {
            res += i;
        }
        co_return res;
    }

    ss::future<> add_failure(uuid_t id, ct::errc err) {
        co_await fetch_handler.invoke_on_all(
          [id, err](ct::fetch_handler& h) { h.injected_errors[id] = err; });
    }

    ss::sharded<l0::read_pipeline<>> pipeline;
    ss::sharded<l0::read_request_scheduler> scheduler;
    ss::sharded<ct::fetch_handler> fetch_handler;
};

static const model::topic_namespace
  test_topic(model::kafka_namespace, model::topic("tapioca"));

static const model::ntp
  test_ntp0(test_topic.ns, test_topic.tp, model::partition_id(0));

static const model::ntp
  test_ntp1(test_topic.ns, test_topic.tp, model::partition_id(1));

TEST_F_CORO(read_request_scheduler_fixture, smoke_test) {
    co_await start();

    for (int i = 0; i < 100; i++) {
        l0::dataplane_query query;
        query.output_size_estimate = 1_KiB;

        query.meta.push_back(
          ct::extent_meta{
            .id = ct::
              object_id{.epoch = ct::cluster_epoch{0}, .name = uuid_t::create()},
            .byte_range_size = ct::byte_range_size_t{1_MiB}});

        auto result = co_await pipeline.local().make_reader(
          test_ntp0, std::move(query), ss::lowres_clock::now() + 10s);
        ASSERT_TRUE_CORO(result.has_value());
        ASSERT_EQ_CORO(result.value().results.size(), 1);
        ASSERT_EQ_CORO(
          result.value().results.front().header().base_offset,
          model::offset{0});
    }
    auto req_cnt = co_await total_requests_cnt();
    ASSERT_EQ_CORO(req_cnt, 100);
    co_await stop();
}

TEST_F_CORO(read_request_scheduler_fixture, error_propagation) {
    co_await start();

    auto failure_uuid = uuid_t::create();
    co_await add_failure(failure_uuid, ct::errc::download_failure);

    l0::dataplane_query query;
    query.output_size_estimate = 1_MiB;

    query.meta.push_back(
      ct::extent_meta{
        .id
        = ct::object_id{.epoch = ct::cluster_epoch{0}, .name = failure_uuid},
        .byte_range_size = ct::byte_range_size_t{1_MiB}});

    auto result = co_await pipeline.local().make_reader(
      test_ntp0, std::move(query), ss::lowres_clock::now() + 10s);

    ASSERT_FALSE_CORO(result.has_value());

    auto req_cnt = co_await total_requests_cnt();
    ASSERT_EQ_CORO(req_cnt, 1);
    co_await stop();
}
