// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/pipeline/pipeline_actor.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/read_merge/read_merge.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "ssx/future-util.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <cstddef>
#include <limits>

using namespace std::chrono_literals;
namespace cloud_topics {

/// Mock fetch handler that processes requests with an optional delay
/// to simulate I/O latency. When delay is zero, requests complete
/// synchronously (original behavior).
struct fetch_handler : public l0::read_pipeline_actor<> {
    using actor_t = l0::read_pipeline_actor<>;

    explicit fetch_handler(
      l0::read_pipeline<>::stage s,
      std::chrono::microseconds delay = std::chrono::microseconds(0))
      : actor_t(std::move(s))
      , _delay(delay) {
        _batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .offset = model::offset(0),
            .allow_compression = false,
            .count = 1,
            .records = 1,
            .record_sizes = std::vector<size_t>{4096},
          });
    }

protected:
    ss::future<> process(l0::pipeline_notification) override {
        auto requests = this->stage().pull_fetch_requests_nowait(
          std::numeric_limits<size_t>::max());
        for (auto& r : requests.requests) {
            ssx::spawn_with_gate(this->_gate, [this, req = &r]() mutable {
                return process_single_request(req);
            });
        }
        co_return;
    }

    void on_error(std::exception_ptr) noexcept override {}

private:
    ss::future<> process_single_request(l0::read_request<>* req) {
        if (_delay.count() > 0) {
            co_await ss::sleep(_delay);
        }

        auto meta = std::move(req->query.meta);
        chunked_vector<model::record_batch> data;
        for (auto& m : meta) {
            std::ignore = m;
            data.push_back(_batch->copy());
        }
        req->set_value({{std::move(data)}});
    }

    std::optional<model::record_batch> _batch;
    std::chrono::microseconds _delay;
};
} // namespace cloud_topics

using namespace cloud_topics;

class read_merge_bench {
public:
    ss::future<> start(
      bool enable_merge,
      std::chrono::microseconds fetch_delay = std::chrono::microseconds(0)) {
        co_await pipeline.start();

        if (enable_merge) {
            co_await merge.start(ss::sharded_parameter([this] {
                return pipeline.local().register_read_pipeline_stage();
            }));

            co_await merge.invoke_on_all([](auto& f) { return f.start(); });
        }

        co_await sink.start(
          ss::sharded_parameter(
            [this] { return pipeline.local().register_read_pipeline_stage(); }),
          fetch_delay);

        co_await pipeline.invoke_on_all([this](auto& p) {
            if (merge.local_is_initialized()) {
                p.register_actor(&merge.local());
            }
            p.register_actor(&sink.local());
        });

        co_await sink.invoke_on_all([](auto& sink) { return sink.start(); });
    }

    ss::future<> stop() {
        co_await pipeline.stop();
        co_await sink.stop();
        if (merge.local_is_initialized()) {
            co_await merge.stop();
        }
    }

    // Run requests serially, each with a unique object_id
    ss::future<> test_run_unique(int num_requests) {
        perf_tests::start_measuring_time();
        for (int i = 0; i < num_requests; i++) {
            l0::dataplane_query query;
            query.output_size_estimate = 1_KiB;
            query.meta.push_back(
              extent_meta{
                .id = object_id{.name = uuid_t::create()},
                .byte_range_size = byte_range_size_t{1_KiB}});

            perf_tests::do_not_optimize(
              co_await pipeline.local().make_reader(
                model::controller_ntp,
                std::move(query),
                ss::lowres_clock::now() + 10s));
        }
        perf_tests::stop_measuring_time();
    }

    // Fire N concurrent requests all targeting the same object_id.
    // Measures the merge path where joiners wait on the shared_future.
    ss::future<> test_run_concurrent_same_object(int concurrency) {
        auto shared_id = object_id{.name = uuid_t::create()};
        chunked_vector<ss::future<
          std::expected<l0::dataplane_query_result, std::error_code>>>
          futures;

        perf_tests::start_measuring_time();
        for (int i = 0; i < concurrency; i++) {
            l0::dataplane_query query;
            query.output_size_estimate = 1_KiB;
            query.meta.push_back(
              extent_meta{
                .id = shared_id, .byte_range_size = byte_range_size_t{1_KiB}});

            futures.push_back(pipeline.local().make_reader(
              model::controller_ntp,
              std::move(query),
              ss::lowres_clock::now() + 10s));
        }
        auto results = co_await ss::when_all(futures.begin(), futures.end());
        perf_tests::do_not_optimize(results);
        perf_tests::stop_measuring_time();
    }

    // Fire N concurrent requests each targeting a unique object_id.
    // read_merge has no false collisions — all requests proceed in
    // parallel without blocking each other.
    ss::future<> test_run_concurrent_unique_objects(int concurrency) {
        chunked_vector<ss::future<
          std::expected<l0::dataplane_query_result, std::error_code>>>
          futures;

        perf_tests::start_measuring_time();
        for (int i = 0; i < concurrency; i++) {
            l0::dataplane_query query;
            query.output_size_estimate = 1_KiB;
            query.meta.push_back(
              extent_meta{
                .id = object_id{.name = uuid_t::create()},
                .byte_range_size = byte_range_size_t{1_KiB}});

            futures.push_back(pipeline.local().make_reader(
              model::controller_ntp,
              std::move(query),
              ss::lowres_clock::now() + 10s));
        }
        auto results = co_await ss::when_all(futures.begin(), futures.end());
        perf_tests::do_not_optimize(results);
        perf_tests::stop_measuring_time();
    }

    ss::sharded<cloud_topics::l0::read_pipeline<>> pipeline;
    ss::sharded<cloud_topics::l0::read_merge<>> merge;
    ss::sharded<cloud_topics::fetch_handler> sink;
};

PERF_TEST_C(read_merge_bench, baseline) {
    // Baseline: no middle stage, serial requests with unique objects.
    co_await start(false);
    co_await test_run_unique(100);
    co_await stop();
}

PERF_TEST_C(read_merge_bench, serial_unique_objects) {
    // Serial requests through read_merge, unique objects.
    // Measures per-request overhead of hash map lookup.
    co_await start(true);
    co_await test_run_unique(100);
    co_await stop();
}

PERF_TEST_C(read_merge_bench, concurrent_same_object) {
    // 50 concurrent requests for the same object through read_merge.
    // Only 1 proxy reaches fetch handler; 49 join on shared_future.
    co_await start(true, 1ms);
    co_await test_run_concurrent_same_object(50);
    co_await stop();
}

PERF_TEST_C(read_merge_bench, concurrent_unique_objects) {
    // 50 concurrent requests for different objects through read_merge.
    // No false collisions — all requests proceed fully in parallel.
    co_await start(true, 1ms);
    co_await test_run_concurrent_unique_objects(50);
    co_await stop();
}
