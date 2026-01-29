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
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "ssx/future-util.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <cstddef>
#include <exception>
#include <limits>

using namespace std::chrono_literals;
namespace cloud_topics {

/// The handler simulates L0 object downloads
/// by injecting sleeps.
struct fetch_handler {
    explicit fetch_handler(l0::read_pipeline<>& p, int concurrency)
      : stage(p.register_read_pipeline_stage())
      , _con(concurrency) {
        // Use same batch to reply to all materialization requests
        // to avoid regenerating it during the run.
        _batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .offset = model::offset(0),
            .allow_compression = false,
            .count = 1,
            .records = 1,
            .record_sizes = std::vector<size_t>{4096},
          });
    }

    ss::future<> start(std::chrono::milliseconds d) {
        _download_latency = d;
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
                // Expected during shutdown
                co_return;
            }

            for (auto& r : result.value().requests) {
                // Process every request concurrently because this is what
                // real fetch handler does.
                ssx::spawn_with_gate(
                  _gate, [this, &r] { return process_single_request(&r); });
            }
        }
    }

    ss::future<> process_single_request(l0::read_request<>* req) {
        auto auto_dispose = ss::defer(
          [req] { req->set_value(errc::unexpected_failure); });

        auto meta = std::move(req->query.meta);
        chunked_vector<model::record_batch> data;
        for (auto& m : meta) {
            std::ignore = m;
            data.push_back(_batch->copy());
            // Simulate download
            if (_download_latency != 0ms) {
                auto u = co_await ss::get_units(_con, 1);
                co_await ss::sleep(_download_latency);
            }
        }

        auto_dispose.cancel();

        req->set_value(l0::dataplane_query_result{.results = std::move(data)});
    }

    std::optional<model::record_batch> _batch;
    l0::read_pipeline<>::stage stage;
    ss::gate _gate;
    std::chrono::milliseconds _download_latency;
    // Semaphore used to simulate connection pool.
    // Up to 20 connections are allowed to run concurrently.
    // This mirrors the default in Redpanda.
    ss::semaphore _con;
};
} // namespace cloud_topics

using namespace cloud_topics;

class read_fanout_bench {
public:
    /// Start benchmark fixture.
    /// \param enable_fanout enables or disables read fanout
    /// \param concurrency defines max number of concurrent downloads
    /// \param dl_lat is a latency of the simulated object download
    ss::future<> start(
      bool enable_fanout, int concurrency, std::chrono::milliseconds dl_lat) {
        co_await pipeline.start();

        if (enable_fanout) {
            co_await fanout.start(ss::sharded_parameter([this] {
                return pipeline.local().register_read_pipeline_stage();
            }));

            co_await fanout.invoke_on_all([](auto& f) { return f.start(); });
        }

        co_await sink.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }),
          concurrency);

        co_await sink.invoke_on_all(
          [dl_lat](auto& sink) { return sink.start(dl_lat); });
    }

    ss::future<> stop() {
        co_await pipeline.stop();
        co_await sink.stop();
        if (fanout.local_is_initialized()) {
            co_await fanout.stop();
        }
    }

    /// Build one big vectorized query and then run it
    ss::future<> vectorized_test_run(int num_requests) {
        l0::dataplane_query query;
        query.output_size_estimate = 1_KiB;
        for (int i = 0; i < num_requests; i++) {
            query.meta.push_back(
              extent_meta{
                .id = object_id{.name = uuid_t::create()},
                .byte_range_size = byte_range_size_t{1_KiB}});
        }

        perf_tests::start_measuring_time();

        perf_tests::do_not_optimize(
          co_await pipeline.local().make_reader(
            model::controller_ntp,
            std::move(query),
            ss::lowres_clock::now() + std::chrono::seconds(10)));

        perf_tests::stop_measuring_time();
    }

    /// Run requests one after another serially
    ss::future<> serialized_test_run(int num_requests) {
        std::vector<ss::future<>> fut;
        for (int i = 0; i < num_requests; i++) {
            l0::dataplane_query query;
            query.output_size_estimate = 1_KiB;
            query.meta.push_back(
              extent_meta{
                .id = object_id{.name = uuid_t::create()},
                .byte_range_size = byte_range_size_t{1_KiB}});

            fut.push_back(
              pipeline.local()
                .make_reader(
                  model::controller_ntp,
                  std::move(query),
                  ss::lowres_clock::now() + std::chrono::seconds(10))
                .discard_result());
        }

        perf_tests::start_measuring_time();

        // The pipeline should run all requests concurrently.
        co_await ss::when_all_succeed(fut.begin(), fut.end());

        perf_tests::stop_measuring_time();
    }

    ss::sharded<cloud_topics::l0::read_pipeline<>> pipeline;
    ss::sharded<cloud_topics::l0::read_fanout> fanout;
    ss::sharded<cloud_topics::fetch_handler> sink;
};

PERF_TEST_C(read_fanout_bench, baseline) {
    // This is a baseline. It shows best performance that we can get.
    // Caching is not taken into account by the test.
    // The download latency is set to 50ms. The concurrency
    // is not limited.
    co_await start(false, 1000, 50ms);

    co_await serialized_test_run(100);

    co_await stop();
}

PERF_TEST_C(read_fanout_bench, serialized) {
    // This is supposed to be very slow.
    // A single vectorized read is issued. All downloads inside
    // it are going to be serialized because read_fanout component
    // is not added to the pipeline.
    co_await start(false, 100, 50ms);

    co_await vectorized_test_run(100);

    co_await stop();
}

PERF_TEST_C(read_fanout_bench, concurrent) {
    // This is supposed to match the baseline.
    // One big vectorized request will be issued but
    // the read_fanout component will scatter it into multiple
    // small requests that can run concurrently.
    co_await start(true, 100, 50ms);

    co_await vectorized_test_run(100);

    co_await stop();
}

PERF_TEST_C(read_fanout_bench, bypass) {
    // This should be as fast as baseline. The requests
    // could be executed concurrently, but we're adding one more
    // step (read_fanout) which should just bypass all requests
    // without adding latency.
    co_await start(true, 1000, 50ms);

    co_await serialized_test_run(100);

    co_await stop();
}

PERF_TEST_C(read_fanout_bench, baseline_noop) {
    // Run 100 requests serially without simulating the download
    // latency. This measures the roundtrip latency.
    co_await start(false, 1000, 0ms);
    co_await serialized_test_run(100);
    co_await stop();
}

PERF_TEST_C(read_fanout_bench, serialized_noop) {
    // Same but with read_fanout in the pipeline.
    co_await start(true, 1000, 0ms);
    co_await serialized_test_run(100);
    co_await stop();
}

PERF_TEST_C(read_fanout_bench, vectorized_noop) {
    // Run single vectorized request that touches 100 objects.
    // The latency should be similar to the baseline.
    co_await start(true, 1000, 0ms);
    co_await vectorized_test_run(100);
    co_await stop();
}
