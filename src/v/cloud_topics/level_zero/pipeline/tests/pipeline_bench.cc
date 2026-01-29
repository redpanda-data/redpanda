/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "storage/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <cstdlib>

namespace ct = ::cloud_topics;

static cloud_topics::cluster_epoch min_epoch{3840};

struct read_pipeline_sink {
    explicit read_pipeline_sink(ct::l0::read_pipeline<>& p)
      : _my_stage(p.register_read_pipeline_stage())
      , _pipeline(&p) {}

    void start() { ssx::background = bg_loop(); }

    ss::future<> stop() { return _gate.close(); }

    ss::future<> bg_loop() {
        auto h = _gate.hold();
        while (!_pipeline->stopped()) {
            auto res = co_await _my_stage.pull_fetch_requests(0x100000);
            if (!res.has_value()) {
                throw std::system_error(res.error());
            }
            for (auto& req : res.value().requests) {
                req.set_value(ct::errc::success);
            }
        }
    }

    ct::l0::read_pipeline<>::stage _my_stage;
    ct::l0::read_pipeline<>* _pipeline;
    ss::gate _gate;
};

struct read_pipeline_bench {};

PERF_TEST_C(read_pipeline_bench, propagation_latency) {
    ct::l0::read_pipeline<> pipeline;
    read_pipeline_sink sink(pipeline);
    sink.start();
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(
      co_await pipeline.make_reader(
        model::controller_ntp,
        {},
        ss::lowres_clock::now() + std::chrono::milliseconds(10)));
    perf_tests::stop_measuring_time();

    co_await pipeline.stop();
    co_await sink.stop();
}

struct write_pipeline_sink {
    explicit write_pipeline_sink(ct::l0::write_pipeline<>& p)
      : _my_stage(p.register_write_pipeline_stage())
      , _pipeline(&p) {}

    void start() { ssx::background = bg_loop(); }

    ss::future<> stop() {
        _as.request_abort();
        return _gate.close();
    }

    ss::future<> bg_loop() {
        auto h = _gate.hold();
        while (!_as.abort_requested()) {
            ct::l0::event_filter<> flt(
              ct::l0::event_type::new_write_request, _my_stage.id());
            auto event = co_await _pipeline->subscribe(flt, _as);
            if (event.type == ct::l0::event_type::shutting_down) {
                break;
            }
            auto res = _my_stage.pull_write_requests(1);
            for (auto& req : res.requests) {
                req.set_value(ct::errc::success);
            }
        }
    }

    ct::l0::write_pipeline<>::stage _my_stage;
    ct::l0::write_pipeline<>* _pipeline;
    ss::gate _gate;
    ss::abort_source _as;
};

struct write_pipeline_bench {};

PERF_TEST_C(write_pipeline_bench, propagation_latency) {
    ct::l0::write_pipeline<> pipeline;
    write_pipeline_sink sink(pipeline);
    sink.start();

    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(
      co_await pipeline.write_and_debounce(
        model::controller_ntp,
        min_epoch,
        {},
        ss::lowres_clock::now() + std::chrono::milliseconds(10)));
    perf_tests::stop_measuring_time();

    co_await sink.stop();
}
