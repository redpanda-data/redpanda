/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/read_debounce/read_debounce.h"

#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "container/chunked_vector.h"
#include "ssx/checkpoint_mutex.h"
#include "ssx/future-util.h"

#include <chrono>
#include <exception>

namespace cloud_topics::l0 {

constexpr size_t max_bytes_per_iter = 10_MiB;
constexpr auto debounce_interval = std::chrono::milliseconds(250);

template<class Clock>
read_debounce<Clock>::read_debounce(read_pipeline<Clock>::stage s)
  : _pipeline_stage(s) {}

template<class Clock>
ss::future<> read_debounce<Clock>::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_loop(); });
    return ss::now();
}

template<class Clock>
ss::future<> read_debounce<Clock>::stop() {
    for (auto& state : _in_flight) {
        state.lock.broken();
    }
    co_await _gate.close();
}

template<class Clock>
ss::future<> read_debounce<Clock>::bg_loop() {
    auto holder = _gate.hold();
    while (!_pipeline_stage.stopped()) {
        // Pick up new requests as fast as possible.
        // Proxy them forward.
        auto fut = co_await ss::coroutine::as_future(
          _pipeline_stage.pull_fetch_requests(max_bytes_per_iter));

        if (fut.failed()) {
            auto e = fut.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read debounce stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read debounce failed to pull requests: {}",
              e);
            continue;
        }
        auto fut_res = std::move(fut).get();
        if (!fut_res.has_value()) {
            auto err = fut_res.error();
            if (err == errc::shutting_down) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read debounce stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read debounce received error pulling requests: {}",
              fut_res.error());
            continue;
        }
        auto to_process = std::move(fut_res.value());
        auto queue = std::move(to_process.requests);
        while (!queue.empty()) {
            auto req = &queue.front();
            queue.pop_front();
            // Safe because the requests are capped by memory use.
            ssx::spawn_with_gate(_gate, [this, req]() mutable {
                return process_single_request(req);
            });
        }
    }
}

template<class Clock>
ss::future<>
read_debounce<Clock>::process_single_request(read_request<Clock>* req) {
    // The request is expected to target a single L0 object.
    try {
        // The metadata is supposed to refer to a single L0 object.
        // If it doesn't there will be no error but the debouncing
        // could be inefficient. Because of that read_debounce should
        // always be paired with the read_fanout. The req->query.meta
        // may still have more than one extent.
        vassert(
          req->query.meta.size() > 0, "Empty read queries are not allowed");

        // Add L0 object UUID to the map of in-flight requests so the subsequent
        // requests could be debounced
        auto id = req->query.meta.front().id.name;
        auto hash = absl::Hash<uuid_t>{}(id);
        auto ix = hash % debounce_hash_size;
        auto u = _in_flight.at(ix).lock.try_get_units();
        if (!u.has_value() && !_pipeline_stage.stopped()) {
            try {
                u = co_await _in_flight.at(ix).lock.get_units(
                  debounce_interval);
            } catch (const ss::semaphore_timed_out&) {
                vlog(
                  req->rtc_logger.debug,
                  "Lock timed out, id: {}, proceeding anyway",
                  id);
            }
        } else if (_pipeline_stage.stopped()) {
            co_return;
        }

        // Here, it's not guaranteed that 'u' will actually have
        // units. It is possible for the 'get_units' call to time out
        // leaving 'u' uninitialized. This is intentional because we don't
        // want to block requests indefinitely. The 'get_units' call is
        // supposed to debounce requests for limited amount of time.
        // In case if 'u' is nullopt we may trigger same download twice
        // which is not a problem for correctness.
        // It's expected that the majority of GetObject requests will be
        // fulfilled within the debounce_interval.
        dataplane_query query{
          .output_size_estimate = req->query.output_size_estimate,
          .meta = req->query.meta.copy(),
        };

        auto proxy = ss::make_lw_shared<read_request<Clock>>(
          req->ntp,
          std::move(query),
          req->expiration_time,
          &_pipeline_stage.get_root_rtc(),
          req->stage);

        _pipeline_stage.push_next_stage(*proxy);

        auto holder = _gate.hold();
        proxy->response.get_future()
          .finally([proxy, u = std::move(u), h = std::move(holder)] mutable {
              // finally is used to capture 'u' and 'proxy'
              // while the request is fulfilled.
              // This call exits shortly but 'proxy' should
              // live until the request is running. The value
              // of 'u' could be 'nullopt'.
              u.reset();
          })
          .forward_to(std::move(req->response));
        // At this point it's guaranteed that the req->response
        // promise will be set.
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            vlog(req->rtc_logger.debug, "Read debounce shutting down");
            req->set_value(errc::shutting_down);
            co_return;
        }
        vlog(
          req->rtc_logger.error,
          "Unexpected exception in read debounce: {}",
          ex);
        req->set_value(errc::unexpected_failure);
    }
}

template class read_debounce<ss::lowres_clock>;
template class read_debounce<ss::manual_clock>;

} // namespace cloud_topics::l0
