/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/read_merge/read_merge.h"

#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "config/configuration.h"
#include "ssx/future-util.h"

#include <algorithm>
#include <exception>

namespace cloud_topics::l0 {

constexpr size_t max_bytes_per_iter = 10_MiB;

template<class Clock>
read_merge<Clock>::read_merge(read_pipeline<Clock>::stage s)
  : _pipeline_stage(s)
  , _probe(config::shard_local_cfg().disable_metrics())
  , _in_flight_sem(
      config::shard_local_cfg().cloud_storage_max_connections(),
      "l0/read_merge") {}

template<class Clock>
ss::future<> read_merge<Clock>::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_loop(); });
    return ss::now();
}

template<class Clock>
ss::future<> read_merge<Clock>::stop() {
    // Signal all waiters so they don't hang during shutdown.
    // Use errc::shutting_down so waiters propagate the error
    // instead of pushing requests to the next stage.
    for (auto& [id, promise] : _in_flight) {
        if (promise.available()) {
            continue;
        }
        promise.set_value(errc::shutting_down);
    }
    _in_flight.clear();
    _in_flight_sem.broken();
    co_await _gate.close();
}

template<class Clock>
ss::future<> read_merge<Clock>::bg_loop() {
    auto holder = _gate.hold();
    while (!_pipeline_stage.stopped()) {
        auto fut = co_await ss::coroutine::as_future(
          _pipeline_stage.pull_fetch_requests(max_bytes_per_iter));

        if (fut.failed()) {
            auto e = fut.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read merge stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read merge failed to pull requests: {}",
              e);
            continue;
        }
        auto fut_res = std::move(fut).get();
        if (!fut_res.has_value()) {
            auto err = fut_res.error();
            if (err == errc::shutting_down) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read merge stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read merge received error pulling requests: {}",
              fut_res.error());
            continue;
        }
        auto to_process = std::move(fut_res.value());
        auto queue = std::move(to_process.requests);

        while (!queue.empty()) {
            auto units_fut = co_await ss::coroutine::as_future(
              ss::get_units(
                _in_flight_sem,
                1,
                _pipeline_stage.get_root_rtc().root_abort_source()));

            if (units_fut.failed()) {
                auto ex = units_fut.get_exception();
                vlog(
                  _pipeline_stage.logger().debug,
                  "read_merge is shutting down: {}",
                  ex);
                for (auto& req : queue) {
                    req.set_value(errc::shutting_down);
                }
                break;
            }
            auto req = &queue.front();
            queue.pop_front();
            ssx::spawn_with_gate(
              _gate, [this, req, u = std::move(units_fut.get())]() mutable {
                  return process_single_request(req, std::move(u));
              });
        }
    }
}

template<class Clock>
ss::future<> read_merge<Clock>::process_single_request(
  read_request<Clock>* req, semaphore_units units) {
    try {
        vassert(
          req->query.meta.size() > 0, "Empty read queries are not allowed");

        _probe.register_request_in(req->query.output_size_estimate);

        // The request is expected to target a single L0 object
        // (read_fanout upstream splits multi-object requests).
        auto id = req->query.meta.front().id;
        dassert(
          std::ranges::all_of(
            req->query.meta, [&id](const auto& m) { return m.id == id; }),
          "read_merge expects all extents to target the same object");

        auto it = _in_flight.find(id);
        if (it != _in_flight.end()) {
            // Another request for the same object is already in flight.
            // Wait for it to complete — on success the download will
            // have populated the cache; on failure we propagate the error.
            vlog(
              req->rtc_logger.debug,
              "Merging read for object {}, waiting for in-flight "
              "download",
              id);
            auto sf = it->second.get_shared_future();
            auto result = co_await std::move(sf);

            if (_pipeline_stage.stopped()) {
                req->set_value(errc::shutting_down);
                co_return;
            }

            if (result.has_value()) {
                vlog(
                  req->rtc_logger.debug,
                  "In-flight download for object {} failed with {}, "
                  "propagating error",
                  id,
                  result.value());
                req->set_value(result.value());
                co_return;
            }

            // The first download succeeded. Push our request to the
            // next stage where it will hit cache.
            _pipeline_stage.push_next_stage(*req);
            co_return;
        }

        // First request for this object. Register in the in-flight map.
        auto [insert_it, inserted] = _in_flight.emplace(
          id, ss::shared_promise<std::optional<errc>>());
        vassert(inserted, "object_id must not already exist in the map");

        // Create a proxy request to push to the next stage.
        dataplane_query query{
          .output_size_estimate = req->query.output_size_estimate,
          .meta = req->query.meta.copy(),
        };

        auto size_estimate = query.output_size_estimate;
        auto proxy = ss::make_lw_shared<read_request<Clock>>(
          req->ntp,
          std::move(query),
          req->expiration_time,
          &_pipeline_stage.get_root_rtc(),
          req->stage);

        _probe.register_request_out(size_estimate);
        _pipeline_stage.push_next_stage(*proxy);

        using request_fut_t
          = ss::future<typename read_request<Clock>::response_t>;
        auto holder = _gate.hold();
        proxy->response.get_future()
          .then_wrapped(
            [this, id, proxy, h = std::move(holder), u = std::move(units)](
              request_fut_t fut) mutable -> request_fut_t {
                auto map_it = _in_flight.find(id);
                if (fut.failed()) {
                    // Proxy request failed with an exception.
                    // Signal waiters with an error.
                    if (
                      map_it != _in_flight.end()
                      && !map_it->second.available()) {
                        map_it->second.set_value(errc::unexpected_failure);
                        _in_flight.erase(map_it);
                    }
                    return ss::make_exception_future<
                      typename read_request<Clock>::response_t>(
                      fut.get_exception());
                }
                auto result = fut.get();
                std::optional<errc> error;
                if (!result.has_value()) {
                    error = result.error();
                }
                if (map_it != _in_flight.end() && !map_it->second.available()) {
                    map_it->second.set_value(error);
                    _in_flight.erase(map_it);
                }
                return ss::make_ready_future<
                  typename read_request<Clock>::response_t>(std::move(result));
            })
          .forward_to(std::move(req->response));

    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            vlog(req->rtc_logger.debug, "Read merge shutting down");
            req->set_value(errc::shutting_down);
            co_return;
        }
        vlog(
          req->rtc_logger.error, "Unexpected exception in read merge: {}", ex);
        req->set_value(errc::unexpected_failure);
    }
}

template class read_merge<ss::lowres_clock>;
template class read_merge<ss::manual_clock>;

} // namespace cloud_topics::l0
