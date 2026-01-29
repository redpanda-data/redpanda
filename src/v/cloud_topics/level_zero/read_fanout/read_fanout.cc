/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/read_fanout/read_fanout.h"

#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "container/chunked_vector.h"
#include "ssx/future-util.h"

namespace cloud_topics::l0 {

constexpr size_t max_bytes_per_iter = 10_MiB;

read_fanout::read_fanout(l0::read_pipeline<>::stage s)
  : _pipeline_stage(s) {}

ss::future<> read_fanout::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_process(); });
    return ss::now();
}
ss::future<> read_fanout::stop() { co_await _gate.close(); }

ss::future<> read_fanout::bg_process() {
    auto holder = _gate.hold();
    while (!_pipeline_stage.stopped()) {
        auto fut = co_await ss::coroutine::as_future(
          _pipeline_stage.pull_fetch_requests(max_bytes_per_iter));

        if (fut.failed()) {
            auto e = fut.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read fanout stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read fanout failed to pull requests: {}",
              e);
            continue;
        }
        auto fut_res = std::move(fut).get();
        if (!fut_res.has_value()) {
            auto err = fut_res.error();
            if (err == errc::shutting_down) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read fanout stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read fanout received error pulling requests: {}",
              fut_res.error());
            continue;
        }
        auto to_process = std::move(fut_res.value());
        auto queue = std::move(to_process.requests);
        while (!queue.empty()) {
            auto req = &queue.front();
            queue.pop_front();
            // It's safe to spawn fiber per request because the memory usage
            // is limited by the read pipeline memory quota.
            ssx::spawn_with_gate(_gate, [this, req]() mutable {
                return process_single_request(req);
            });
        }
    }
}

read_fanout::stats read_fanout::get_stats() const noexcept { return _stats; }

ss::future<> read_fanout::process_single_request(l0::read_request<>* req) {
    try {
        _stats.requests_in++;
        if (req->query.meta.size() <= 1) {
            // Fast path
            _stats.requests_out++;
            _pipeline_stage.push_next_stage(*req);
            co_return;
        }

        const chunked_vector<extent_meta>& metadata = req->query.meta;
        using ret_future_t
          = ss::future<std::expected<dataplane_query_result, errc>>;
        chunked_vector<ret_future_t> futures;
        std::optional<dataplane_query> curr_query;
        curr_query.emplace();
        auto timeout = req->expiration_time;
        for (const auto& meta : metadata) {
            if (
              curr_query->meta.empty()
              || curr_query->meta.back().id == meta.id) {
                curr_query->output_size_estimate += meta.byte_range_size();
                curr_query->meta.push_back(meta);
                continue;
            }
            vassert(
              !curr_query->meta.empty(), "extent list shouldn't be empty");

            auto proxy = ss::make_lw_shared<l0::read_request<>>(
              req->ntp,
              std::move(curr_query.value()),
              timeout,
              &_pipeline_stage.get_root_rtc(),
              req->stage);

            auto fut = proxy->response.get_future().finally([proxy] {});

            // start proxy request
            _pipeline_stage.push_next_stage(*proxy);

            futures.emplace_back(std::move(fut));
            curr_query.emplace();
            curr_query->output_size_estimate = meta.byte_range_size();
            curr_query->meta.push_back(meta);
        }
        if (!curr_query->meta.empty()) {
            auto proxy = ss::make_lw_shared<l0::read_request<>>(
              req->ntp,
              std::move(curr_query.value()),
              timeout,
              &_pipeline_stage.get_root_rtc(),
              req->stage);
            auto fut = proxy->response.get_future().finally([proxy] {});
            // start proxy request
            _pipeline_stage.push_next_stage(*proxy);
            futures.push_back(std::move(fut));
        }
        auto fut_res = co_await ss::when_all(futures.begin(), futures.end());

        // First process all exception, propagate the first one to the source
        // request Then process error codes, if there are any errors propagate
        // it. If there are multiple errors the shutdown error gets priority,
        // otherwise the first error code is propagated.
        std::exception_ptr first_exception;
        for (auto& fut : fut_res) {
            if (fut.failed()) {
                auto err = fut.get_exception();
                vlog(
                  req->rtc_logger.warn,
                  "Materialize operation failed: {}",
                  err);
                if (!first_exception) {
                    first_exception = err;
                }
            }
        }
        if (first_exception) {
            vlog(
              req->rtc_logger.warn,
              "Materialize operation failed, propagating exception {}",
              first_exception);
            _stats.requests_fail += fut_res.size();
            req->set_value(errc::unexpected_failure);
            co_return;
        }
        chunked_vector<model::record_batch> results;
        for (auto& fut : fut_res) {
            auto res = fut.get();
            if (!res.has_value()) {
                vlog(
                  req->rtc_logger.warn,
                  "Materialize operation returned error: {}",
                  res.error());
                _stats.requests_fail += fut_res.size();
                req->set_value(res.error());
                co_return;
            }
            std::move(
              res.value().results.begin(),
              res.value().results.end(),
              std::back_inserter(results));
        }
        vlog(
          req->rtc_logger.debug,
          "Materialize operation succeeded, {} batches materialized",
          results.size());

        _stats.requests_out += fut_res.size();
        req->set_value(dataplane_query_result{.results = std::move(results)});
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            vlog(
              req->rtc_logger.debug,
              "Read fanout stopping processing due to shutdown");
            req->set_value(errc::shutting_down);
            co_return;
        }
        vlog(
          req->rtc_logger.error, "Unexpected exception in read fanout: {}", ex);
        req->set_value(errc::unexpected_failure);
    }
}

} // namespace cloud_topics::l0
