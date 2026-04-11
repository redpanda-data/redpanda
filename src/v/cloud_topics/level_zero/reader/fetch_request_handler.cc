/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/fetch_request_handler.h"

#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "cloud_topics/level_zero/reader/materialized_extent_reader.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/internal/timers.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <exception>

namespace cloud_topics::l0 {

fetch_handler::fetch_handler(
  l0::read_pipeline<>::stage pipeline_stage,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* remote,
  cloud_io::basic_cache_service_api<>* cache)
  : _bucket(std::move(bucket))
  , _remote(remote)
  , _cache(cache)
  , _rtc(&pipeline_stage.get_root_rtc())
  , _logger(cd_log, _rtc, "ct:l0_fetch_handler")
  , _pipeline_stage(pipeline_stage) {}

ss::future<> fetch_handler::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_process_requests(); });
    return ss::now();
}

ss::future<> fetch_handler::stop() { co_await _gate.close(); }

ss::future<> fetch_handler::bg_process_requests() {
    while (!_rtc.root_abort_source().abort_requested()) {
        auto fut = co_await ss::coroutine::as_future(process_requests());
        if (fut.failed()) {
            auto e = fut.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  _logger.debug,
                  "Got shutdown error while resolving the request: "
                  "{}",
                  e);
                co_return;
            } else {
                // Unexpected exception failure
                vlog(
                  _logger.error,
                  "Got unexpected failure while resolving the request: {}",
                  e);
                _pipeline_stage.register_pipeline_error(
                  errc::unexpected_failure);
            }
        } else {
            auto res = fut.get();
            if (!res.has_value()) {
                if (res.error() == errc::shutting_down) {
                    vlog(_logger.debug, "Shutting down");
                    co_return;
                } else {
                    // Other types of errors are logged inside
                    // the 'process_request'
                    _pipeline_stage.register_pipeline_error(res.error());
                }
            } else {
                auto msg = res.value()
                             ? "no work, l0_fetch_handler will be suspended"
                             : "l0_fetch_handler will not be suspended";
                vlog(_logger.trace, "{}", msg);
            }
        }
    }
}

ss::future<> fetch_handler::process_single_request(l0::read_request<>* req) {
    auto h = _gate.hold();
    auto auto_dispose = ss::defer([req] {
        // Handle situation when the request is not handled correctly
        // during shutdown or in any other case.
        vlog(req->rtc_logger.error, "Auto-dispose triggered");
        req->set_value(errc::unexpected_failure);
    });
    std::optional<model::record_batch_reader> prepared;
    std::optional<chunked_vector<model::tx_range>> aborted_tx;
    try {
        auto meta = std::move(req->query.meta);
        auto extent = co_await ss::coroutine::as_future(
          materialize_placeholders(
            _bucket,
            std::move(meta),
            *_remote,
            *_cache,
            req->query.allow_mat_failure,
            req->rtc,
            req->rtc_logger));

        if (extent.failed()) {
            auto ex = extent.get_exception();
            vlog(
              req->rtc_logger.warn,
              "Failed to materialize placeholders, error: {}",
              ex);
            req->set_value(errc::download_failure);
            auto_dispose.cancel();
            co_return;
        }

        auto [res, probe] = extent.get();
        // The registration happens even for failed requests because
        // failed requests are consuming resources (API calls).
        _pipeline_stage.register_micro_probe(probe);
        if (!res.has_value()) {
            vlog(
              req->rtc_logger.warn,
              "Failed to materialize placeholders, error: {} ({})",
              res.error(),
              res.error().message());
            std::error_code ec = res.error();
            if (ec.category() == error_category()) {
                req->set_value(static_cast<errc>(res.error().value()));
            } else {
                req->set_value(errc::unexpected_failure);
            }
            auto_dispose.cancel();
            co_return;
        }

        auto data = std::move(res.value());
        if (data.empty()) {
            vlog(req->rtc_logger.debug, "Empty response");
        }

        auto_dispose.cancel();
        req->set_value(l0::dataplane_query_result{.results = std::move(data)});

    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            vlog(req->rtc_logger.debug, "Failed to fetch due to shutdown");
            req->set_value(errc::shutting_down);
        } else {
            vlog(
              req->rtc_logger.error,
              "Failed to fetch, exception: {}",
              std::current_exception());
            req->set_value(errc::unexpected_failure);
        }
        auto_dispose.cancel();
        co_return;
    }
    vlog(req->rtc_logger.debug, "Request processing completed");
}

ss::future<checked<bool, errc>> fetch_handler::process_requests() {
    // The limit here defines how much memory can be used by all
    // fetch requests on a shard. The pipeline has its own limit
    // but it should only be used to avoid OOM'ing on read_request
    // instances.
    // TODO: use proper limit
    auto to_process = co_await _pipeline_stage.pull_fetch_requests(100_MiB);
    if (!to_process.has_value()) {
        co_return to_process.error();
    }
    vlog(
      _logger.trace,
      "got {} requests from the pipeline, completeness: {}",
      to_process.value().requests.size(),
      to_process.value().complete);
    chunked_vector<ss::future<>> bg;
    for (auto& req : to_process.value().requests) {
        bg.push_back(process_single_request(&req));
    }
    co_await ss::when_all_succeed(bg.begin(), bg.end());
    co_return to_process.value().complete;
}

} // namespace cloud_topics::l0
