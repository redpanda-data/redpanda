/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/read_pipeline.h"

#include "base/units.h"
#include "cloud_topics/level_zero/pipeline/circuit_breaker.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/abort_source.h"
#include "utils/human.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <chrono>
#include <exception>
#include <variant>

namespace cloud_topics::l0 {

// Memory limit used in tests (when cluster config is disabled)
static constexpr size_t max_memory_when_disabled = 100 * 1024 * 1024;

namespace {
size_t get_cloud_topics_l0_read_path_memory() {
    return memory_groups().cloud_topics_memory() > 0
             // TODO: take L1 into account.
             ? memory_groups().cloud_topics_memory() / 2
             : max_memory_when_disabled;
}
} // namespace

template<class Clock>
void read_pipeline<Clock>::stage::push_next_stage(
  read_request<Clock>& r, bool signal) {
    _parent->reenqueue(r, signal);
}

template<class Clock>
read_pipeline<Clock>::read_pipeline()
  : _mem_quota_capacity(get_cloud_topics_l0_read_path_memory())
  , _mem_quota(_mem_quota_capacity, "read-pipeline")
  // TODO: use config parameter
  , _breaker(10, std::chrono::seconds(1))
  , _probe(
      "read",
      config::shard_local_cfg().disable_metrics(),
      config::shard_local_cfg().disable_public_metrics()) {}

template<class Clock>
ss::future<std::expected<dataplane_query_result, std::error_code>>
read_pipeline<Clock>::make_reader(
  model::ntp ntp,
  dataplane_query query,
  timestamp_t timeout,
  model::opt_abort_source_t caller_as) {
    auto h = this->hold_gate();

    /*
     * if caller provides an abort source, combine it with rtc
     */
    std::optional<ssx::composite_abort_source> combined_as;
    auto& as = [this, &caller_as, &combined_as] -> ss::abort_source& {
        auto& rtc_as = this->get_root_rtc().root_abort_source();
        if (caller_as.has_value()) {
            combined_as.emplace(caller_as->get(), rtc_as);
            return combined_as.value().as();
        }
        return rtc_as;
    }();

    auto size_estimate = query.output_size_estimate;
    _probe.register_request();
    _probe.set_memory_usage_gauge(_current_size + size_estimate);
    auto lat_probe = _probe.register_request_processing_time();
    auto err_fallback = ss::defer([this] { _probe.register_request_error(); });
    std::optional<
      ss::semaphore_units<ss::named_semaphore_exception_factory, Clock>>
      half_open_units;
    switch (_breaker.state()) {
    case circuit_breaker_state::open:
        break;
    case circuit_breaker_state::half_open:
        // If the circuit breaker is half open acquire units twice.
        // Possibly, we will have to use different mechanism here.
        half_open_units = ss::try_get_units(_mem_quota, size_estimate);
        if (!half_open_units) {
            // Track the time we are waiting for memory as memory pressure event
            auto measure = _probe.register_memory_pressure_blocked(
              size_estimate);
            half_open_units = co_await ss::get_units(
              _mem_quota, size_estimate, as);
        }
        break;
    case circuit_breaker_state::closed:
        err_fallback.cancel();
        _probe.register_request_timeout();
        co_return std::unexpected(errc::timeout);
    }

    // TODO: add timeout
    auto units = ss::try_get_units(_mem_quota, size_estimate);
    if (!units) {
        auto measure = _probe.register_memory_pressure_blocked(size_estimate);
        units = co_await ss::get_units(_mem_quota, size_estimate, as);
    }
    _current_size += size_estimate;

    // The read request is stored on the stack of the
    // fiber until the 'response' promise is set.

    auto d = ss::defer(
      [this, size_estimate] { _current_size -= size_estimate; });

    auto stage = this->first_stage();

    l0::read_request<Clock> request(
      std::move(ntp), std::move(query), timeout, &this->get_root_rtc(), stage);

    vlog(
      request.rtc_logger.trace,
      "read_pipeline.make_reader called with {}, (timeout: {})",
      size_estimate,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        timeout - Clock::now()));

    auto fut = request.response.get_future();
    this->get_pending().push_back(request);

    // Notify all active event_filter instances that new item is enqueued
    this->signal(stage);

    if (this->stopped()) {
        err_fallback.cancel();
        co_return std::unexpected(errc::shutting_down);
    }
    auto res = co_await std::move(fut);

    if (!res.has_value()) {
        if (res.error() == errc::timeout) {
            err_fallback.cancel();
            _probe.register_request_timeout();
        }
        co_return std::unexpected(make_error_code(res.error()));
    }
    err_fallback.cancel();
    _probe.register_request_completed();
    for (auto& r : res.value().results) {
        _probe.register_bytes_out(r.size_bytes());
    }
    co_return std::move(res.value());
}

template<class Clock>
read_pipeline<Clock>::read_requests_list
read_pipeline<Clock>::get_fetch_requests(
  size_t max_bytes, pipeline_stage stage) {
    // First remove timed out write request to avoid returning them
    this->remove_timed_out_requests();
    auto& logger = this->logger();
    auto& pending = this->get_pending();

    vlog(
      logger.debug, "get_fetch_requests called with max_bytes = {}", max_bytes);

    read_requests_list result(this, stage);
    size_t acc_size = 0;

    auto it = pending.begin();
    for (; it != pending.end();) {
        if (it->stage != stage) {
            it++;
            continue;
        }
        auto sz = it->query.output_size_estimate;
        acc_size += sz;
        vlog(
          it->rtc_logger.trace,
          "get_fetch_requests processing req for {}, size estimate: {}",
          it->ntp,
          acc_size);
        // Always include the first request even if it exceeds max_bytes
        // to avoid stalling the pipeline with oversized requests
        if (acc_size >= max_bytes && !result.requests.empty()) {
            break;
        }
        auto& el = *it;
        it++;
        el._hook.unlink();
        result.requests.push_back(el);
    }
    result.complete = std::none_of(
      it, pending.end(), [stage](const auto& r) { return r.stage == stage; });
    vlog(
      logger.debug,
      "get_fetch_requests returned {} requests which are querying {} ({}B)",
      result.requests.size(),
      human::bytes(acc_size),
      acc_size);
    return result;
}

template<class Clock>
void read_pipeline<Clock>::register_pipeline_error(errc e) {
    /// Register error related to the system in general (no disk space,
    /// network is down, etc). Ignore errors which are related to partition.
    /// The idea is that errors which may affect any read request should
    /// be reaching the circuit breaker.
    vlog(this->logger().debug, "registered error {}", e);
    switch (e) {
    case errc::success:
    case errc::timeout:
    case errc::upload_failure:
    case errc::failed_to_get_epoch:
    case errc::cache_write_error:
    case errc::download_not_found:
    case errc::shutting_down:
        break;
    case errc::download_failure:
    case errc::slow_down:
    case errc::unexpected_failure:
    case errc::cache_read_error:
        // These errors may indicate that some
        // resource is oversaturated. They can
        // potentially throttle the read path.
        _breaker.register_error();
        break;
    }
}

template<class Clock>
void read_pipeline<Clock>::reenqueue(read_request<Clock>& r, bool signal) {
    if (r._hook.is_linked()) {
        r._hook.unlink();
    }
    if (r.has_expired()) {
        vlog(r.rtc_logger.debug, "Read request has expired");
        r.set_value(errc::timeout);
    } else {
        auto next = this->next_stage(r.stage);
        vlog(
          r.rtc_logger.debug,
          "Read request is returned, stage will be propagated from {} to {} {}",
          r.stage,
          next,
          signal ? "with signal" : "without signal");
        // Move all re-enqueued requests to the next stage automatically
        // and notify the corresponding event filter.
        r.stage = next;
        this->get_pending().push_back(r);
        if (signal) {
            this->signal(r.stage);
        }
    }
}

template<class Clock>
void read_pipeline<Clock>::signal(pipeline_stage stage) {
    this->do_signal(
      stage, event_type::new_read_request, _current_size, _bytes_total);
}

template<class Clock>
event read_pipeline<Clock>::trigger_event(pipeline_stage stage) {
    return event{
      .stage = stage,
      .type = event_type::new_read_request,
      .pending_read_bytes = _current_size,
      .total_read_bytes = _bytes_total,
    };
}

template class read_pipeline<ss::lowres_clock>;
template class read_pipeline<ss::manual_clock>;

} // namespace cloud_topics::l0
