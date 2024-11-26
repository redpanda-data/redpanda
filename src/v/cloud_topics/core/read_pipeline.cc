/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/core/read_pipeline.h"

#include "base/units.h"
#include "cloud_topics/core/circuit_breaker.h"
#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/read_request.h"
#include "cloud_topics/logger.h"
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

namespace experimental::cloud_topics::core {

static constexpr size_t max_pipeline_stages = 10;

template<class Clock>
read_pipeline<Clock>::read_pipeline()
  // TODO: use config parameter
  : _mem_quota(10_MiB, "read-pipeline")
  , _root_rtc(_as)
  , _logger(cd_log, _root_rtc, "ct:read_pipeline")
  , _stages(max_pipeline_stages)
  // TODO: use config parameter
  , _breaker(10, std::chrono::seconds(1)) {}

template<class Clock>
ss::future<result<read_request_fetch_result>> read_pipeline<Clock>::make_reader(
  model::ntp ntp,
  storage::log_reader_config cfg,
  std::chrono::milliseconds timeout) {
    auto h = _gate.hold();

    std::optional<
      ss::semaphore_units<ss::named_semaphore_exception_factory, Clock>>
      half_open_units;
    switch (_breaker.state()) {
    case circuit_breaker_state::open:
        break;
    case circuit_breaker_state::half_open:
        // If the circuit breaker is half open acquire units twice.
        // Possibly, we will have to use different mechanism here.
        half_open_units = co_await ss::get_units(_mem_quota, 1_MiB, _as);
        break;
    case circuit_breaker_state::closed:
        co_return errc::timeout;
    }

    // TODO: add timeout
    auto units = co_await ss::get_units(_mem_quota, 1_MiB, _as);

    // TODO: fixme
    _current_size += 1_MiB; // TODO: use actual memory quota based on estimate
                            // (readahead + buffer size)

    // The read request is stored on the stack of the
    // fiber until the 'response' promise is set.

    auto d = ss::defer([this] {
        // TODO: fixme, use proper size estimate
        _current_size -= 1_MiB;
    });

    auto stage = _stages.first_stage();
    core::read_request<Clock> request(
      std::move(ntp), cfg, timeout, &_root_rtc, stage);
    vlog(
      request.rtc_logger.trace,
      "read_pipeline.make_reader called with {}, (timeout: {})",
      cfg,
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count());
    auto fut = request.response.get_future();
    _pending.push_back(request);

    // Notify all active event_filter instances that new item is enqueued
    signal(stage);

    auto res = co_await std::move(fut);
    if (res.has_error()) {
        co_return res.error();
    }

    vassert(
      std::holds_alternative<read_request_fetch_result>(res.value()),
      "Fetch result expected");

    co_return std::get<read_request_fetch_result>(std::move(res.value()));
}

template<class Clock>
ss::future<result<read_request_timequery_result>>
read_pipeline<Clock>::timequery(
  model::ntp, storage::timequery_config, std::chrono::milliseconds) {
    // FIXME: Not implemented
    co_return errc::unexpected_failure;
}

template<class Clock>
ss::future<event> read_pipeline<Clock>::subscribe(
  event_filter<Clock>& flt, ss::abort_source& as) noexcept {
    auto sub = as.subscribe(
      [&flt](const std::optional<std::exception_ptr>&) noexcept {
          // This code just cancels event subscription but it can be improved
          // by transferring exception to the subscriber.
          flt.cancel();
      });
    co_return co_await subscribe(flt);
}

template<class Clock>
read_pipeline<Clock>::read_requests_list
read_pipeline<Clock>::get_fetch_requests(
  size_t max_bytes, pipeline_stage stage) {
    // First remove timed out write request to avoid returning them
    remove_timed_out_fetch_requests();

    vlog(
      _logger.debug,
      "get_fetch_requests called with max_bytes = {}",
      max_bytes);

    read_requests_list result{};
    size_t acc_size = 0;

    // The elements in the list are in the insertion order.
    auto it = _pending.begin();
    for (; it != _pending.end(); it++) {
        if (it->stage != stage) {
            continue;
        }
        if (it->is_timequery()) {
            continue;
        }
        // TODO: avoid copy
        auto cfg = it->get_log_reader_config();
        auto sz = cfg.max_bytes;
        acc_size += sz;
        vlog(
          it->rtc_logger.trace,
          "get_fetch_requests processing req for {}, config: {}, total size: "
          "{}",
          it->ntp,
          cfg,
          acc_size);
        if (acc_size >= max_bytes) {
            // Include last element
            it++;
            break;
        }
    }
    result.ready.splice(result.ready.end(), _pending, _pending.begin(), it);
    result.complete = _pending.empty();
    vlog(
      _logger.debug,
      "get_fetch_requests returned {} requests which are querying {} ({}B)",
      result.ready.size(),
      human::bytes(acc_size),
      acc_size);
    return result;
}

template<class Clock>
ss::future<event>
read_pipeline<Clock>::subscribe(event_filter<Clock>& flt) noexcept {
    _filters.push_back(flt);
    // If the pipeline already has some requests we need to set the future
    // eagerly
    bool found = false;
    for (auto& wr : _pending) {
        if (wr.stage == flt.get_stage()) {
            found = true;
            break;
        }
    }
    if (found) {
        signal(flt.get_stage());
    }
    auto ev = co_await ss::coroutine::as_future(flt.get_future());
    if (ev.failed()) {
        auto ep = ev.get_exception();
        if (ssx::is_shutdown_exception(ep)) {
            co_return event{.type = event_type::shutting_down};
        }
        // The only exception that can be thrown here is a shutdown
        // exception (broken_promise). We never set the promise to
        // any other exception.
        vassert(false, "Unexpected failure in the event subscription: {}", ep);
    }
    co_return ev.get();
}

template<class Clock>
ss::future<> read_pipeline<Clock>::stop() {
    _as.request_abort_ex(std::make_exception_ptr(pipeline_abort_requested()));
    auto fut = _gate.close();
    for (auto& f : _filters) {
        f.cancel();
    }
    co_await std::move(fut);
}

template<class Clock>
void read_pipeline<Clock>::remove_timed_out_fetch_requests() {
    chunked_vector<ss::weak_ptr<core::read_request<Clock>>> expired;
    for (auto& wr : _pending) {
        if (wr.has_expired()) {
            expired.push_back(wr.weak_from_this());
        }
    }
    if (!expired.empty()) {
        vlog(_logger.debug, "{} fetch requests have expired", expired.size());
    }
    for (auto& wp : expired) {
        if (wp != nullptr) {
            vlog(_logger.debug, "{} write requests have expired", wp->ntp);
            wp->_hook.unlink();
            wp->set_value(errc::timeout);
        }
    }
}

template<class Clock>
void read_pipeline<Clock>::signal(pipeline_stage stage) {
    vlog(_logger.debug, "read_pipeline:signal, stage: {}", stage);
    event ev{
      .stage = stage, .type = event_type::new_read_request,
      // TODO: specific fields
    };
    for (auto& f : _filters) {
        if (
          f.get_type() == event_type::new_read_request
          && f.get_stage() == stage) {
            // TODO: add logging
            f.trigger(ev);
        }
        // The cleanup is performed by the subscriber
    }
}

template<class Clock>
pipeline_stage read_pipeline<Clock>::register_pipeline_stage() noexcept {
    return _stages.register_pipeline_stage();
}

template<class Clock>
void read_pipeline<Clock>::register_pipeline_error(errc e) {
    /// Register error related to the system in general (no disk space,
    /// network is down, etc). Ignore errors which are related to partition.
    /// The idea is that errors which may affect any read request should
    /// be reaching the circuit breaker.
    vlog(_logger.debug, "registered error {}", e);
    switch (e) {
    case errc::success:
    case errc::timeout:
    case errc::upload_failure:
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

template class read_pipeline<ss::lowres_clock>;
template class read_pipeline<ss::manual_clock>;

} // namespace experimental::cloud_topics::core
