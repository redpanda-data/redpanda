/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/throttler/throttler.h"

#include "base/unreachable.h"
#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/write_pipeline.h"
#include "cloud_topics/core/write_request.h"
#include "cloud_topics/logger.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <limits>

namespace experimental::cloud_topics {

template<class Clock>
throttler<Clock>::throttler(
  size_t tput_limit, core::write_pipeline<Clock>::stage s)
  : _write_tput_tb(tput_limit, "ct:throttler")
  , _my_stage(std::move(s)) {}

template<class Clock>
ss::future<> throttler<Clock>::start() {
    ssx::spawn_with_gate(
      _gate, [this] { return bg_throttle_write_pipeline(); });
    return ss::now();
}

template<class Clock>
ss::future<> throttler<Clock>::stop() {
    _as.request_abort();
    co_await _gate.close();
}

template<class Clock>
void throttler<Clock>::throttle_tput(size_t overshoot) {
    auto list = _my_stage.pull_write_requests(overshoot);
    chunked_vector<write_req_ptr> tmp;
    for (auto& wr : list.requests) {
        tmp.push_back(wr.weak_from_this());
    }

    // [list.ready content | old throttled elements]
    std::for_each(tmp.begin(), tmp.end(), [this](write_req_ptr req) {
        if (req.get() == nullptr) {
            return;
        }
        // Every element stays in the list while it's throttled and then goes
        // back to the pipeline though 'backfill' method. The method just
        // pushes the element into the list without counting bytes (the bytes
        // were already counted at this point). It is safe to background the
        // future because we have the memory limit.
        vlog(
          cd_log.info,
          "Throttling write request, size = {} bytes, {} units available",
          req->size_bytes(),
          _write_tput_tb.available());

        _throttle_by_tput++;
        _outstanding_throttled_requests++;
        ssx::background = _write_tput_tb.maybe_throttle(req->size_bytes(), _as)
                            .finally([this, req, h = _gate.hold()]() noexcept {
                                auto r = req.get();
                                if (r != nullptr) {
                                    _my_stage.push_next_stage(*r);
                                }
                                _outstanding_throttled_requests--;
                            });
    });
}

template<class Clock>
ss::future<result<size_t>>
throttler<Clock>::throttle_write_pipeline_once(size_t prev_total_size) {
    size_t total_bytes = prev_total_size;
    auto ev = co_await _my_stage.wait_next(&_as);
    if (ev.has_error()) {
        co_return ev.error();
    }

    auto event = ev.value();

    // We got the write_request notification
    vassert(
      total_bytes <= event.total_write_bytes,
      "New total_bytes value {} is smaller than the previous one {}. The "
      "value is a counter and shouldn't go back.",
      event.total_write_bytes,
      total_bytes);

    co_return apply_throttle(prev_total_size, event);
}

template<class Clock>
size_t throttler<Clock>::apply_throttle(
  size_t prev_total_size, const core::event& event) {
    // NOTE: this method shouldn't be a coroutine to guarantee that there are no
    // scheduling points inside it.
    size_t total_bytes = prev_total_size;
    auto new_bytes = event.total_write_bytes - total_bytes;
    vlog(
      cd_log.debug,
      "Throttler event: total bytes: {}, pending bytes: {}, new bytes: {}, "
      "available: {}",
      event.total_write_bytes,
      event.pending_write_bytes,
      new_bytes,
      _write_tput_tb.available());
    total_bytes = event.total_write_bytes;
    if (!_write_tput_tb.try_throttle(new_bytes)) {
        throttle_tput(new_bytes);
    }
    // Advance all write requests which are not throttled to next stage
    _my_stage.process(
      [](const core::write_request<Clock>&) noexcept
      -> checked<core::request_processing_result, errc> {
          return core::request_processing_result::advance_and_continue;
      });
    _total_events++;
    return total_bytes;
}

template<class Clock>
ss::future<> throttler<Clock>::bg_throttle_write_pipeline() {
    auto h = _gate.hold();
    size_t total_bytes{0};
    while (!_as.abort_requested()) {
        auto fut = co_await ss::coroutine::as_future(
          throttle_write_pipeline_once(total_bytes));
        if (fut.failed()) {
            vlog(
              cd_log.error,
              "Pipeline throttling error: {}",
              fut.get_exception());
        } else {
            auto res = fut.get();
            if (res.has_error()) {
                vlog(
                  cd_log.error, "Pipeline throttling error: {}", res.error());
            } else {
                total_bytes = res.value();
            }
        }
    }
}

template class throttler<ss::manual_clock>;
template class throttler<ss::lowres_clock>;
} // namespace experimental::cloud_topics
