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

namespace experimental::cloud_topics {

template<class Clock>
throttler<Clock>::throttler(
  size_t tput_limit, core::write_pipeline<Clock>& pipeline)
  : _pipeline(pipeline)
  , _write_tput_tb(tput_limit, "ct:throttler")
  , _my_stage(_pipeline.register_pipeline_stage()) {}

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
    auto list = _pipeline.get_write_requests(overshoot, _my_stage);
    chunked_vector<write_req_ptr> tmp;
    for (auto& wr : list.ready) {
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
                                    _pipeline.reenqueue(*r);
                                }
                                _outstanding_throttled_requests--;
                            });
    });
}

template<class Clock>
ss::future<result<size_t>>
throttler<Clock>::throttle_write_pipeline_once(size_t prev_total_size) {
    size_t total_bytes = prev_total_size;
    core::event_filter<Clock> filter(
      core::event_type::new_write_request, _my_stage);
    auto event = co_await _pipeline.subscribe(filter, _as);
    switch (event.type) {
    case core::event_type::shutting_down:
        co_return errc::shutting_down;
    case core::event_type::err_timedout:
    case core::event_type::new_read_request:
    case core::event_type::none:
        unreachable();
    case core::event_type::new_write_request:
        break;
    }

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
    _pipeline.process_stage(
      [](const core::write_request<Clock>&) noexcept
      -> checked<core::write_request_process_result, errc> {
          return outcome::success(core::write_request_process_result{
            .stop_iteration = ss::stop_iteration::no,
            .advance_next_stage = true,
          });
      },
      _my_stage);
    _total_events++;
    return total_bytes;
}

template<class Clock>
ss::future<> throttler<Clock>::bg_throttle_write_pipeline() {
    auto h = _gate.hold();
    size_t total_bytes{0};
    while (!_as.abort_requested()) {
        auto res = co_await ss::coroutine::as_future(
          throttle_write_pipeline_once(total_bytes));
        if (res.failed()) {
            vlog(
              cd_log.error,
              "Pipeline throttling error: {}",
              res.get_exception());
        } else if (res.get().has_error()) {
            vlog(cd_log.error, "Pipeline throttling error: {}", res.get());
        } else {
            total_bytes = res.get().value();
        }
    }
}

template class throttler<ss::manual_clock>;
template class throttler<ss::lowres_clock>;
} // namespace experimental::cloud_topics
