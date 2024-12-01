/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/evictor/evictor.h"

#include "base/unreachable.h"
#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/write_pipeline.h"
#include "cloud_topics/core/write_request.h"
#include "cloud_topics/logger.h"

#include <seastar/core/loop.hh>
#include <seastar/coroutine/as_future.hh>

namespace experimental::cloud_topics {

evictor::evictor(size_t mem_limit, core::write_pipeline<>& pipeline)
  : _pipeline(pipeline)
  , _mem_limit(mem_limit)
  , _my_stage(_pipeline.register_pipeline_stage()) {}

ss::future<> evictor::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_evict_pipeline(); });
    return ss::now();
}

ss::future<> evictor::stop() {
    _as.request_abort();
    co_await _gate.close();
}

size_t evictor::throttle_memory(size_t current_bytes_pending) {
    if (current_bytes_pending <= _mem_limit) {
        // Fast path for the case when we're limited by tput
        // and not by memory usage.
        return 0;
    }
    auto overshoot = current_bytes_pending - _mem_limit;
    auto list = _pipeline.get_write_requests(overshoot, _my_stage);
    std::for_each(
      list.ready.begin(), list.ready.end(), [](core::write_request<>& req) {
          req.set_value(errc::slow_down);
      });
    _throttle_by_mem++;
    return list.size_bytes;
}

ss::future<result<size_t>> evictor::evict_once(size_t prev_total_size) {
    size_t total_bytes = prev_total_size;
    core::event_filter<> filter(core::event_type::new_write_request, _my_stage);
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

    co_return do_evict(prev_total_size, event);
}

size_t evictor::do_evict(size_t prev_total_size, const core::event& event) {
    size_t total_bytes = prev_total_size;
    auto new_bytes = event.total_write_bytes - total_bytes;

    vlog(
      cd_log.debug,
      "Evictor event: total bytes: {}, pending bytes: {}, new bytes: {}",
      event.total_write_bytes,
      event.pending_write_bytes,
      new_bytes);

    total_bytes = event.total_write_bytes;
    if (event.pending_write_bytes > _mem_limit) {
        size_t removed_bytes = throttle_memory(event.pending_write_bytes);
        vlog(
          cd_log.info,
          "Evictor have removed {} bytes from the write_queue due to memory "
          "pressure",
          removed_bytes);
        if (new_bytes > removed_bytes) {
            new_bytes -= removed_bytes;
        } else {
            new_bytes = 0;
        }
    }
    // Advance all write requests which are not throttled to next stage
    _pipeline.process_stage(
      [](const core::write_request<>&) noexcept
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

ss::future<> evictor::bg_evict_pipeline() {
    auto h = _gate.hold();
    size_t total_bytes{0};
    while (!_as.abort_requested()) {
        auto res = co_await ss::coroutine::as_future(evict_once(total_bytes));
        if (res.failed()) {
            vlog(
              cd_log.error, "Pipeline eviction error: {}", res.get_exception());
        } else if (res.get().has_error()) {
            vlog(cd_log.error, "Pipeline eviction error: {}", res.get());
        } else {
            total_bytes = res.get().value();
        }
    }
}
} // namespace experimental::cloud_topics
