/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/core/write_pipeline.h"

#include "base/units.h"
#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/pipeline_stage.h"
#include "cloud_topics/core/write_request.h"
#include "cloud_topics/logger.h"
#include "utils/human.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <chrono>
#include <exception>

namespace experimental::cloud_topics::core {

static constexpr size_t max_pipeline_stages = 10;

template<class Clock>
write_pipeline<Clock>::write_pipeline()
  // TODO: use configuration parameter and binding
  : _mem_budget(10_MiB, "write-pipeline")
  , _stages(max_pipeline_stages) {}

template<class Clock>
write_pipeline<Clock>::~write_pipeline() = default;

template<class Clock>
ss::future<result<model::record_batch_reader>>
write_pipeline<Clock>::write_and_debounce(
  model::ntp ntp,
  model::record_batch_reader r,
  std::chrono::milliseconds timeout) {
    auto h = _gate.hold();
    // The write request is stored on the stack of the
    // fiber until the 'response' promise is set. The
    // promise can be set by any fiber that completed
    // the request processing.
    auto data_chunk = co_await core::serialize_in_memory_record_batch_reader(
      std::move(r));
    auto sz = data_chunk.payload.size_bytes();
    // Grab the semaphore after the size of the write request
    // is known. It's impossible to do this in advance because
    // the memory is actually allocated before this call.
    auto units = co_await ss::get_units(_mem_budget, sz, _as);
    _current_size += sz;
    _bytes_total += sz;
    auto d = ss::defer([this, sz] { _current_size -= sz; });
    auto stage = _stages.first_stage();
    core::write_request<Clock> request(
      std::move(ntp), std::move(data_chunk), timeout, stage);
    vlog(
      cd_log.trace,
      "write_pipeline.write_and_debounce, created write_request(size={}, "
      "timeout={})",
      sz,
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count());
    auto fut = request.response.get_future();
    _pending.push_back(request);

    // Notify all active event_filter instances
    signal(stage);

    auto res = co_await std::move(fut);
    if (res.has_error()) {
        co_return res.error();
    }
    // At this point the request is no longer referenced
    // by any other shard
    auto rdr = model::make_memory_record_batch_reader(std::move(res.value()));
    co_return std::move(rdr);
}

template<class Clock>
void write_pipeline<Clock>::reenqueue(write_request<Clock>& r) {
    if (r._hook.is_linked()) {
        r._hook.unlink();
    }
    if (r.has_expired()) {
        vlog(cd_log.debug, "Write request has expired");
        r.set_value(errc::timeout);
    } else {
        vlog(
          cd_log.debug,
          "Write request is returned, stage will be propagated from {}",
          r.stage);
        // Move all re-enqueued requests to the next stage automatically
        // and notify the corresponding event filter.
        r.stage = _stages.next_stage(r.stage);
        _pending.push_back(r);
        signal(r.stage);
    }
}

template<class Clock>
typename write_pipeline<Clock>::size_limited_write_req_list
write_pipeline<Clock>::get_write_requests(
  size_t max_bytes, pipeline_stage stage) {
    // First remove timed out write request to avoid returning them
    remove_timed_out_write_requests();

    vlog(
      cd_log.debug, "get_write_requests called with max_bytes = {}", max_bytes);

    size_limited_write_req_list result;
    size_t acc_size = 0;

    // The elements in the list are in the insertion order.
    auto it = _pending.begin();
    for (; it != _pending.end(); it++) {
        if (it->stage != stage) {
            continue;
        }
        auto sz = it->data_chunk.payload.size_bytes();
        acc_size += sz;
        if (acc_size >= max_bytes) {
            // Include last element
            it++;
            break;
        }
    }
    result.ready.splice(result.ready.end(), _pending, _pending.begin(), it);
    result.size_bytes = acc_size;
    result.complete = _pending.empty();
    vlog(
      cd_log.debug,
      "get_write_requests returned {} elements, containing {} ({}B)",
      result.ready.size(),
      human::bytes(result.size_bytes),
      result.size_bytes);
    return result;
}

template<class Clock>
ss::future<event> write_pipeline<Clock>::subscribe(
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
ss::future<event>
write_pipeline<Clock>::subscribe(event_filter<Clock>& flt) noexcept {
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
void write_pipeline<Clock>::remove_timed_out_write_requests() {
    chunked_vector<ss::weak_ptr<core::write_request<Clock>>> expired;
    for (auto& wr : _pending) {
        if (wr.has_expired()) {
            expired.push_back(wr.weak_from_this());
        }
    }
    if (!expired.empty()) {
        vlog(cd_log.debug, "{}B write requests have expired", expired.size());
    }
    for (auto& wp : expired) {
        if (wp != nullptr) {
            vlog(
              cd_log.debug,
              "{}B write requests have expired",
              wp->size_bytes());
            wp->_hook.unlink();
            wp->set_value(errc::timeout);
        }
    }
}

template<class Clock>
void write_pipeline<Clock>::signal(pipeline_stage stage) {
    vlog(cd_log.debug, "write_pipeline:signal, stage: {}", stage);
    event ev{
      .stage = stage,
      .type = event_type::new_write_request,
      .pending_write_bytes = _current_size,
      .total_write_bytes = _bytes_total,
    };
    for (auto& f : _filters) {
        if (
          f.get_type() == event_type::new_write_request
          && f.get_stage() == stage) {
            vlog(
              cd_log.debug,
              "write_pipeline.signal, pending_write_bytes: {}, "
              "total_write_bytes: {}",
              ev.pending_write_bytes,
              ev.total_write_bytes);
            f.trigger(ev);
        }
        // The cleanup is performed by the subscriber
    }
}

template<class Clock>
pipeline_stage write_pipeline<Clock>::register_pipeline_stage() noexcept {
    return _stages.register_pipeline_stage();
}

template class write_pipeline<ss::lowres_clock>;
template class write_pipeline<ss::manual_clock>;

} // namespace experimental::cloud_topics::core
