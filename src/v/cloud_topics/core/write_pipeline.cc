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
#include <seastar/util/optimized_optional.hh>

#include <algorithm>
#include <chrono>
#include <exception>

namespace experimental::cloud_topics::core {

template<class Clock>
write_pipeline<Clock>::write_pipeline()
  // TODO: use configuration parameter and binding
  : _mem_budget(10_MiB, "write-pipeline") {}

template<class Clock>
write_pipeline<Clock>::~write_pipeline() = default;

template<class Clock>
ss::future<result<model::record_batch_reader>>
write_pipeline<Clock>::write_and_debounce(
  model::ntp ntp,
  model::record_batch_reader r,
  std::chrono::milliseconds timeout) {
    auto h = this->hold_gate();
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
    auto units = co_await ss::get_units(
      _mem_budget, sz, this->get_root_rtc().root_abort_source());
    _current_size += sz;
    _bytes_total += sz;
    auto d = ss::defer([this, sz] { _current_size -= sz; });
    auto stage = this->first_stage();
    core::write_request<Clock> request(
      std::move(ntp), std::move(data_chunk), timeout, stage);
    vlog(
      cd_log.trace,
      "write_pipeline.write_and_debounce, created write_request(size={}, "
      "timeout={})",
      sz,
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count());
    auto fut = request.response.get_future();
    this->get_pending().push_back(request);

    // Notify all active event_filter instances
    this->signal(stage);

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
        r.stage = this->next_stage(r.stage);
        this->get_pending().push_back(r);
        this->signal(r.stage);
    }
}

template<class Clock>
write_pipeline<Clock>::stage::stage(write_pipeline<Clock>* p, pipeline_stage s)
  : _parent(p)
  , _ps(s) {}

template<class Clock>
typename write_pipeline<Clock>::write_requests_list
write_pipeline<Clock>::get_write_requests(
  size_t max_bytes, pipeline_stage stage) {
    // First remove timed out write request to avoid returning them
    this->remove_timed_out_requests();

    vlog(
      cd_log.debug, "get_write_requests called with max_bytes = {}", max_bytes);

    auto& pending = this->get_pending();

    write_requests_list result(this, stage, {});

    size_t acc_size = 0;

    // The elements in the list are in the insertion order.
    auto it = pending.begin();
    for (; it != pending.end(); it++) {
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
    result.requests.splice(result.requests.end(), pending, pending.begin(), it);
    result.complete = pending.empty();
    vlog(
      cd_log.debug,
      "get_write_requests returned {} elements, containing {} ({}B)",
      result.requests.size(),
      human::bytes(acc_size),
      acc_size);
    return result;
}

template<class Clock>
write_pipeline<Clock>::stage
write_pipeline<Clock>::register_write_pipeline_stage() noexcept {
    return stage{this, this->register_pipeline_stage()};
}

template<class Clock>
void write_pipeline<Clock>::signal(pipeline_stage stage) {
    this->do_signal(
      stage, event_type::new_write_request, _current_size, _bytes_total);
}

template<class Clock>
bool write_pipeline<Clock>::stage::stopped() const noexcept {
    return _parent->stopped();
}

template<class Clock>
void write_pipeline<Clock>::stage::push_next_stage(write_request<Clock>& req) {
    _parent->reenqueue(req);
}

template<class Clock>
write_pipeline<Clock>::write_requests_list
write_pipeline<Clock>::stage::pull_write_requests(size_t max_bytes) {
    return _parent->get_write_requests(max_bytes, _ps);
}

template<class Clock>
ss::future<checked<event, errc>> write_pipeline<Clock>::stage::wait_until(
  size_t max_bytes,
  typename Clock::time_point deadline,
  ss::abort_source* maybe_as) noexcept {
    auto [sub, as] = choose_abort_source(maybe_as);
    while (true) {
        core::event_filter<Clock> filter(
          core::event_type::new_write_request, _ps, deadline);
        auto event_fut = co_await ss::coroutine::as_future(
          _parent->subscribe(filter, *as));
        if (event_fut.failed()) {
            auto err = event_fut.get_exception();
            if (ssx::is_shutdown_exception(err)) {
                co_return errc::shutting_down;
            }
            co_return errc::unexpected_failure;
        }
        auto event = event_fut.get();
        switch (event.type) {
        case core::event_type::shutting_down:
            co_return errc::shutting_down;
        case core::event_type::new_write_request:
            if (event.pending_write_bytes < max_bytes) {
                // Ignore all write requests until timed
                // out or enough data.
                break;
            }
            [[fallthrough]];
        case core::event_type::err_timedout:
            co_return errc::success;
        case core::event_type::new_read_request:
        case core::event_type::none:
            vassert(false, "Read request added to the write pipeline");
        }
        co_return event;
    }
}

template<class Clock>
ss::future<checked<event, errc>>
write_pipeline<Clock>::stage::wait_next(ss::abort_source* maybe_as) noexcept {
    core::event_filter<Clock> filter(core::event_type::new_write_request, _ps);
    auto [sub, as] = choose_abort_source(maybe_as);
    auto event = co_await _parent->subscribe(filter, *as);
    switch (event.type) {
    case core::event_type::shutting_down:
        co_return errc::shutting_down;
    case core::event_type::err_timedout:
    case core::event_type::new_read_request:
    case core::event_type::none:
        vassert(false, "Read request added to the write pipeline");
    case core::event_type::new_write_request:
        break;
    }
    co_return event;
}

template<class Clock>

std::pair<
  ss::optimized_optional<ss::abort_source::subscription>,
  ss::abort_source*>
write_pipeline<Clock>::stage::choose_abort_source(ss::abort_source* maybe_as) {
    auto as = maybe_as;
    ss::optimized_optional<ss::abort_source::subscription> sub;
    if (as == nullptr) {
        as = &_parent->get_root_rtc().root_abort_source();
    } else {
        sub = _parent->get_root_rtc().root_abort_source().subscribe(
          [as](const std::optional<std::exception_ptr>&) noexcept {
              as->request_abort();
          });
    }
    return std::make_pair(std::move(sub), as);
}

template class write_pipeline<ss::lowres_clock>;
template class write_pipeline<ss::manual_clock>;

} // namespace experimental::cloud_topics::core
