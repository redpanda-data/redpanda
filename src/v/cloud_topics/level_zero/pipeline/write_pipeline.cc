/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/write_pipeline.h"

#include "base/units.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/semaphore.h"
#include "utils/human.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/optimized_optional.hh>

#include <algorithm>
#include <chrono>
#include <exception>

namespace cloud_topics::l0 {

// The max value is used in unit-tests only. Normally, the memory_groups
// function is used to set up the limit. If the cloud_topics are disabled
// but the pipeline is still created (which is something we do in tests)
// then this constant will be used.
static constexpr size_t max_memory_when_disabled = 100 * 1024 * 1024;

namespace {
size_t get_cloud_topics_l0_write_path_memory() {
    return memory_groups().cloud_topics_memory() > 0
             // Split memory in half between read and write path.
             // TODO: take L1 into account.
             ? memory_groups().cloud_topics_memory() / 2
             : max_memory_when_disabled;
}
} // namespace

template<class Clock>
write_pipeline<Clock>::write_pipeline()
  : _mem_budget(get_cloud_topics_l0_write_path_memory(), "write-pipeline")
  , _req_budget(
      config::shard_local_cfg().cloud_topics_produce_write_inflight_limit(),
      "write-pipeline-req-count")
  , _probe(
      "write",
      config::shard_local_cfg().disable_metrics(),
      config::shard_local_cfg().disable_public_metrics()) {
    vlog(
      cd_log.trace,
      "write_pipeline created, memory budget: {}",
      _mem_budget.current());
}

template<class Clock>
write_pipeline<Clock>::~write_pipeline() = default;

template<class Clock>
ss::future<std::expected<upload_meta, std::error_code>>
write_pipeline<Clock>::write_and_debounce(
  model::ntp ntp,
  cluster_epoch min_epoch,
  chunked_vector<model::record_batch> batches,
  Clock::time_point timeout) {
    auto staged = co_await prepare_write(std::move(batches));
    if (!staged.has_value()) {
        co_return std::unexpected(staged.error());
    }
    co_return co_await execute_write(
      std::move(ntp), min_epoch, std::move(staged.value()), timeout);
}

template<class Clock>
auto write_pipeline<Clock>::prepare_write(
  chunked_vector<model::record_batch> batches)
  -> ss::future<std::expected<prepared_data, std::error_code>> {
    auto h = this->hold_gate();
    auto data_chunk = co_await l0::serialize_batches(std::move(batches));
    auto sz = data_chunk.payload.size_bytes();
    // Register data influx
    _probe.register_bytes_in(sz);
    _probe.set_memory_usage_gauge(current_size() + sz);
    // Grab the semaphore after the size of the write request
    // is known. It's impossible to do this in advance because
    // the memory is actually allocated before this call.
    auto units = ss::try_get_units(_mem_budget, sz);
    if (!units) {
        auto measure = _probe.register_memory_pressure_blocked(sz);
        units = co_await ss::get_units(
          _mem_budget, sz, this->get_root_rtc().root_abort_source());
    }

    auto req_units = ss::try_get_units(_req_budget, 1);
    if (!req_units) {
        _probe.register_request_limit_blocked();
        req_units = co_await ss::get_units(
          _req_budget, 1, this->get_root_rtc().root_abort_source());
    }
    co_return prepared_data{
      .data_chunk = std::move(data_chunk),
      .mem_units = std::move(units.value()),
      .req_units = std::move(req_units.value())};
}

template<class Clock>
ss::future<std::expected<upload_meta, std::error_code>>
write_pipeline<Clock>::execute_write(
  model::ntp ntp,
  cluster_epoch min_epoch,
  prepared_data prepped,
  Clock::time_point timeout) {
    auto h = this->hold_gate();
    // The write request is stored on the stack of the
    // fiber until the 'response' promise is set. The
    // promise can be set by any fiber that completed
    // the request processing.
    auto sz = prepped.data_chunk.payload.size_bytes();

    _probe.register_request();
    auto lat_measure = _probe.register_request_processing_time();
    auto err_probe = ss::defer([this] { _probe.register_request_error(); });
    _bytes_total += sz;
    l0::write_request<Clock> request(
      std::move(ntp), min_epoch, std::move(prepped.data_chunk), timeout);
    auto stage_cleanup = ss::defer([this, &request] {
        transfer_stage_bytes(
          request.stage, unassigned_pipeline_stage, request.size_bytes());
    });
    vlog(
      cd_log.trace,
      "write_pipeline.write_and_debounce, created write_request(size={}, "
      "timeout={})",
      sz,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        timeout - Clock::now())
        .count());
    auto fut = request.response.get_future();
    reenqueue(request, /*signal=*/true);

    auto res = co_await std::move(fut);
    if (!res.has_value()) {
        if (res.error() == errc::timeout) {
            err_probe.cancel();
            _probe.register_request_timeout();
        }
        co_return std::unexpected(make_error_code(res.error()));
    }
    err_probe.cancel();
    _probe.register_request_completed();
    co_return std::move(res.value());
}

template<class Clock>
void write_pipeline<Clock>::reenqueue(write_request<Clock>& r, bool signal) {
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
        advance_request_stage(r);
        this->get_pending().push_back(r);
        if (signal) {
            this->signal(r.stage);
        }
    }
}

template<class Clock>
write_pipeline<Clock>::stage::stage(write_pipeline<Clock>* p, pipeline_stage s)
  : _parent(p)
  , _ps(s) {}

template<class Clock>
typename write_pipeline<Clock>::write_requests_list
write_pipeline<Clock>::get_write_requests(
  size_t max_bytes, pipeline_stage stage, size_t max_requests) {
    // First remove timed out write request to avoid returning them
    this->remove_timed_out_requests();

    vlog(
      cd_log.trace, "get_write_requests called with max_bytes = {}", max_bytes);

    auto& pending = this->get_pending();

    write_requests_list result(this, stage, {});

    size_t acc_size = 0;
    size_t acc_req = 0;

    auto it = pending.begin();
    for (; it != pending.end();) {
        if (it->stage != stage) {
            it++;
            continue;
        }
        auto sz = it->data_chunk.payload.size_bytes();
        acc_size += sz;
        acc_req++;
        // Always include the first request even if it exceeds limits
        // to avoid stalling the pipeline with oversized requests
        if (
          (acc_size >= max_bytes || acc_req >= max_requests)
          && !result.requests.empty()) {
            break;
        }
        auto& el = *it;
        it++;
        el._hook.unlink();
        if (el.stage != unassigned_pipeline_stage) {
            auto idx = static_cast<size_t>(el.stage()->get_numeric_id());
            _stage_bytes[idx] -= el.size_bytes();
            el.stage = unassigned_pipeline_stage;
        }
        result.requests.push_back(el);
    }
    result.complete = std::none_of(
      it, pending.end(), [stage](const auto& r) { return r.stage == stage; });
    vlog(
      cd_log.trace,
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
      stage, event_type::new_write_request, stage_bytes(stage), _bytes_total);
}

template<class Clock>
event write_pipeline<Clock>::trigger_event(pipeline_stage stage) {
    return event{
      .stage = stage,
      .type = event_type::new_write_request,
      .pending_write_bytes = stage_bytes(stage),
      .total_write_bytes = _bytes_total,
    };
}

template<class Clock>
bool write_pipeline<Clock>::stage::stopped() const noexcept {
    return _parent->stopped();
}

template<class Clock>
void write_pipeline<Clock>::stage::push_next_stage(
  write_request<Clock>& req, bool signal) {
    _parent->reenqueue(req, signal);
}

template<class Clock>
void write_pipeline<Clock>::stage::signal_next_stage() {
    _parent->signal(_parent->next_stage(_ps));
}

template<class Clock>
void write_pipeline<Clock>::stage::enqueue_foreign_request(
  write_request<Clock>& req, bool signal) {
    auto next = _parent->next_stage(_ps);
    req.stage = next;
    if (next != unassigned_pipeline_stage) {
        auto idx = static_cast<size_t>(next()->get_numeric_id());
        _parent->_stage_bytes.at(idx) += req.size_bytes();
    }
    _parent->get_pending().push_back(req);
    if (signal) {
        _parent->signal(next);
    }
}

template<class Clock>
write_pipeline<Clock>::write_requests_list
write_pipeline<Clock>::stage::pull_write_requests(
  size_t max_bytes, size_t max_requests) {
    return _parent->get_write_requests(max_bytes, _ps, max_requests);
}

template<class Clock>
ss::future<std::expected<event, errc>> write_pipeline<Clock>::stage::wait_until(
  size_t max_bytes,
  typename Clock::time_point deadline,
  ss::abort_source* maybe_as) noexcept {
    auto [sub, as] = choose_abort_source(maybe_as);
    l0::event_filter<Clock> filter(
      l0::event_type::new_write_request,
      _ps,
      deadline,
      {.min_pending_write_bytes = max_bytes});
    auto event_fut = co_await ss::coroutine::as_future(
      _parent->subscribe(filter, *as));
    if (event_fut.failed()) {
        auto err = event_fut.get_exception();
        if (ssx::is_shutdown_exception(err)) {
            co_return std::unexpected(errc::shutting_down);
        }
        co_return std::unexpected(errc::unexpected_failure);
    }
    auto event = event_fut.get();
    switch (event.type) {
    case l0::event_type::shutting_down:
        co_return std::unexpected(errc::shutting_down);
    case l0::event_type::new_write_request:
    case l0::event_type::err_timedout:
        break;
    case l0::event_type::new_read_request:
    case l0::event_type::none:
        vunreachable("Read request added to the write pipeline");
    }
    co_return event;
}

template<class Clock>
ss::future<std::expected<event, errc>>
write_pipeline<Clock>::stage::wait_next(ss::abort_source* maybe_as) noexcept {
    l0::event_filter<Clock> filter(l0::event_type::new_write_request, _ps);
    auto [sub, as] = choose_abort_source(maybe_as);
    auto event = co_await _parent->subscribe(filter, *as);
    switch (event.type) {
    case l0::event_type::shutting_down:
        co_return std::unexpected(errc::shutting_down);
    case l0::event_type::err_timedout:
    case l0::event_type::new_read_request:
    case l0::event_type::none:
        vunreachable("Read request added to the write pipeline");
    case l0::event_type::new_write_request:
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
        if (!sub) {
            as->request_abort();
        }
    }
    return std::make_pair(std::move(sub), as);
}

template<class Clock>
void write_pipeline<Clock>::transfer_stage_bytes(
  pipeline_stage from, pipeline_stage to, size_t bytes) {
    if (from != unassigned_pipeline_stage) {
        _stage_bytes[static_cast<size_t>(from()->get_numeric_id())] -= bytes;
    }
    if (to != unassigned_pipeline_stage) {
        _stage_bytes[static_cast<size_t>(to()->get_numeric_id())] += bytes;
    }
}

template<class Clock>
void write_pipeline<Clock>::advance_request_stage(write_request<Clock>& req) {
    auto next = this->next_stage(req.stage);
    transfer_stage_bytes(req.stage, next, req.size_bytes());
    req.stage = next;
}

template<class Clock>
size_t write_pipeline<Clock>::stage_bytes(pipeline_stage s) const {
    if (s == unassigned_pipeline_stage) {
        return 0;
    }
    return _stage_bytes[static_cast<size_t>(s()->get_numeric_id())].count;
}

template<class Clock>
const std::atomic<size_t>*
write_pipeline<Clock>::stage_bytes_ref(pipeline_stage s) const {
    if (s == unassigned_pipeline_stage) {
        return nullptr;
    }
    return &_stage_bytes[static_cast<size_t>(s()->get_numeric_id())].count;
}

template<class Clock>
const std::atomic<size_t>*
write_pipeline<Clock>::stage_bytes_ref_by_index(int index) const {
    if (index < 0 || static_cast<size_t>(index) >= _stage_bytes.size()) {
        return nullptr;
    }
    return &_stage_bytes[static_cast<size_t>(index)].count;
}

template<class Clock>
size_t write_pipeline<Clock>::current_size() const {
    size_t total = 0;
    for (const auto& bytes : _stage_bytes) {
        total += bytes.count.load(std::memory_order_relaxed);
    }
    return total;
}

template class write_pipeline<ss::lowres_clock>;
template class write_pipeline<ss::manual_clock>;

} // namespace cloud_topics::l0
