/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "base/vlog.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/logger.h"
#include "container/chunked_vector.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l0 {

//
class pipeline_abort_requested : public ss::abort_requested_exception {
public:
    const char* what() const noexcept override {
        return "pipeline abort requested";
    }
};

static constexpr size_t max_pipeline_stages = 10;

/// Result of the processing of the single request
enum class request_processing_result {
    /// Advance current request to the next pipeline stage and
    /// continue iteration.
    advance_and_continue,
    /// Advance current request to the next pipeline stage and
    /// stop iteration.
    advance_and_stop,
    /// Do not advance current request to the next pipeline stage and
    /// continue iteration.
    ignore_and_continue,
    /// Do not advance current request to the next pipeline stage and
    /// stop iteration.
    ignore_and_stop,
};

template<class Request>
using intrusive_request_list = intrusive_list<Request, &Request::_hook>;

template<class Pipeline, class Request>
struct requests_list {
    requests_list() = delete;
    ~requests_list() = default;
    requests_list(
      Pipeline* parent,
      pipeline_stage ps,
      intrusive_request_list<Request> list = {})
      : requests(std::move(list))
      , _ps(ps)
      , _parent(parent) {}

    requests_list(const requests_list<Pipeline, Request>&) = delete;
    requests_list& operator=(const requests_list<Pipeline, Request>&) = delete;
    requests_list(requests_list<Pipeline, Request>&&) noexcept = default;
    requests_list&
    operator=(requests_list<Pipeline, Request>&&) noexcept = default;

    // Intrusive list of pending requests
    intrusive_request_list<Request> requests;
    bool complete{true};

    pipeline_stage _ps;

    Pipeline* _parent;
};

/// Base class of the read_pipeline and write_pipeline.
/// The base_pipeline contains code which is reused by both pipelines.
template<class Request, class Derived, class Clock = ss::lowres_clock>
class base_pipeline {
public:
    base_pipeline()
      : _root_rtc(_as)
      , _logger(cd_log, _root_rtc, static_cast<Derived*>(this)->pipeline_name())
      , _stages(max_pipeline_stages) {}
    ~base_pipeline() = default;
    base_pipeline(const base_pipeline<Request, Derived, Clock>&) = delete;
    base_pipeline&
    operator=(const base_pipeline<Request, Derived, Clock>&) = delete;
    base_pipeline(base_pipeline<Request, Derived, Clock>&&) noexcept = delete;
    base_pipeline&
    operator=(base_pipeline<Request, Derived, Clock>&&) noexcept = delete;

    /// Subscribe to events of certain type
    ///
    /// The returned future will become ready when new data will be added to the
    /// pipeline or when the shutdown even will occur.
    ss::future<event>
    subscribe(event_filter<Clock>& flt, ss::abort_source& as) noexcept {
        auto sub = as.subscribe(
          [&flt](const std::optional<std::exception_ptr>&) noexcept {
              // This code just cancels event subscription but it can be
              // improved by transferring exception to the subscriber.
              flt.cancel();
          });
        if (!sub) {
            co_return event{.type = event_type::shutting_down};
        }
        co_return co_await subscribe(flt);
    }
    ss::future<event> subscribe(event_filter<Clock>& flt) noexcept {
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
            // Trigger event immediately without waiting for the future
            auto event = static_cast<Derived*>(this)->trigger_event(
              flt.get_stage());
            if (flt.trigger(event)) {
                co_return event;
            }
        }
        _filters.push_back(flt);
        auto ev = co_await ss::coroutine::as_future(flt.get_future());
        if (ev.failed()) {
            auto ep = ev.get_exception();
            if (ssx::is_shutdown_exception(ep)) {
                co_return event{.type = event_type::shutting_down};
            }
            // The only exception that can be thrown here is a shutdown
            // exception (broken_promise). We never set the promise to
            // any other exception.
            vassert(
              false, "Unexpected failure in the event subscription: {}", ep);
        }
        co_return ev.get();
    }

    /// Get root retry chain node to use with async
    /// operations.
    basic_retry_chain_node<Clock>& get_root_rtc() noexcept { return _root_rtc; }

    void shutdown() {
        _as.request_abort_ex(
          std::make_exception_ptr(pipeline_abort_requested()));
        remove_requests_for_shutdown();
    }

    ss::future<> stop() {
        if (!_as.abort_requested()) {
            _as.request_abort_ex(
              std::make_exception_ptr(pipeline_abort_requested()));
            remove_requests_for_shutdown();
        }
        auto fut = _gate.close();
        for (auto& f : _filters) {
            f.cancel();
        }
        co_await std::move(fut);
    }

    bool stopped() const noexcept { return _as.abort_requested(); }

protected:
    //
    // This is API used by different pipeline implementations (read/write).
    //

    /// Return first stage of the pipeline
    pipeline_stage first_stage() { return _stages.first_stage(); }

    /// Return next stage
    pipeline_stage next_stage(pipeline_stage s) const {
        return _stages.next_stage(s);
    }

    /// Return next stage index without checking if stage is registered.
    /// This is useful for accessing pre-allocated resources.
    int next_stage_index(pipeline_stage s) const {
        return _stages.next_stage_index(s);
    }

    /// Resolve every pending write that matches the predicate with an error.
    template<typename Pred>
    void remove_requests(Pred pred, errc error, std::string_view reason) {
        chunked_vector<ss::weak_ptr<Request>> expired;
        for (auto& wr : _pending) {
            if (pred(wr)) {
                expired.push_back(wr.weak_from_this());
            }
        }
        if (!expired.empty()) {
            vlog(
              _logger.debug, "{} requests removed: {}", expired.size(), reason);
        }
        for (auto& wp : expired) {
            if (wp != nullptr) {
                vlog(_logger.debug, "{} requests removed: {}", wp->ntp, reason);
                wp->_hook.unlink();
                wp->set_value(error);
            }
        }
    }

    /// Find all timed out requests and remove them from the list
    /// atomically.
    void remove_timed_out_requests() {
        remove_requests(
          [](const auto& wr) { return wr.has_expired(); },
          errc::timeout,
          "expired");
    }

    /// Remove every pending request and resolve with shutdown error.
    void remove_requests_for_shutdown() {
        remove_requests(
          [](const auto&) { return true; },
          errc::shutting_down,
          "shutting down");
    }

    basic_retry_chain_logger<Clock>& logger() { return _logger; }

    ss::gate::holder hold_gate() { return _gate.hold(); }

    /// Get the underlying queue
    intrusive_request_list<Request>& get_pending() noexcept { return _pending; }
    const intrusive_request_list<Request>& get_pending() const noexcept {
        return _pending;
    }

    /// Get list of event filters
    auto& get_filters() noexcept { return _filters; }

    /// Create new pipeline stage object
    pipeline_stage register_pipeline_stage() noexcept {
        return _stages.register_pipeline_stage();
    }

    /// Signal all active filters
    void do_signal(
      pipeline_stage stage,
      event_type ev_type,
      size_t pending_bytes,
      size_t total_bytes) {
        vlog(_logger.debug, "signal, stage: {}", stage);
        event ev{
          .stage = stage,
          .type = ev_type,
        };
        if (ev_type == event_type::new_read_request) {
            ev.pending_read_bytes = pending_bytes;
            ev.total_read_bytes = total_bytes;
        } else if (ev_type == event_type::new_write_request) {
            ev.pending_write_bytes = pending_bytes;
            ev.total_write_bytes = total_bytes;
        }
        for (auto& f : _filters) {
            if (f.get_type() == ev_type && f.get_stage() == stage) {
                vlog(
                  _logger.debug,
                  "{}.signal, pending_write_bytes: {}, "
                  "total_write_bytes: {}",
                  static_cast<Derived*>(this)->pipeline_name(),
                  ev.pending_write_bytes,
                  ev.total_write_bytes);
                f.trigger(ev);
            }
            // The cleanup is performed by the subscriber
        }
    }

private:
    intrusive_request_list<Request> _pending;
    ss::gate _gate;

    ss::abort_source _as;
    basic_retry_chain_node<Clock> _root_rtc;
    basic_retry_chain_logger<Clock> _logger;

    event_filter<Clock>::event_filter_list _filters;
    pipeline_stage_container _stages;
};
} // namespace cloud_topics::l0
