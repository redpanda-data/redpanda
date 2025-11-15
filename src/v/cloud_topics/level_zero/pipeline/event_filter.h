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

#include "base/seastarx.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "container/intrusive_list_helpers.h"

#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

namespace cloud_topics::l0 {

enum class event_type {
    // This indicates default initialized event
    none,
    // New read request registered
    new_read_request,
    // New write request registered
    new_write_request,
    // Pipeline is shutting down
    shutting_down,
    // Filter timed out
    err_timedout,
};

struct event {
    pipeline_stage stage;
    event_type type{event_type::none};
    size_t pending_write_bytes{0};
    size_t total_write_bytes{0};
    size_t pending_read_bytes{0};
    size_t total_read_bytes{0};
};

/// This is similar to remote::event_filter in
/// the cloud_storage. It's used to trigger different activities
/// by events.
/// E.g. the background upload loop in the batcher can start L0
/// upload early if enough data is accumulated.
template<class Clock = ss::lowres_clock>
class event_filter {
public:
    struct predicate {
        size_t min_pending_write_bytes = 0;
    };
    event_filter() = delete;
    ~event_filter() = default;
    event_filter(const event_filter&) = delete;
    event_filter(event_filter&&) noexcept = default;
    event_filter& operator=(const event_filter&) = delete;
    event_filter& operator=(event_filter&&) noexcept = default;

    /// Filter without timeout
    event_filter(event_type type, pipeline_stage stage)
      : _type(type)
      , _stage(stage) {
        _promise.emplace();
    }

    /// Filter that can timeout when the deadline is reached
    event_filter(
      event_type type,
      pipeline_stage stage,
      typename Clock::time_point deadline,
      predicate pred)
      : _type(type)
      , _stage(stage)
      , _pred(pred) {
        _promise.emplace();
        _expiry.emplace();
        _expiry->set_callback([this] {
            if (_promise.has_value()) {
                _hook.unlink();
                _promise.value().set_value(
                  event{.type = event_type::err_timedout});
                _promise = std::nullopt;
            }
        });
        _expiry->arm(deadline);
    }

    void cancel() {
        if (_promise.has_value()) {
            _hook.unlink();
            _promise.reset();
        }
    }

    event_type get_type() const noexcept { return _type; }

    pipeline_stage get_stage() const noexcept { return _stage; }

    bool trigger(const event& e) {
        if (e.type != _type) {
            return false;
        }
        if (e.stage != _stage) {
            return false;
        }
        if (e.pending_write_bytes < _pred.min_pending_write_bytes) {
            return false;
        }
        if (_promise.has_value()) {
            _promise.value().set_value(e);
            _promise = std::nullopt;
        }
        return true;
    }

    ss::future<event> get_future() noexcept {
        return _promise.value().get_future();
    }

private:
    intrusive_list_hook _hook;
    event_type _type{event_type::none};
    pipeline_stage _stage;
    predicate _pred;
    std::optional<ss::promise<event>> _promise;
    std::optional<ss::timer<Clock>> _expiry;

public:
    template<class TT, class... Options>
    friend class boost::intrusive::list;

    using event_filter_list
      = intrusive_list<event_filter, &event_filter::_hook>;
};

} // namespace cloud_topics::l0
