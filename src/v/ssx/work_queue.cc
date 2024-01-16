// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/work_queue.h"

#include "ssx/future-util.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/switch_to.hh>

namespace ssx {

work_queue::work_queue(error_reporter_fn fn)
  : work_queue(ss::current_scheduling_group(), std::move(fn)) {}

work_queue::work_queue(ss::scheduling_group sg, error_reporter_fn fn)
  : _error_reporter(std::move(fn)) {
    ssx::background = ss::with_gate(_gate, [this, sg] {
        return ss::with_scheduling_group(sg, [this] { return process(); });
    });
}

void work_queue::submit(ss::noncopyable_function<ss::future<>()> fn) {
    if (_as.abort_requested()) {
        return;
    }
    _tasks.push_back(std::move(fn));
    _cond_var.signal();
}

ss::future<> work_queue::shutdown() {
    _as.request_abort();
    _cond_var.signal();
    co_await _gate.close();
}

void work_queue::submit_after(
  ss::future<> fut, ss::noncopyable_function<ss::future<>()> fn) {
    auto holder = _gate.hold();
    ssx::background = fut.then_wrapped(
      [this, fn = std::move(fn), h = std::move(holder)](auto fut) mutable {
          fut.ignore_ready_future();
          submit(std::move(fn));
      });
}

ss::future<> work_queue::process() {
    while (true) {
        co_await _cond_var.wait(
          [this] { return !_tasks.empty() || _as.abort_requested(); });
        if (_as.abort_requested()) {
            co_return;
        }
        auto task_fn = std::move(_tasks.front());
        _tasks.pop_front();
        try {
            co_await task_fn();
        } catch (...) {
            _error_reporter(std::current_exception());
        }
    }
}

} // namespace ssx
