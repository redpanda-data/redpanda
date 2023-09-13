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
#include <seastar/core/sleep.hh>

#include <exception>

namespace ssx {
work_queue::work_queue(error_reporter_fn fn)
  : _error_reporter(std::move(fn)) {}

void work_queue::submit(ss::noncopyable_function<ss::future<>()> fn) {
    if (_as.abort_requested()) {
        return;
    }
    _tail = _tail
              .then([this, fn = std::move(fn)]() {
                  if (_as.abort_requested()) {
                      return ss::now();
                  }
                  return fn();
              })
              .handle_exception(
                [this](const std::exception_ptr& ex) { _error_reporter(ex); });
}

ss::future<> work_queue::shutdown() {
    _as.request_abort();
    co_await _gate.close();
    co_await std::exchange(_tail, ss::now());
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
} // namespace ssx
