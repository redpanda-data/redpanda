// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/work_queue.h"

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
    return std::exchange(_tail, ss::now());
}
} // namespace ssx
