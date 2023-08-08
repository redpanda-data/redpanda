// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "wasm/work_queue.h"

#include "vassert.h"

#include <seastar/core/future-util.hh>

namespace wasm {
ss::future<> threaded_work_queue::submit(ss::noncopyable_function<void()> fn) {
    auto _ = co_await _submit_mutex.get_units();
    _as.check();
    _pending_task_completed = {};
    _pending_task.set_value(std::move(fn));
    co_await _pending_task_completed.get_future();
}

ss::future<> threaded_work_queue::start() {
    _pending_task = {};
    _pending_task_completed = {};
    _as = {};
    _thread = ss::thread([this] {
        while (!_as.abort_requested()) {
            auto task = _pending_task.get_future().get();
            _pending_task = {};
            try {
                task();
                _pending_task_completed.set_value();
            } catch (...) {
                _pending_task_completed.set_to_current_exception();
            }
        }
    });
    return ss::now();
}
ss::future<> threaded_work_queue::stop() {
    // already have stopped
    if (_as.abort_requested()) {
        co_return;
    }
    co_await submit([this] { _as.request_abort(); });
    co_await _thread.join();
}
} // namespace wasm
