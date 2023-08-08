/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>

namespace wasm {

/**
 * A utility for running sequential work on a ss::thread
 *
 * This utility is essentially the same as `ss::async`, except that it reuses
 * the `ss::thread` between calls. This is important because creating a
 * `ss::thread` is quite slow as it has to swap out the stack and makes a number
 * of system calls.
 */
class threaded_work_queue {
public:
    ss::future<> start();
    ss::future<> stop();

    template<typename T>
    ss::future<T> enqueue(ss::noncopyable_function<T()> fn) {
        ss::promise<T> p;
        try {
            co_await submit([&p, fn = std::move(fn)] {
                try {
                    if constexpr (std::is_void_v<T>) {
                        fn();
                        p.set_value();
                    } else {
                        p.set_value(fn());
                    }
                } catch (...) {
                    p.set_to_current_exception();
                }
            });
        } catch (...) {
            p.set_to_current_exception();
        }
        co_return co_await std::move(p.get_future());
    }

private:
    ss::future<> submit(ss::noncopyable_function<void()>);
    mutex _submit_mutex;
    ss::promise<ss::noncopyable_function<void()>> _pending_task;
    ss::promise<> _pending_task_completed;
    ss::abort_source _as;
    ss::thread _thread{};
};

} // namespace wasm
