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

#include "base/seastarx.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/util/noncopyable_function.hh>

namespace ssx {

/**
 * A small utility for running async tasks sequentially on a single fiber.
 */
class work_queue {
public:
    using error_reporter_fn
      = ss::noncopyable_function<void(const std::exception_ptr&)>;

    explicit work_queue(error_reporter_fn);
    work_queue(ss::scheduling_group, error_reporter_fn);
    // Add a task to the queue to be processed.
    void submit(ss::noncopyable_function<ss::future<>()>);
    // Add a task to the queue to be processed after some timeout.
    template<typename Clock = ss::lowres_clock>
    void
      submit_delayed(Clock::duration, ss::noncopyable_function<ss::future<>()>);
    // Shutdown the queue, waiting for the currently executing task to finish.
    ss::future<> shutdown();

private:
    void submit_after(ss::future<>, ss::noncopyable_function<ss::future<>()>);

    ss::future<> process();

    error_reporter_fn _error_reporter;
    ss::condition_variable _cond_var;
    ss::chunked_fifo<ss::noncopyable_function<ss::future<>()>> _tasks;
    ss::abort_source _as;
    ss::gate _gate;
};

template<typename Clock>
void work_queue::submit_delayed(
  Clock::duration delay, ss::noncopyable_function<ss::future<>()> fn) {
    if (_as.abort_requested()) {
        return;
    }
    submit_after(ss::sleep_abortable<Clock>(delay, _as), std::move(fn));
}
} // namespace ssx
