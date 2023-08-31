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
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
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
    // Add a task to the queue to be processed.
    void submit(ss::noncopyable_function<ss::future<>()>);
    // Shutdown the queue, waiting for the currently executing task to finish.
    ss::future<> shutdown();

private:
    error_reporter_fn _error_reporter;
    ss::future<> _tail = ss::now();
    ss::abort_source _as;
};

} // namespace ssx
