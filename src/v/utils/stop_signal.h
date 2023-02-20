/*
 * Copyright 2020 Redpanda Data, Inc.
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

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

class stop_signal {
public:
    stop_signal() {
        ss::engine().handle_signal(SIGINT, [this] { signaled(); });
        ss::engine().handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
        ss::engine().handle_signal(SIGINT, [] {});
        ss::engine().handle_signal(SIGTERM, [] {});

        // We should signal for unit tests, because some background tasks does
        // not finish without it
        signaled();
    }
    ss::future<> wait() {
        return _cond.wait([this] { return _as.abort_requested(); });
    }

    bool stopping() const { return _as.abort_requested(); }

    ss::abort_source& abort_source() { return _as; };

private:
    void signaled() {
        if (!_as.abort_requested()) {
            _as.request_abort();
        }
        _cond.broadcast();
    }

    ss::condition_variable _cond;
    ss::abort_source _as;
};
