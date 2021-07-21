/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/std-coroutine.hh>

#include <pandaproxy/schema_registry/types.h>

namespace pandaproxy::schema_registry {

class offset_waiter {
    struct data {
        model::offset _offset;
        ss::promise<offset_conflict> _promise;
    };

public:
    explicit offset_waiter(size_t size = 1)
      : _waiters{size} {}

    ss::future<offset_conflict> wait(model::offset offset) {
        data d{._offset{offset}};
        auto fut = d._promise.get_future();
        co_await _waiters.push_eventually(std::move(d));
        co_return co_await std::move(fut);
    }

    void signal(model::offset offset, offset_conflict conflict) {
        while (!_waiters.empty()) {
            if (_waiters.front()._offset == offset) {
                _waiters.front()._promise.set_value(conflict);
                _waiters.pop();
            } else {
                // offsets should be ordered
                return;
            }
        }
    }

private:
    ss::queue<data> _waiters;
};

} // namespace pandaproxy::schema_registry
