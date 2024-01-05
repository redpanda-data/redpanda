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
#include "base/seastarx.h"

#include <seastar/core/future.hh>

template<class... T>
class available_promise {
public:
    ss::future<T...> get_future() { return _promise.get_future(); }

    template<typename... A>
    void set_value(A&&... a) {
        _available = true;
        _promise.set_value(std::forward<A>(a)...);
    }

    bool available() { return _available; }

private:
    bool _available{false};
    ss::promise<T...> _promise;
};
