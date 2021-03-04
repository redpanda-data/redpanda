/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "outcome.h"
#include "rpc/errc.h"
#include "rpc/exceptions.h"
#include "rpc/types.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/future.hh>

namespace rpc::internal {
class response_handler {
public:
    using response_ptr = result<std::unique_ptr<streaming_context>>;
    using promise_t = ss::promise<response_ptr>;
    using timer_ptr = std::unique_ptr<rpc::timer_type>;

    response_handler() noexcept = default;
    ~response_handler() noexcept = default;
    response_handler(response_handler&&) noexcept = default;
    response_handler& operator=(response_handler&&) noexcept = default;
    response_handler(const response_handler&) = delete;
    response_handler& operator=(const response_handler&) = delete;

    // clang-format off
    template<typename Func>
    CONCEPT(requires requires(Func f){
        {f()} -> std::same_as<void>;
    })
    // clang-format on
    void with_timeout(
      rpc::clock_type::time_point timeout, Func&& timeout_action) {
        _timeout_timer = std::make_unique<rpc::timer_type>(
          [this, f = std::forward<Func>(timeout_action)]() mutable {
              complete_with_timeout(std::forward<Func>(f));
          });
        _timeout_timer->arm(timeout);
    }

    ss::future<response_ptr> get_future() { return _promise.get_future(); }

    template<typename Exception>
    void set_exception(Exception&& e) {
        maybe_cancel_timer();
        _promise.set_exception(std::forward<Exception>(e));
    }

    void set_value(response_ptr r) {
        maybe_cancel_timer();
        _promise.set_value(std::move(r));
    }

private:
    template<typename Func>
    void complete_with_timeout(Func&& timeout_action) {
        _promise.set_value(response_ptr(rpc::errc::client_request_timeout));
        timeout_action();
    }
    void maybe_cancel_timer() {
        if (_timeout_timer && _timeout_timer->armed()) {
            _timeout_timer->cancel();
        }
    }
    promise_t _promise;
    timer_ptr _timeout_timer;
};
} // namespace rpc::internal
