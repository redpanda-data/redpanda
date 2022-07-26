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
#include "likely.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/timer.hh>

#include <system_error>

template<typename T, typename Clock = ss::lowres_clock>
class expiring_promise {
public:
    expiring_promise() = default;
    expiring_promise(expiring_promise&&) = delete;
    expiring_promise& operator=(const expiring_promise&) = delete;
    expiring_promise& operator=(expiring_promise&&) = delete;
    expiring_promise(const expiring_promise&) = delete;

    template<typename ErrorFactory>
    ss::future<T> get_future_with_timeout(
      typename Clock::time_point timeout,
      ErrorFactory&& err_factory,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt) {
        if (!_available) {
            // handle abort source
            if (as) {
                auto opt_sub = as.value().get().subscribe([this]() noexcept {
                    if (_timer.cancel()) {
                        set_exception(ss::abort_requested_exception{});
                    }
                });
                if (opt_sub) {
                    _sub = std::move(*opt_sub);
                } else {
                    set_exception(ss::abort_requested_exception{});
                    return _promise.get_future();
                }
            }
            _timer.set_callback(
              [this, ef = std::forward<ErrorFactory>(err_factory)] {
                  using err_t = std::invoke_result_t<ErrorFactory>;

                  constexpr bool is_result = std::is_same_v<err_t, T>;
                  constexpr bool is_std_error_code
                    = std::is_convertible_v<err_t, std::error_code>;

                  // if errors are encoded in values f.e. result<T> or errc
                  if constexpr (is_result || is_std_error_code) {
                      set_value(ef());
                  } else {
                      set_exception(ef());
                  }
                  unlink_abort_source();
              });
            _timer.arm(timeout);
        }

        auto f = _promise.get_future();
        return f;
    };

    void set_value(T val) {
        if (_timer.cancel()) {
            unlink_abort_source();
        }

        if (likely(!_available)) {
            _available = true;
            _promise.set_value(std::move(val));
        }
    }

    bool available() const { return _available; }

    template<typename Exception>
    void set_exception(Exception&& ex) {
        if (_timer.cancel()) {
            unlink_abort_source();
        }

        if (likely(!_available)) {
            _available = true;
            _promise.set_exception(std::forward<Exception>(ex));
        }
    }

private:
    void unlink_abort_source() {
        if (_sub.is_linked()) {
            _sub.unlink();
        }
    }

    bool _available{false};
    ss::promise<T> _promise;
    ss::timer<Clock> _timer;
    ss::abort_source::subscription _sub;
};
