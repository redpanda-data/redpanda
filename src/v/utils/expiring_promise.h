#pragma once
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
    template<typename ErrorFactory>
    ss::future<T> get_future_with_timeout(
      typename Clock::time_point timeout,
      ErrorFactory&& err_factory,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt) {
        if (!_promise.available()) {
            // handle abort source
            if (as) {
                auto opt_sub = as.value().get().subscribe([this]() noexcept {
                    if (_timer.cancel()) {
                        _promise.set_exception(ss::abort_requested_exception{});
                    }
                });
                if (opt_sub) {
                    _sub = std::move(*opt_sub);
                } else {
                    _promise.set_exception(ss::abort_requested_exception{});
                    return _promise.get_shared_future();
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
                      _promise.set_value(ef());
                  } else {
                      _promise.set_exception(ef());
                  }
                  unlink_abort_source();
              });
            _timer.arm(timeout);
        }

        auto f = _promise.get_shared_future();
        return f;
    };

    void set_value(T val) {
        if (_timer.cancel()) {
            unlink_abort_source();
        }

        if (!_promise.available()) {
            _promise.set_value(std::move(val));
        }
    }

    void set_exception(std::exception_ptr&& ex) {
        if (_timer.cancel()) {
            _promise.set_exception(ex);
            unlink_abort_source();
        }

        if (!_promise.available()) {
            _promise.set_exception(ex);
        }
    }

    void set_exception(const std::exception_ptr& ex) {
        if (_timer.cancel()) {
            unlink_abort_source();
        }

        if (!_promise.available()) {
            _promise.set_exception(ex);
        }
    }

private:
    void unlink_abort_source() {
        if (_sub.is_linked()) {
            _sub.unlink();
        }
    }

    ss::shared_promise<T> _promise;
    ss::timer<Clock> _timer;
    ss::abort_source::subscription _sub;
};
