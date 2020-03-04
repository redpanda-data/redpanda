#pragma once
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <exception>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>

template<typename T, typename Clock = ss::lowres_clock>
class expiring_promise {
public:
    template<typename ErrorFactory>
    ss::future<T> get_future_with_timeout(
      typename Clock::time_point timeout, ErrorFactory&& err_factory) {
        _timer.set_callback(
          [this, ef = std::forward<ErrorFactory>(err_factory)] {
              using err_t = std::result_of_t<ErrorFactory()>;
              if constexpr (std::is_same_v<err_t, T>) {
                  // if errors are encoded in values f.e. result<T> or errc
                  _promise.set_value(ef());
              } else {
                  _promise.set_exception(ef());
              }
          });
        auto f = _promise.get_future();
        _timer.arm(timeout);
        return std::move(f);
    };

    void set_value(T val) {
        if (_timer.cancel()) {
            _promise.set_value(std::move(val));
        }
    }

    void set_exception(std::exception_ptr&& ex) {
        if (_timer.cancel()) {
            _promise.set_exception(ex);
        }
    }

    void set_exception(const std::exception_ptr& ex) {
        if (_timer.cancel()) {
            _promise.set_exception(ex);
        }
    }

private:
    ss::promise<T> _promise;
    ss::timer<Clock> _timer;
};