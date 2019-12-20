#pragma once
#include "rpc/exceptions.h"
#include "rpc/types.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/future.hh>

namespace rpc {
namespace internal {
class response_handler {
public:
    using response_ptr = std::unique_ptr<streaming_context>;
    using promise_t = promise<response_ptr>;
    using timer_ptr = std::unique_ptr<rpc::timer_type>;

    response_handler() = default;
    // clang-format off
    template<typename Func>
    CONCEPT(requires requires(Func f){
        {f()} -> void;
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

    response_handler(response_handler&&) = default;
    response_handler& operator=(response_handler&&) = default;

    future<response_ptr> get_future() { return _promise.get_future(); }

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
        // TODO: Replace with error code when RPC layer will
        //       be integrated with outcome
        set_exception(
          request_timeout_exception("Timeout while waiting for response"));
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
} // namespace internal
} // namespace rpc