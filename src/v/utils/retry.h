#pragma once
#include "utils/concepts-enabled.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

class exp_backoff_policy {
public:
    inline uint32_t next_backoff() {
        return std::min<uint32_t>(
          300, std::max<uint32_t>(1, _current_backoff) << 1);
    }

private:
    uint32_t _current_backoff = 0;
};
// clang-format off

CONCEPT(
template<typename Policy>
concept BackoffPolicy = requires (Policy b) {
    { b.next_backoff() } -> uint32_t;
};
)
// clang-format on

// clang-format off
template<typename Func, typename Policy = exp_backoff_policy>
CONCEPT(
  requires requires (Func f) {
      { f() } -> seastar::futurize_t<std::result_of_t<Func()>>;
  } 
  && BackoffPolicy<Policy>
)
// clang-format on
seastar::futurize_t<std::result_of_t<Func()>> retry_with_backoff(
  int max_retries, Func&& f) {
    using seastar::stop_iteration;
    using ret = seastar::futurize<std::result_of_t<Func()>>;
    return seastar::do_with(
      0,
      Policy{},
      (typename ret::promise_type){},
      [f = std::forward<Func>(f), max_retries](
        int& attempt,
        Policy& backoff_policy,
        typename ret::promise_type& promise) mutable {
          return seastar::repeat([&attempt,
                                  f = std::forward<Func>(f),
                                  &backoff_policy,
                                  &promise,
                                  max_retries]() mutable {
                     attempt++;
                     return f()
                       .then([&promise](auto... vals) {
                           // success case
                           promise.set_value(vals...);
                           return seastar::make_ready_future<stop_iteration>(
                             stop_iteration::yes);
                       })
                       .handle_exception(
                         [&attempt, &backoff_policy, max_retries, &promise](
                           std::exception_ptr e) mutable {
                             if (attempt > max_retries) {
                                 // out ot attempts
                                 promise.set_exception(e);
                                 return seastar::make_ready_future<
                                   stop_iteration>(stop_iteration::yes);
                             }

                             // retry

                             auto next = backoff_policy.next_backoff();
                             return seastar::sleep(std::chrono::seconds(next))
                               .then([] { return stop_iteration::no; });
                         });
                 })
            .then([&promise] { return promise.get_future(); });
      });
}