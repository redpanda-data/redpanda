#pragma once
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

namespace tests {

// clang-format off
template<typename Rep, typename Period, typename Predicate>
CONCEPT(requires ss::ApplyReturns<Predicate, bool> ||
        ss::ApplyReturns<Predicate, ss::future<bool>>)
// clang-format on
/// Used to wait for Prediacate to become true
ss::future<> cooperative_spin_wait_with_timeout(
  std::chrono::duration<Rep, Period> timeout, Predicate p) {
    using futurator = ss::futurize<std::result_of_t<Predicate()>>;
    return ss::with_timeout(
      model::timeout_clock::now() + timeout,
      ss::repeat([p = std::forward<Predicate>(p)]() {
          auto f = futurator::apply(p);
          return f.then([](bool stop) {
              if (stop) {
                  return ss::make_ready_future<ss::stop_iteration>(
                    ss::stop_iteration::yes);
              }
              return ss::sleep(std::chrono::milliseconds(10)).then([] {
                  return ss::stop_iteration::no;
              });
          });
      }));
}
}; // namespace tests
