#pragma once
#include "model/timeout_clock.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>

#include <chrono>

namespace tests {

// clang-format off
template<typename Rep, typename Period, typename Predicate>
CONCEPT(requires requires(Predicate&& p) {
        { p() } -> bool;
})
// clang-format on
/// Used to wait for Prediacate to become true
void cooperative_spin_wait_with_timeout(
  std::chrono::duration<Rep, Period> timeout, Predicate p) {
    ss::with_timeout(
      model::timeout_clock::now() + timeout,
      ss::repeat([p = std::forward<Predicate>(p)]() {
          if (p()) {
              return ss::make_ready_future<ss::stop_iteration>(
                ss::stop_iteration::yes);
          }
          return ss::sleep(std::chrono::milliseconds(10)).then([] {
              return ss::stop_iteration::no;
          });
      }))
      .get();
}
}; // namespace tests