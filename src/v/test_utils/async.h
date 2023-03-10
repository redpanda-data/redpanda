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
#include "model/timeout_clock.h"
#include "seastarx.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace tests {

// clang-format off
template<typename Rep, typename Period, typename Predicate>
requires ss::ApplyReturns<Predicate, bool> ||
         ss::ApplyReturns<Predicate, ss::future<bool>>
    // clang-format on
    /// Used to wait for Prediacate to become true
    ss::future<> cooperative_spin_wait_with_timeout(
      std::chrono::duration<Rep, Period> timeout, Predicate p) {
    using futurator = ss::futurize<std::invoke_result_t<Predicate>>;
    auto tout = model::timeout_clock::now() + timeout;
    return ss::with_timeout(
      tout, ss::repeat([tout, p = std::forward<Predicate>(p)]() mutable {
          if (model::timeout_clock::now() > tout) {
              return ss::make_exception_future<ss::stop_iteration>(
                ss::timed_out_error());
          }
          auto f = futurator::invoke(p);
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

// When a test expects that any background fibers should complete promptly,
// and wants to send a barrier through all the inter-CPU queues to ensure
// that earlier-submitted tasks have reached their destination cores already.
//
// This is useful in tests that know they have put the system into a state where
// it will get to a known state once all the non-i/o-blocking tasks in flight
// have completed, such as background release of quota units.
//
// **Be aware** that there are assumptions to using this:
// A) That your test code is using the same default scheduling group that
//    this routine will run within.
// B) Seastar's debug-mode shuffling of tasks doesn't re-order stuff so
//    far that resulting delay outruns the invoke_on_all round trip, *and*
//    the timer sleep that we do in debug mode.
// C) You are calling from a seastar thread (.get() is used)
// D) Tasks that have exhausted their scheduling quota and been suspended
//    can still be running after this returns.
inline void flush_tasks() {
    // Ensure anything in inter-CPU queues before we entered the function
    // has drained: this is an all-to-all to cover the full mesh of queues
    // between cores.
    ss::smp::invoke_on_all([]() {
        return ss::smp::invoke_on_all([]() { return ss::yield(); });
    }).get();

    // Yield to anything that ended up runnable on the current core as a result
    // of the above flush.
    ss::thread::yield();

    // Mitigate shuffling task queues in debug mode with a crude sleep
#ifndef NDEBUG
    ss::sleep(10ms).get();
#endif
}

}; // namespace tests
