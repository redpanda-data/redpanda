/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/likely.h"
#include "base/seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

namespace pandaproxy {

///\brief The first invocation of one_shot::operator()() will invoke func and
/// wait for it to finish. Concurrent invocatons will also wait.
///
/// On success, all waiters will be allowed to continue. Successive invocations
/// of one_shot::operator()() will return a ready future.
///
/// If func fails, waiters will receive the error, and one_shot will be reset.
/// Successive calls to operator()() will restart the process.
class one_shot {
public:
    explicit one_shot(ss::noncopyable_function<ss::future<>()> func)
      : _func{std::move(func)} {}
    ss::future<> operator()() {
        if (likely(_started_sem.available_units() != 0)) {
            // Ensure we can get a unit that the oneshot has completed
            // then return the units.w:
            return ss::get_units(_started_sem, 1).discard_result();
        }
        auto units = ss::consume_units(_started_sem, 1);
        return _func().then_wrapped(
          [this, units{std::move(units)}](ss::future<> f) mutable noexcept {
              if (f.failed()) {
                  units.release();
                  auto ex = f.get_exception();
                  _started_sem.broken(ex);
                  _started_sem = {0, "pproxy/oneshot"};
                  return ss::make_exception_future(ex);
              }

              _started_sem.signal(_started_sem.max_counter());
              return ss::make_ready_future();
          });
    }

private:
    ss::noncopyable_function<ss::future<>()> _func;
    ssx::semaphore _started_sem{0, "pproxy/oneshot"};
};

} // namespace pandaproxy
