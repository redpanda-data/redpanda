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

#include "likely.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/util/noncopyable_function.hh>

namespace pandaproxy {

///\brief The first invocation of one_shot::operator()() will invoke func and
/// wait for it to finish. Concurrent invocatons will also wait.
///
/// On success, all waiters will be allowed to continue. Successive invocations
/// of one_shot::operator()() will return ss::now().
///
/// If func fails, waiters will receive the error, and one_shot will be reset.
/// Successive calls to operator()() will restart the process.
class one_shot {
    enum class state { empty, started, available };
    using futurator = ss::futurize<ssx::semaphore_units>;

public:
    explicit one_shot(ss::noncopyable_function<ss::future<>()> func)
      : _func{std::move(func)} {}
    futurator::type operator()() {
        if (likely(_started_sem.available_units() != 0)) {
            return ss::get_units(_started_sem, 1);
        }
        auto units = ss::consume_units(_started_sem, 1);
        return _func().then_wrapped(
          [this, units{std::move(units)}](ss::future<> f) mutable noexcept {
              if (f.failed()) {
                  units.release();
                  auto ex = f.get_exception();
                  _started_sem.broken(ex);
                  _started_sem = {0, "pproxy/oneshot"};
                  return futurator::make_exception_future(ex);
              }

              _started_sem.signal(_started_sem.max_counter());
              return futurator::convert(std::move(units));
          });
    }

private:
    ss::noncopyable_function<ss::future<>()> _func;
    ssx::semaphore _started_sem{0, "pproxy/oneshot"};
};

} // namespace pandaproxy
