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

#include "ssx/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include <utility>

namespace ssx {

// We use named semaphores because the provided name will be included in
// exception messages, making diagnosing broken or timed-out semaphores much
// easier.

template<typename Clock = seastar::timer<>::clock>
class named_semaphore
  : public seastar::
      basic_semaphore<seastar::named_semaphore_exception_factory, Clock> {
public:
    named_semaphore(size_t count, seastar::sstring name)
      : seastar::
        basic_semaphore<seastar::named_semaphore_exception_factory, Clock>(
          count, seastar::named_semaphore_exception_factory{name})
      , _original_count(static_cast<ssize_t>(count))
      , _name(std::move(name)) {}

    named_semaphore(named_semaphore&) = default;
    named_semaphore(named_semaphore&&) noexcept = default;
    named_semaphore& operator=(const named_semaphore&) = default;
    named_semaphore& operator=(named_semaphore&&) noexcept = default;

    ~named_semaphore() noexcept {
        auto available_units = seastar::basic_semaphore<
          seastar::named_semaphore_exception_factory,
          Clock>::available_units();

        auto interesting = _name.find("raft") == 0 || _name.find("rpc") == 0;
        if (interesting && available_units < _original_count) {
            vlog(
              ssxlogger.info,
              "Semaphore destroyed before releasing units: {}",
              _name);
        }
        /*
        vassert(
          !interesting || available_units >= _original_count,
          "Semaphore {} destroyed before units are returned, original {}, "
          "available {}",
          _name,
          _original_count,
          available_units);
          */
    }

private:
    ssize_t _original_count;
    seastar::sstring _name;
};

using semaphore = named_semaphore<>;

using semaphore_units = seastar::semaphore_units<semaphore::exception_factory>;

} // namespace ssx
