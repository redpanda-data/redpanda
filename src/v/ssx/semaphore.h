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
            count,
            seastar::named_semaphore_exception_factory{std::move(name)}) {}
};

using semaphore = named_semaphore<>;

using semaphore_units = seastar::semaphore_units<semaphore::exception_factory>;

} // namespace ssx
