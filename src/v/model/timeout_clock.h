/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace model {

using timeout_clock = ss::lowres_clock;

static constexpr timeout_clock::time_point no_timeout
  = timeout_clock::time_point::max();

static constexpr timeout_clock::duration max_duration
  = timeout_clock::duration::max();

} // namespace model
