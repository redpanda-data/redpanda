#pragma once

#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace model {

using timeout_clock = seastar::lowres_clock;

static constexpr timeout_clock::time_point no_timeout
  = timeout_clock::time_point::max();

} // namespace model
