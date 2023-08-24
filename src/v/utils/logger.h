#pragma once

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace fanout {
inline ss::logger fanout_log("fanout");
} // namespace fanout
