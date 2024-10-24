#pragma once
#include "base/seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace cluster {
ss::future<bool>
sleep_abortable(std::chrono::milliseconds dur, ss::abort_source& as);
}
