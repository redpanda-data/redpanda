#pragma once

#include "kafka/client.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>

namespace pandaproxy {

struct context_t {
    ss::semaphore mem_sem;
    ss::abort_source as;
    kafka::client& client;
};

} // namespace pandaproxy
