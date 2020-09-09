#pragma once

#include "pandaproxy/client/client.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>

namespace pandaproxy {

struct context_t {
    ss::semaphore mem_sem;
    ss::abort_source as;
    pandaproxy::client::client& client;
};

} // namespace pandaproxy
