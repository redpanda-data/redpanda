#pragma once

#include "seastarx.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace seastar {
class smp_service_group;
}

namespace kafka {
class request_context;
class response_writer;

class response;
using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;
} // namespace kafka
