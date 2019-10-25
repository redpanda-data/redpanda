#pragma once

#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

class find_coordinator_api final {
public:
    static constexpr const char* name = "find coordinator";
    static constexpr api_key key = api_key(10);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(2);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

} // namespace kafka
