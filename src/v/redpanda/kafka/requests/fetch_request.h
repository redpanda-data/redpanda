#pragma once

#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka::requests {

class fetch_api final {
public:
    static constexpr const char* name = "fetch";
    static constexpr api_key key = api_key(1);
    static constexpr api_version min_supported = api_version(4);
    static constexpr api_version max_supported = api_version(4);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

} // namespace kafka::requests
