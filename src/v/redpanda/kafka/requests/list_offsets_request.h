#pragma once

#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka::requests {

class list_offsets_api final {
public:
    static constexpr const char* name = "list_offsets";
    static constexpr api_key key = api_key(2);
    static constexpr api_version min_supported = api_version(1);
    static constexpr api_version max_supported = api_version(1);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

} // namespace kafka::requests
