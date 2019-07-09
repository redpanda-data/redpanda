#pragma once

#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/requests/response.h"

#include <seastar/core/future.hh>

namespace kafka::requests {

class metadata_request final {
public:
    static constexpr api_key key = api_key(3);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(7);

    static seastar::future<response_ptr>
    process(request_context&, seastar::smp_service_group);
};

} // namespace kafka::requests