#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

class api_versions_api final {
    static constexpr api_version v0 = api_version(0);

public:
    static constexpr const char* name = "API versions";
    static constexpr api_key key = api_key(18);
    static constexpr api_version min_supported = v0;
    static constexpr api_version max_supported = api_version(2);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

} // namespace kafka
