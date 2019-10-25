#pragma once
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/requests/fwd.h"
#include "redpanda/kafka/types.h"

#include <seastar/core/future.hh>

namespace kafka {

struct leave_group_api final {
    static constexpr const char* name = "leave group";
    static constexpr api_key key = api_key(13);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(2);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

struct leave_group_request final {
    kafka::group_id group_id;
    kafka::member_id member_id;

    void decode(request_context& ctx);
};

struct leave_group_response final {
    std::chrono::milliseconds throttle_time; // >= v1
    error_code error;

    // TODO: throttle will be filled in automatically
    leave_group_response(error_code error)
      : throttle_time(0)
      , error(error) {
    }

    void encode(const request_context& ctx, response& resp);
};

} // namespace kafka
