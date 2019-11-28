#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"

#include <seastar/core/future.hh>

namespace kafka {

struct heartbeat_api final {
    static constexpr const char* name = "heartbeat";
    static constexpr api_key key = api_key(12);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

struct heartbeat_request final {
    kafka::group_id group_id;
    kafka::generation_id generation_id;
    kafka::member_id member_id;
    std::optional<kafka::group_instance_id> group_instance_id; // >= v3

    void encode(const request_context& ctx, response_writer& writer);
    void decode(request_context& ctx);
};

struct heartbeat_response final {
    std::chrono::milliseconds throttle_time; // >= v1
    error_code error;

    // TODO: throttle will be filled in automatically
    heartbeat_response(error_code error)
      : throttle_time(0)
      , error(error) {}

    void encode(const request_context& ctx, response& resp);
};

} // namespace kafka
