#pragma once

#include "redpanda/kafka/groups/types.h"
#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka::requests {

class request_context;
class response;
using response_ptr = foreign_ptr<std::unique_ptr<response>>;

struct sync_group_api final {
    // api
    static constexpr const char* name = "sync group";
    static constexpr api_key key = api_key(14);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static future<response_ptr> process(request_context&&, smp_service_group);

    // request message
    struct member_assignment {
        kafka::member_id member;
        bytes assignment;
    };

    kafka::group_id group_id;
    kafka::generation_id generation_id;
    kafka::member_id member_id;
    std::optional<kafka::group_instance_id> group_instance_id; // >= v3
    std::vector<member_assignment> assignments;

    void decode(request_context& ctx);
};

struct sync_group_response final {
    std::chrono::milliseconds throttle_time; // >= v1
    errors::error_code error;
    bytes assignment;

    // TODO: throttle will be filled in automatically
    explicit sync_group_response(bytes assignment)
      : throttle_time(0)
      , error(errors::error_code::none)
      , assignment(assignment) {
    }

    explicit sync_group_response(errors::error_code error)
      : throttle_time(0)
      , error(error) {
    }

    void encode(const request_context& ctx, response& resp);
};

} // namespace kafka::requests
