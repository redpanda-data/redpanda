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

struct join_group_api final {
    // api
    static constexpr const char* name = "join group";
    static constexpr api_key key = api_key(11);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static future<response_ptr> process(request_context&&, smp_service_group);

    // request message
    kafka::group_id group_id;
    std::chrono::milliseconds session_timeout;
    std::chrono::milliseconds rebalance_timeout; // >= v1
    kafka::member_id member_id;
    std::optional<kafka::group_instance_id> group_instance_id; // >= v5
    kafka::protocol_type protocol_type;
    std::vector<member_protocol> protocols;

    // extra context from request header
    api_version version;
    std::optional<sstring> client_id;

    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const join_group_api&);

/*
 * TODO
 * - auto fill throttle for all reply types
 */
struct join_group_response final {
    struct member_config {
        kafka::member_id member_id;
        std::optional<kafka::group_instance_id> group_instance_id; // >= v5
        bytes metadata;
    };

    std::chrono::milliseconds throttle_time; // >= v2
    errors::error_code error;
    kafka::generation_id generation_id;
    kafka::protocol_name protocol_name;
    kafka::member_id leader_id;
    kafka::member_id member_id;
    std::vector<member_config> members;

    join_group_response(
      errors::error_code error,
      kafka::generation_id generation_id,
      kafka::protocol_name protocol_name,
      kafka::member_id leader_id,
      kafka::member_id member_id,
      std::vector<member_config> members = {})
      : throttle_time(0)
      , error(error)
      , generation_id(std::move(generation_id))
      , protocol_name(std::move(protocol_name))
      , leader_id(std::move(leader_id))
      , member_id(std::move(member_id))
      , members(std::move(members)) {
    }

    void encode(const request_context& ctx, response& resp);
};

std::ostream& operator<<(std::ostream&, const join_group_response&);

} // namespace kafka::requests
