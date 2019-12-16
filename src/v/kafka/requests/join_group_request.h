#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"

#include <seastar/core/future.hh>

namespace kafka {

struct join_group_api final {
    static constexpr const char* name = "join group";
    static constexpr api_key key = api_key(11);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

struct join_group_request final {
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

    void encode(const request_context& ctx, response_writer& writer);
    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const join_group_request&);

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
    kafka::error_code error;
    kafka::generation_id generation_id;
    kafka::protocol_name protocol_name;
    kafka::member_id leader_id;
    kafka::member_id member_id;
    std::vector<member_config> members;

    join_group_response(
      kafka::error_code error,
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
      , members(std::move(members)) {}

    void encode(const request_context& ctx, response& resp);
};

static inline const kafka::member_id no_member("");
static inline const kafka::member_id no_leader("");
static constexpr kafka::generation_id no_generation(-1);
static inline const kafka::protocol_name no_protocol("");

static inline join_group_response
_make_join_error(kafka::member_id member_id, error_code error) {
    return join_group_response(
      error, no_generation, no_protocol, no_leader, std::move(member_id));
}

static inline future<join_group_response>
make_join_error(kafka::member_id member_id, error_code error) {
    return make_ready_future<join_group_response>(
      _make_join_error(member_id, error));
}

std::ostream& operator<<(std::ostream&, const join_group_response&);

} // namespace kafka
