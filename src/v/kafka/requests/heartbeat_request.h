#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct heartbeat_api final {
    static constexpr const char* name = "heartbeat";
    static constexpr api_key key = api_key(12);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct heartbeat_request final {
    kafka::group_id group_id;
    kafka::generation_id generation_id;
    kafka::member_id member_id;
    std::optional<kafka::group_instance_id> group_instance_id; // >= v3

    void encode(const request_context& ctx, response_writer& writer);
    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const heartbeat_request&);

struct heartbeat_response final {
    std::chrono::milliseconds throttle_time = std::chrono::milliseconds(
      0); // >= v1
    error_code error;

    explicit heartbeat_response(error_code error)
      : throttle_time(0)
      , error(error) {}

    void encode(const request_context& ctx, response& resp);
};

static inline ss::future<heartbeat_response>
make_heartbeat_error(error_code error) {
    return ss::make_ready_future<heartbeat_response>(error);
}

std::ostream& operator<<(std::ostream&, const heartbeat_response&);

} // namespace kafka
