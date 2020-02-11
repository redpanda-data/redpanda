#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct leave_group_api final {
    static constexpr const char* name = "leave group";
    static constexpr api_key key = api_key(13);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(2);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct leave_group_request final {
    kafka::group_id group_id;
    kafka::member_id member_id;

    void encode(const request_context& ctx, response_writer& writer);
    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const leave_group_request&);

struct leave_group_response final {
    std::chrono::milliseconds throttle_time = std::chrono::milliseconds(
      0); // >= v1
    error_code error;

    explicit leave_group_response(error_code error)
      : throttle_time(0)
      , error(error) {}

    void encode(const request_context& ctx, response& resp);
};

static inline ss::future<leave_group_response>
make_leave_error(error_code error) {
    return ss::make_ready_future<leave_group_response>(error);
}

std::ostream& operator<<(std::ostream&, const leave_group_response&);

} // namespace kafka
