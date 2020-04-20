#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/schemata/leave_group_request.h"
#include "kafka/requests/schemata/leave_group_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
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
    leave_group_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const leave_group_request& r) {
    return os << r.data;
}

struct leave_group_response final {
    leave_group_response_data data;

    explicit leave_group_response(error_code error)
      : data({
        .throttle_time_ms = std::chrono::milliseconds(0),
        .error_code = error,
      }) {}

    void encode(const request_context& ctx, response& resp) {
        data.encode(ctx, resp);
    }
};

static inline ss::future<leave_group_response>
make_leave_error(error_code error) {
    return ss::make_ready_future<leave_group_response>(error);
}

static inline std::ostream&
operator<<(std::ostream& os, const leave_group_response& r) {
    return os << r.data;
}

} // namespace kafka
