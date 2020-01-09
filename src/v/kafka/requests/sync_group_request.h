#pragma once
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <utility>

namespace kafka {

struct sync_group_api final {
    static constexpr const char* name = "sync group";
    static constexpr api_key key = api_key(14);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct sync_group_request final {
    struct member_assignment {
        kafka::member_id member;
        bytes assignment;
    };

    kafka::group_id group_id;
    kafka::generation_id generation_id;
    kafka::member_id member_id;
    std::optional<kafka::group_instance_id> group_instance_id; // >= v3
    std::vector<member_assignment> assignments;

    void encode(const request_context& ctx, response_writer& writer);
    void decode(request_context& ctx);

    assignments_type member_assignments() && {
        assignments_type res;
        res.reserve(assignments.size());
        std::for_each(
          std::begin(assignments),
          std::end(assignments),
          [&res](member_assignment& a) mutable {
              res.emplace(std::move(a.member), std::move(a.assignment));
          });
        return res;
    }
};

std::ostream& operator<<(std::ostream&, const sync_group_request&);

struct sync_group_response final {
    std::chrono::milliseconds throttle_time; // >= v1
    error_code error;
    bytes assignment;

    sync_group_response(error_code error, bytes assignment)
      : throttle_time(0)
      , error(error)
      , assignment(std::move(assignment)) {}

    explicit sync_group_response(error_code error)
      : throttle_time(0)
      , error(error) {}

    void encode(const request_context& ctx, response& resp);
};

static inline ss::future<sync_group_response>
make_sync_error(error_code error) {
    return ss::make_ready_future<sync_group_response>(error);
}

std::ostream& operator<<(std::ostream&, const sync_group_response&);

} // namespace kafka
