#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/find_coordinator_request.h"
#include "kafka/requests/schemata/find_coordinator_response.h"
#include "kafka/types.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct find_coordinator_response;

class find_coordinator_api final {
public:
    using response_type = find_coordinator_response;

    static constexpr const char* name = "find coordinator";
    static constexpr api_key key = api_key(10);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(2);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct find_coordinator_request final {
    using api_type = find_coordinator_api;

    find_coordinator_request_data data;

    find_coordinator_request() = default;

    find_coordinator_request(
      ss::sstring key, coordinator_type key_type = coordinator_type::group)
      : data({
        .key = std::move(key),
        .key_type = key_type,
      }) {}

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.key_type = coordinator_type::group;
        data.decode(reader, version);
    }
};

struct find_coordinator_response final {
    using api_type = find_coordinator_api;

    find_coordinator_response_data data;

    find_coordinator_response()
      : data({.throttle_time_ms = std::chrono::milliseconds(0)}) {}

    find_coordinator_response(
      error_code error, model::node_id node, ss::sstring host, int32_t port)
      : data({
        .throttle_time_ms = std::chrono::milliseconds(0),
        .error_code = error,
        .node_id = node,
        .host = std::move(host),
        .port = port,
      }) {}

    find_coordinator_response(
      model::node_id node, ss::sstring host, int32_t port)
      : find_coordinator_response(
        error_code::none, node, std::move(host), port) {}

    explicit find_coordinator_response(error_code error)
      : find_coordinator_response(error, model::node_id(-1), "", -1) {}

    void encode(const request_context& ctx, response& resp) {
        data.encode(resp.writer(), ctx.header().version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const find_coordinator_response& r) {
    return os << r.data;
}

} // namespace kafka
