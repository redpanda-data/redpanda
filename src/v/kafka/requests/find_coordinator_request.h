#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
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

enum class coordinator_type : int8_t {
    group = 0,
    transaction = 1,
};

struct find_coordinator_request final {
    using api_type = find_coordinator_api;

    ss::sstring key;
    coordinator_type key_type{coordinator_type::group}; // >= v1

    find_coordinator_request(request_context& ctx) { decode(ctx); }
    find_coordinator_request(ss::sstring key)
      : key(key)
      , key_type(coordinator_type::group) {}

    void encode(response_writer& writer, api_version version);
    void decode(request_context& ctx);
};

struct find_coordinator_response final {
    std::chrono::milliseconds throttle{0}; // >= v1
    error_code error;
    std::optional<ss::sstring> error_message; // >= v1
    model::node_id node;
    ss::sstring host;
    int32_t port;

    find_coordinator_response() = default;

    find_coordinator_response(
      error_code error, model::node_id node, ss::sstring host, int32_t port)
      : error(error)
      , node(node)
      , host(std::move(host))
      , port(port) {}

    find_coordinator_response(
      model::node_id node, ss::sstring host, int32_t port)
      : find_coordinator_response(
        error_code::none, node, std::move(host), port) {}

    explicit find_coordinator_response(error_code error)
      : find_coordinator_response(error, model::node_id(-1), "", -1) {}

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

} // namespace kafka
