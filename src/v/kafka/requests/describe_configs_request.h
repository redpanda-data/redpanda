#pragma once
#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/schemata/describe_configs_request.h"
#include "kafka/requests/schemata/describe_configs_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace kafka {

struct describe_configs_response;

class describe_configs_api final {
public:
    using response_type = describe_configs_response;

    static constexpr const char* name = "describe_configs";
    static constexpr api_key key = api_key(32);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(2);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct describe_configs_request final {
    using api_type = describe_configs_api;

    describe_configs_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const describe_configs_request& r) {
    return os << r.data;
}

struct describe_configs_response final {
    using api_type = describe_configs_api;

    describe_configs_response_data data;

    describe_configs_response()
      : data({.throttle_time_ms = std::chrono::milliseconds(0)}) {}

    void encode(const request_context& ctx, response& resp) {
        data.encode(ctx, resp);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const describe_configs_response& r) {
    return os << r.data;
}

} // namespace kafka
