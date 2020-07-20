#pragma once
#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/schemata/alter_configs_request.h"
#include "kafka/requests/schemata/alter_configs_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace kafka {

struct alter_configs_response;

class alter_configs_api final {
public:
    using response_type = alter_configs_response;

    static constexpr const char* name = "alter_configs";
    static constexpr api_key key = api_key(33);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(1);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct alter_configs_request final {
    using api_type = alter_configs_api;

    alter_configs_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const alter_configs_request& r) {
    return os << r.data;
}

struct alter_configs_response final {
    using api_type = alter_configs_api;

    alter_configs_response_data data;

    void encode(const request_context& ctx, response& resp) {
        data.encode(ctx, resp);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const alter_configs_response& r) {
    return os << r.data;
}

} // namespace kafka
