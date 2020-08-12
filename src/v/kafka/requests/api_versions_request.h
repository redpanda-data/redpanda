#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/api_versions_request.h"
#include "kafka/requests/schemata/api_versions_response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct api_versions_response;

struct api_versions_api final {
    using response_type = api_versions_response;

    static constexpr const char* name = "api-versions";
    static constexpr api_key key = api_key(18);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(2);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct api_versions_request final {
    using api_type = api_versions_api;

    api_versions_request_data data;

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }
};

struct api_versions_response final {
    using api_type = api_versions_api;

    api_versions_response_data data;

    void encode(const request_context& ctx, response& resp) {
        data.encode(resp.writer(), ctx.header().version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

std::ostream& operator<<(std::ostream&, const api_versions_response&);

static inline bool operator==(
  const api_versions_response_key& a, const api_versions_response_key& b) {
    return a.api_key == b.api_key && a.min_version == b.min_version
           && a.max_version == b.max_version;
}

std::vector<api_versions_response_key> get_supported_apis();

} // namespace kafka
