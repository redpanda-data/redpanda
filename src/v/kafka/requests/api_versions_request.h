#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct api_versions_response;

struct api_versions_api final {
    using response_type = api_versions_response;

    static constexpr const char* name = "API versions";
    static constexpr api_key key = api_key(18);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct api_versions_request final {
    using api_type = api_versions_api;

    ss::sstring client_software_name;    // >= v3
    ss::sstring client_software_version; // >= v3

    bool valid(api_version version) {
        return version < api_version(3)
               || (!client_software_name.empty() && !client_software_version.empty());
    }

    void decode(request_reader& reader, api_version version);
    void encode(response_writer& writer, api_version version);
};

struct api_versions_response final {
    struct api {
        api_key key;
        api_version min_version;
        api_version max_version;
    };

    error_code error;
    std::vector<api> apis;
    std::chrono::milliseconds throttle = std::chrono::milliseconds(0); // >= v1

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

static bool operator==(
  const api_versions_response::api& a, const api_versions_response::api& b) {
    return a.key == b.key && a.min_version == b.min_version
           && a.max_version == b.max_version;
}

std::vector<api_versions_response::api> get_supported_apis();

} // namespace kafka
