#pragma once
#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
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

    struct resource {
        int8_t type;
        ss::sstring name;
        std::vector<ss::sstring> config_names;
    };

    std::vector<resource> resources;
    bool include_synonyms{false}; // >= v1

    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const describe_configs_request&);

struct describe_configs_response final {
    using api_type = describe_configs_api;

    struct config_synonym {
        ss::sstring name;
        std::optional<ss::sstring> value;
        int8_t source;
    };

    struct config {
        ss::sstring name;
        std::optional<ss::sstring> value;
        bool read_only{false};
        bool is_default{false}; // == v0
        int8_t source{-1};      // >= v1
        bool is_sensitive{false};
        std::vector<config_synonym> synonyms; // >= v1
    };

    struct resource {
        error_code error;
        std::optional<ss::sstring> error_msg;
        int8_t type;
        ss::sstring name;
        std::vector<config> configs;
    };

    std::chrono::milliseconds throttle_time_ms{0};
    std::vector<resource> results;

    void encode(const request_context& ctx, response& resp);
};

std::ostream& operator<<(std::ostream&, const describe_configs_response&);

} // namespace kafka
