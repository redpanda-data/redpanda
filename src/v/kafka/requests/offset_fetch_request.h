#pragma once

#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct offset_fetch_response;

class offset_fetch_api final {
public:
    using response_type = offset_fetch_response;

    static constexpr const char* name = "offset fetch";
    static constexpr api_key key = api_key(9);
    static constexpr api_version min_supported = api_version(2);
    static constexpr api_version max_supported = api_version(4);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct offset_fetch_request final {
    using api_type = offset_fetch_api;

    struct topic {
        model::topic name;
        std::vector<model::partition_id> partitions;
    };

    kafka::group_id group_id;
    std::optional<std::vector<topic>> topics;

    offset_fetch_request() = default;
    offset_fetch_request(request_context& ctx) { decode(ctx); }

    void encode(response_writer& writer, api_version version);
    void decode(request_context& ctx);
};

struct offset_fetch_response final {
    struct partition {
        model::partition_id id;
        model::offset offset;
        std::optional<ss::sstring> metadata;
        error_code error;
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    std::chrono::milliseconds throttle_time_ms{0}; // >= v3
    std::vector<topic> topics;
    error_code error;

    offset_fetch_response() = default;

    offset_fetch_response(error_code error)
      : error(error) {}

    offset_fetch_response(
      std::optional<std::vector<offset_fetch_request::topic>>& topics)
      : error(error_code::none) {
        if (topics) {
            for (auto& topic : *topics) {
                std::vector<partition> partitions;
                for (auto id : topic.partitions) {
                    partitions.push_back(
                      partition{id, model::offset(-1), "", error_code::none});
                }
            }
        }
    }

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

} // namespace kafka
