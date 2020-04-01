#pragma once

#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct offset_commit_response;

struct offset_commit_api final {
    using response_type = offset_commit_response;

    static constexpr const char* name = "offset commit";
    static constexpr api_key key = api_key(8);
    static constexpr api_version min_supported = api_version(1);
    static constexpr api_version max_supported = api_version(7);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct offset_commit_request final {
    using api_type = offset_commit_api;

    struct partition {
        model::partition_id id;
        model::offset committed;
        model::timestamp commit_timestamp; // == v1
        int32_t leader_epoch;              // >= v6
        std::optional<ss::sstring> metadata;
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    kafka::group_id group_id;
    kafka::generation_id generation_id;
    kafka::member_id member_id;
    std::optional<std::chrono::milliseconds> retention_time_ms; // >= v2, < v5
    std::optional<kafka::group_instance_id> group_instance_id;  // >= v7
    std::vector<topic> topics;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    offset_commit_request() = default;
    offset_commit_request(request_context& ctx) { decode(ctx); }

    void encode(response_writer& writer, api_version version);
    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const offset_commit_request&);

struct offset_commit_response final {
    using api_type = offset_commit_api;

    struct partition {
        model::partition_id id;
        error_code error;
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    std::chrono::milliseconds throttle_time_ms{0}; // >= v3
    std::vector<topic> topics;

    offset_commit_response() = default;

    offset_commit_response(
      const offset_commit_request& request, error_code error) {
        for (const auto& t : request.topics) {
            topic tmp{.name = t.name};
            for (const auto& p : t.partitions) {
                tmp.partitions.push_back({.id = p.id, error});
            }
            topics.push_back(std::move(tmp));
        }
    }

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

std::ostream& operator<<(std::ostream&, const offset_commit_response&);

} // namespace kafka
