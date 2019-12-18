#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct metadata_response;

struct metadata_api final {
    using response_type = metadata_response;

    static constexpr const char* name = "metadata";
    static constexpr api_key key = api_key(3);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(7);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

struct metadata_request {
    using api_type = metadata_api;

    std::vector<model::topic> topics;
    bool allow_auto_topic_creation = false;             // version >= 4
    bool include_cluster_authorized_operations = false; // version >= 8
    bool include_topic_authorized_operations = false;   // version >= 8

    void encode(response_writer& writer, api_version version);
    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const metadata_request&);

struct metadata_response {
    struct broker {
        model::node_id node_id;
        sstring host;
        int32_t port;
        std::optional<sstring> rack; // version >= 1
    };

    struct partition {
        error_code err_code;
        model::partition_id index;
        model::node_id leader;
        int32_t leader_epoch; // version >= 7
        std::vector<model::node_id> replica_nodes;
        std::vector<model::node_id> offline_replicas; // version >= 5
        void encode(api_version version, response_writer& rw) const;
    };

    struct topic {
        error_code err_code;
        model::topic name;
        bool is_internal; // version >= 1
        std::vector<partition> partitions;
        int32_t topic_authorized_operations; // version >= 8
        void encode(api_version version, response_writer& rw) const;
        static topic make_from_topic_metadata(model::topic_metadata&& tp_md);
    };

    std::chrono::milliseconds throttle_time; // version >= 3
    std::vector<broker> brokers;
    std::optional<sstring> cluster_id; // version >= 2
    model::node_id controller_id;      // version >= 1
    std::vector<topic> topics;
    int32_t cluster_authorized_operations = 0; // version >= 8

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

std::ostream& operator<<(std::ostream&, const metadata_response&);

} // namespace kafka
