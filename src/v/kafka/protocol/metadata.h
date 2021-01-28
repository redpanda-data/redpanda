/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <chrono>

namespace kafka {

struct metadata_response;

struct metadata_api final {
    using response_type = metadata_response;

    static constexpr const char* name = "metadata";
    static constexpr api_key key = api_key(3);
};

struct metadata_request {
    using api_type = metadata_api;

    std::optional<std::vector<model::topic>> topics;
    bool allow_auto_topic_creation = true;              // version >= 4
    bool include_cluster_authorized_operations = false; // version >= 8
    bool include_topic_authorized_operations = false;   // version >= 8

    bool list_all_topics{false};

    void encode(response_writer& writer, api_version version);
    void decode(request_context& ctx);
};

std::ostream& operator<<(std::ostream&, const metadata_request&);

struct metadata_response {
    using api_type = metadata_api;

    struct broker {
        model::node_id node_id;
        ss::sstring host;
        int32_t port;
        std::optional<ss::sstring> rack; // version >= 1
    };

    struct partition {
        error_code err_code;
        model::partition_id index;
        model::node_id leader;
        int32_t leader_epoch; // version >= 7
        std::vector<model::node_id> replica_nodes;
        std::vector<model::node_id> isr_nodes;
        std::vector<model::node_id> offline_replicas; // version >= 5
        void encode(api_version version, response_writer& rw) const;
    };

    struct topic {
        error_code err_code;
        model::topic name;
        bool is_internal{false}; // version >= 1
        std::vector<partition> partitions;
        int32_t topic_authorized_operations; // version >= 8
        void encode(api_version version, response_writer& rw) const;
        static topic make_from_topic_metadata(model::topic_metadata&& tp_md);
        static metadata_response::topic make_from_topic_metadata(
          model::topic_metadata&& tp_md, model::topic&& topic);
    };

    std::chrono::milliseconds throttle_time = std::chrono::milliseconds(
      0); // version >= 3
    std::vector<broker> brokers;
    std::optional<ss::sstring> cluster_id; // version >= 2
    model::node_id controller_id;          // version >= 1
    std::vector<topic> topics;
    int32_t cluster_authorized_operations = 0; // version >= 8

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

std::ostream& operator<<(std::ostream&, const metadata_response&);

} // namespace kafka
