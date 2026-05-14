/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "kafka/protocol/schemata/metadata_request.h"
#include "kafka/protocol/schemata/metadata_response.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>

#include <chrono>

namespace kafka {

struct metadata_request {
    using api_type = metadata_api;

    metadata_request_data data;

    bool list_all_topics{false};

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
        if (version > api_version(0)) {
            list_all_topics = !data.topics;
        } else {
            if (unlikely(!data.topics)) {
                // Version 0 of protocol doesn't use nullable topics set
                throw std::runtime_error(
                  "Null topics received for version 0 of metadata request");
            }
            // For metadata API version 0, empty array requests all topics
            list_all_topics = data.topics->empty();
        }
    }

    metadata_request copy() const {
        static_assert(
          api_type::max_valid == api_version(13),
          "Please update the metadata_request::copy method when updating the "
          "Metadata API");
        return {
          .data{
            .topics
            = data.topics.has_value()
                ? std::make_optional<chunked_vector<metadata_request_topic>>(
                    data.topics->copy())
                : std::nullopt,
            .allow_auto_topic_creation = data.allow_auto_topic_creation,
            .include_cluster_authorized_operations
            = data.include_cluster_authorized_operations,
            .include_topic_authorized_operations
            = data.include_topic_authorized_operations,
            .unknown_tags = data.unknown_tags},
          .list_all_topics = list_all_topics,
        };
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

struct metadata_response {
    using api_type = metadata_api;
    using topic = metadata_response_topic;
    using partition = metadata_response_partition;
    using broker = metadata_response_broker;

    metadata_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{}", data);
    }
};

} // namespace kafka
