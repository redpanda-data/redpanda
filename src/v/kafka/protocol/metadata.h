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

#include "kafka/protocol/schemata/metadata_request.h"
#include "kafka/protocol/schemata/metadata_response.h"
#include "model/metadata.h"
#include "seastarx.h"

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

    friend std::ostream&
    operator<<(std::ostream& os, const metadata_request& r) {
        return os << r.data;
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

    friend std::ostream&
    operator<<(std::ostream& os, const metadata_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
