/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/protocol/schemata/offset_delete_request.h"
#include "kafka/protocol/schemata/offset_delete_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"

namespace kafka {

struct offset_delete_request final {
    using api_type = offset_delete_api;

    offset_delete_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const offset_delete_request& r) {
        return os << r.data;
    }
};

struct offset_delete_response final {
    using api_type = offset_delete_api;

    offset_delete_response_data data;

    offset_delete_response(error_code error) { data.error_code = error; }

    offset_delete_response(const offset_delete_request&, error_code error)
      : offset_delete_response(error) {}

    offset_delete_response(const offset_delete_request& request) {
        for (const auto& t : request.data.topics) {
            offset_delete_response_topic tmp{.name = t.name};
            for (const auto& p : t.partitions) {
                tmp.partitions.push_back(
                  {.partition_index = p.partition_index});
            }
            data.topics.push_back(std::move(tmp));
        }
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const offset_delete_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
