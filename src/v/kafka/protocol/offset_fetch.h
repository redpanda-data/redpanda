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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/offset_fetch_request.h"
#include "kafka/protocol/schemata/offset_fetch_response.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace kafka {

struct offset_fetch_request final {
    using api_type = offset_fetch_api;

    offset_fetch_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const offset_fetch_request& r) {
        return os << r.data;
    }
};

struct offset_fetch_response final {
    using api_type = offset_fetch_api;

    offset_fetch_response_data data;

    offset_fetch_response() = default;

    offset_fetch_response(error_code error) { data.error_code = error; }

    offset_fetch_response(const offset_fetch_request&, error_code error)
      : offset_fetch_response(error) {}

    offset_fetch_response(
      std::optional<std::vector<offset_fetch_request_topic>>& topics) {
        data.error_code = error_code::none;
        if (topics) {
            for (auto& topic : *topics) {
                small_fragment_vector<offset_fetch_response_partition>
                  partitions;
                for (auto id : topic.partition_indexes) {
                    offset_fetch_response_partition p = {
                      .partition_index = id,
                      .committed_offset = model::offset(-1),
                      .metadata = "",
                      .error_code = error_code::none,
                    };
                    partitions.push_back(std::move(p));
                }
                offset_fetch_response_topic t = {
                  .name = topic.name,
                  .partitions = std::move(partitions),
                };
                data.topics.push_back(std::move(t));
            }
        }
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const offset_fetch_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
