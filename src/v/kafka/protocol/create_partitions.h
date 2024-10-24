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
#include "kafka/protocol/schemata/create_partitions_request.h"
#include "kafka/protocol/schemata/create_partitions_response.h"

#include <seastar/core/future.hh>

namespace kafka {

struct create_partitions_request final {
    using api_type = create_partitions_api;

    create_partitions_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const create_partitions_request& r) {
        return os << r.data;
    }
};

struct create_partitions_response final {
    using api_type = create_partitions_api;

    create_partitions_response_data data;

    create_partitions_response() = default;

    create_partitions_response(
      error_code ec,
      std::optional<ss::sstring> error_message,
      create_partitions_response current_resp,
      create_partitions_request request_data,
      std::vector<create_partitions_topic>::difference_type request_topic_end) {
        data.results.reserve(
          current_resp.data.results.size() + request_data.data.topics.size());
        std::transform(
          current_resp.data.results.begin(),
          current_resp.data.results.end(),
          std::back_inserter(data.results),
          [ec, &error_message](const create_partitions_topic_result& r) {
              return create_partitions_topic_result{
                .name = r.name,
                .error_code = ec,
                .error_message = error_message,
              };
          });
        std::transform(
          request_data.data.topics.begin(),
          request_data.data.topics.begin() + request_topic_end,
          std::back_inserter(data.results),
          [ec, &error_message](const create_partitions_topic& t) {
              return create_partitions_topic_result{
                .name = t.name,
                .error_code = ec,
                .error_message = error_message};
          });
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const create_partitions_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
