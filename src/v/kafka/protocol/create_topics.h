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
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/create_topics_response.h"

#include <seastar/core/future.hh>

#include <unordered_map>

namespace kafka {

struct create_topics_request final {
    using api_type = create_topics_api;

    create_topics_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const create_topics_request& r) {
        return os << r.data;
    }
};

struct create_topics_response final {
    using api_type = create_topics_api;

    create_topics_response_data data;

    create_topics_response() = default;

    create_topics_response(
      error_code ec,
      std::optional<ss::sstring> error_message,
      create_topics_response current_response,
      create_topics_request current_request) {
        data.topics.reserve(
          current_response.data.topics.size()
          + current_request.data.topics.size());

        std::transform(
          current_response.data.topics.begin(),
          current_response.data.topics.end(),
          std::back_inserter(data.topics),
          [ec, &error_message](creatable_topic_result& r) {
              r.error_code = ec;
              r.error_message = error_message;
              r.topic_config_error_code = ec;
              return std::move(r);
          });

        std::transform(
          current_request.data.topics.begin(),
          current_request.data.topics.end(),
          std::back_inserter(data.topics),
          [ec, &error_message](const creatable_topic& t) {
              return creatable_topic_result{
                .name = t.name,
                .error_code = ec,
                .error_message = error_message,
                .topic_config_error_code = ec};
          });
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const create_topics_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
