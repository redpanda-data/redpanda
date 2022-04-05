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

#include "kafka/protocol/errors.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/protocol/schemata/produce_response.h"
#include "kafka/types.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct produce_response;

/**
 * Support starts at version 3 because this is the first version that supports
 * version 2 of the kafka message format.
 */
class produce_api final {
public:
    using response_type = produce_response;

    static constexpr const char* name = "produce";
    static constexpr api_key key = api_key(0);
};

struct produce_response;

struct produce_request final {
    using api_type = produce_api;
    using partition = partition_produce_data;
    using topic = topic_produce_data;

    produce_request_data data;

    produce_request() = default;

    produce_request(
      std::optional<ss::sstring> t_id,
      int16_t acks,
      std::vector<produce_request::topic> topics) {
        if (t_id) {
            data.transactional_id = transactional_id(std::move(*t_id));
        }
        data.acks = acks;
        data.topics = std::move(topics);
    }

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    /**
     * Build a generic error response for a given request.
     */
    produce_response make_error_response(error_code error) const;

    /// True if the request contains a batch with a transactional id.
    bool has_transactional = false;

    /// True if the request contains a batch with a producer id.
    bool has_idempotent = false;
};

inline std::ostream& operator<<(std::ostream& os, const produce_request& r) {
    return os << r.data;
}

struct produce_response final {
    using api_type = produce_api;
    using partition = partition_produce_response;
    using topic = topic_produce_response;

    produce_response_data data;

    void encode(response_writer& writer, api_version version) {
        // normalize errors
        for (auto& r : data.responses) {
            for (auto& p : r.partitions) {
                if (p.error_code != error_code::none) {
                    p.base_offset = model::offset(-1);
                    p.log_append_time_ms = model::timestamp(-1);
                    p.log_start_offset = model::offset(-1);
                }
            }
        }
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream& operator<<(std::ostream& os, const produce_response& r) {
    return os << r.data;
}

} // namespace kafka
