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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/add_partitions_to_txn_request.h"
#include "kafka/protocol/schemata/add_partitions_to_txn_response.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace kafka {

struct add_partitions_to_txn_request final {
    using api_type = add_partitions_to_txn_api;

    add_partitions_to_txn_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const add_partitions_to_txn_request& r) {
        return os << r.data;
    }
};

struct add_partitions_to_txn_response final {
    using api_type = add_partitions_to_txn_api;

    add_partitions_to_txn_response_data data;

    add_partitions_to_txn_response() = default;
    add_partitions_to_txn_response(
      const add_partitions_to_txn_request& request, error_code error)
      : add_partitions_to_txn_response(
          request, [error](auto) { return error; }) {}

    add_partitions_to_txn_response(
      const add_partitions_to_txn_request& request,
      std::function<error_code(const model::topic&)> err_fn) {
        data.results.reserve(request.data.topics.size());
        for (const auto& topic : request.data.topics) {
            add_partitions_to_txn_topic_result t_result{.name = topic.name};
            t_result.results.reserve(topic.partitions.size());
            for (const auto& partition : topic.partitions) {
                t_result.results.push_back(
                  add_partitions_to_txn_partition_result{
                    .partition_index = partition,
                    .error_code = err_fn(topic.name),
                  });
            }
            data.results.push_back(std::move(t_result));
        }
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const add_partitions_to_txn_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
