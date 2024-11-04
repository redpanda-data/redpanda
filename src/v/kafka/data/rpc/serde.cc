/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/data/rpc/serde.h"

namespace kafka::data::rpc {

kafka_topic_data::kafka_topic_data(
  model::topic_partition tp, model::record_batch b)
  : tp(std::move(tp)) {
    batches.reserve(1);
    batches.push_back(std::move(b));
}

kafka_topic_data::kafka_topic_data(
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> b)
  : tp(std::move(tp))
  , batches(std::move(b)) {}

kafka_topic_data kafka_topic_data::share() {
    ss::chunked_fifo<model::record_batch> shared;
    shared.reserve(batches.size());
    for (auto& batch : batches) {
        shared.push_back(batch.share());
    }
    return {tp, std::move(shared)};
}

produce_request produce_request::share() {
    ss::chunked_fifo<kafka::data::rpc::kafka_topic_data> shared;
    shared.reserve(topic_data.size());
    for (auto& data : topic_data) {
        shared.push_back(data.share());
    }
    return {std::move(shared), timeout};
}

std::ostream& operator<<(std::ostream& os, const produce_request& req) {
    fmt::print(
      os,
      "{{ topic_data: {}, timeout: {} }}",
      fmt::join(req.topic_data, ", "),
      req.timeout);
    return os;
}

std::ostream& operator<<(std::ostream& os, const produce_reply& reply) {
    fmt::print(os, "{{ results: {} }}", fmt::join(reply.results, ", "));
    return os;
}

std::ostream&
operator<<(std::ostream& os, const kafka_topic_data_result& result) {
    fmt::print(os, "{{ errc: {}, tp: {} }}", result.err, result.tp);
    return os;
}

std::ostream& operator<<(std::ostream& os, const kafka_topic_data& data) {
    fmt::print(
      os, "{{ tp: {}, batches_size: {} }}", data.tp, data.batches.size());
    return os;
}

} // namespace kafka::data::rpc
