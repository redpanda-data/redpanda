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

#include "transform/rpc/serde.h"

#include "model/record.h"

#include <seastar/core/chunked_fifo.hh>

namespace transform::rpc {
transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, model::record_batch b)
  : tp(std::move(tp)) {
    batches.reserve(1);
    batches.push_back(std::move(b));
}

transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> b)
  : tp(std::move(tp))
  , batches(std::move(b)) {}

transformed_topic_data transformed_topic_data::share() {
    ss::chunked_fifo<model::record_batch> shared;
    shared.reserve(batches.size());
    for (auto& batch : batches) {
        shared.push_back(batch.share());
    }
    return {tp, std::move(shared)};
}

produce_request produce_request::share() {
    ss::chunked_fifo<transformed_topic_data> shared;
    shared.reserve(topic_data.size());
    for (auto& data : topic_data) {
        shared.push_back(data.share());
    }
    return {std::move(shared), timeout};
}
} // namespace transform::rpc
