/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/produce_batcher.h"
#include "kafka/client/produce_partition.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <absl/container/flat_hash_map.h>

namespace kafka::client {

class brokers;

class producer {
public:
    using error_handler
      = ss::noncopyable_function<ss::future<>(std::exception_ptr)>;
    using shared_produce_partition = ss::lw_shared_ptr<produce_partition>;
    using partitions_t
      = absl::flat_hash_map<model::topic_partition, shared_produce_partition>;

    producer(brokers& brokers, error_handler&& error_handler)
      : _partitions{}
      , _error_handler(std::move(error_handler))
      , _brokers(brokers) {}

    ss::future<produce_response::partition>
    produce(model::topic_partition tp, model::record_batch&& batch);

    ss::future<> stop() {
        return ssx::parallel_transform(
          std::move(_partitions),
          [](partitions_t::value_type p) { return p.second->stop(); });
    }

private:
    ss::future<> send(model::topic_partition tp, model::record_batch&& batch);

    ss::future<produce_response::partition>
    do_send(model::topic_partition tp, model::record_batch&& batch);

    auto make_consumer(model::topic_partition tp) {
        return [this, tp](model::record_batch&& batch) {
            (void)send(tp, std::move(batch));
        };
    }

    shared_produce_partition get_context(model::topic_partition tp) {
        if (auto it = _partitions.find(tp); it != _partitions.end()) {
            return it->second;
        }
        return _partitions
          .emplace(tp, ss::make_lw_shared<produce_partition>(make_consumer(tp)))
          .first->second;
    }

    absl::flat_hash_map<model::topic_partition, shared_produce_partition>
      _partitions;
    error_handler _error_handler;
    brokers& _brokers;
};

} // namespace kafka::client
