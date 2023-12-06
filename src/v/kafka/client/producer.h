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

#include "kafka/client/logger.h"
#include "kafka/client/produce_batcher.h"
#include "kafka/client/produce_partition.h"
#include "kafka/client/topic_cache.h"
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

    producer(
      const configuration& config,
      topic_cache& topic_cache,
      brokers& brokers,
      int16_t acks,
      error_handler&& error_handler)
      : _config{config}
      , _partitions{}
      , _error_handler(std::move(error_handler))
      , _topic_cache(topic_cache)
      , _brokers(brokers)
      , _acks(acks) {}

    ss::future<produce_response::partition>
    produce(model::topic_partition tp, model::record_batch&& batch);

    ss::future<> stop();

private:
    ss::future<> send(model::topic_partition tp, model::record_batch&& batch);

    ss::future<produce_response::partition>
    do_send(model::topic_partition tp, model::record_batch batch);

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
          .emplace(
            tp,
            ss::make_lw_shared<produce_partition>(_config, make_consumer(tp)))
          .first->second;
    }

    const configuration& _config;
    absl::flat_hash_map<model::topic_partition, shared_produce_partition>
      _partitions;
    error_handler _error_handler;
    topic_cache& _topic_cache;
    brokers& _brokers;
    int16_t _acks;
    ss::abort_source _as;
    ss::abort_source _ingest_as;
    ss::gate _gate;
};

} // namespace kafka::client
