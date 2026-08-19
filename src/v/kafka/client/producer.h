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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "kafka/client/logger.h"
#include "kafka/client/produce_batcher.h"
#include "kafka/client/produce_partition.h"
#include "kafka/client/topic_cache.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"
#include "utils/prefix_logger.h"

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
      producer_configuration config,
      retries_configuration& retries_config,
      topic_cache& topic_cache,
      brokers& brokers,
      prefix_logger& logger,
      error_handler&& error_handler)
      : _config{config}
      , _retries_config(retries_config)
      , _partitions{}
      , _logger(&logger)
      , _error_handler(std::move(error_handler))
      , _topic_cache(topic_cache)
      , _brokers(brokers) {}

    ss::future<produce_response::partition>
    produce(model::topic_partition tp, model::record_batch&& batch);

    ss::future<> stop();

    void set_batch_record_count(int32_t count) {
        _config.batch_record_count = count;
    }

    void set_batch_size_bytes(int32_t size) { _config.batch_size_bytes = size; }

    void set_batch_delay(std::chrono::milliseconds delay) {
        _config.batch_delay = delay;
    }

private:
    friend struct producer_test_access;

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

    producer_configuration _config;
    retries_configuration& _retries_config;
    absl::flat_hash_map<model::topic_partition, shared_produce_partition>
      _partitions;
    /// Partitions with a produce dispatch outstanding; send() rejects a
    /// second dispatch for a partition already present.
    absl::flat_hash_set<model::topic_partition> _in_flight_sends;
    prefix_logger* _logger;
    error_handler _error_handler;
    topic_cache& _topic_cache;
    brokers& _brokers;
    ss::abort_source _as;
    ss::abort_source _ingest_as;
    ss::gate _gate;
};

} // namespace kafka::client
