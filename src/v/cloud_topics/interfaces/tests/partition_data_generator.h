//
// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
//

#pragma once

#include "cluster/partition.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/timestamp.h"

namespace tests {

class partition_data_generator {
public:
    partition_data_generator(
      kafka::client::transport transport, cluster::partition& partition)
      : _producer(std::move(transport))
      , _partition(partition) {}

    partition_data_generator& total_number_of_records(size_t n) {
        _records_target = n;
        return *this;
    }
    partition_data_generator& records_per_batch(size_t r) {
        _records_per_batch = r;
        return *this;
    }
    kafka_produce_transport& producer() { return _producer; }

    ss::future<int> produce() {
        co_await _producer.start();
        size_t total_records = 0;
        auto timestamp = model::timestamp::now();
        for (size_t i = 0; i < _records_target; i += _records_per_batch) {
            std::vector<kv_t> records = kv_t::sequence(
              total_records, std::min(_records_per_batch, _records_target - i));
            total_records += _records_per_batch;
            co_await _producer.produce_to_partition(
              _partition.ntp().tp.topic,
              _partition.ntp().tp.partition,
              std::move(records),
              timestamp);
        }
        co_return total_records;
    }

private:
    kafka_produce_transport _producer;
    cluster::partition& _partition;
    size_t _records_per_batch{1};
    size_t _records_target{1};
};

} // namespace tests
