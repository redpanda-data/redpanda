/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "bytes/iobuf.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "storage/record_batch_builder.h"

namespace tests {

using kv_t = std::pair<ss::sstring, ss::sstring>;
using pid_to_kvs_map_t
  = absl::flat_hash_map<model::partition_id, std::vector<kv_t>>;

// Wrapper around a Kafka transport that encapsulates producing to a node.
//
// The primary goal of this class is to allow tests to produce without dealing
// with the Kafka protocol. To that end, it exposes a protocol-agnostic API.
class kafka_produce_transport {
public:
    using pid_to_offset_map_t
      = absl::flat_hash_map<model::partition_id, model::offset>;

    explicit kafka_produce_transport(kafka::client::transport&& t)
      : _transport(std::move(t)) {}

    ss::future<> start() { return _transport.connect(); }

    // Produces the given records per partition to the given topic.
    ss::future<pid_to_offset_map_t> produce(
      model::topic topic_name,
      pid_to_kvs_map_t records_per_partition,
      std::optional<model::timestamp> ts = std::nullopt);

    // Produces the given records to the given topic partition.
    ss::future<pid_to_offset_map_t> produce_to_partition(
      model::topic topic_name,
      model::partition_id pid,
      std::vector<kv_t> records,
      std::optional<model::timestamp> ts = std::nullopt) {
        pid_to_kvs_map_t m;
        m.emplace(pid, std::move(records));
        return produce(std::move(topic_name), std::move(m), ts);
    }

private:
    // Convert the given records-per-partition mapping to a set of per-partition
    // produce requests. Each request, once sent, will correspond to a
    // replicated Raft batch.
    // NOTE: input must remain valid for the lifetimes of the returned requests.
    static std::vector<kafka::partition_produce_data>
    produce_partition_requests(
      const pid_to_kvs_map_t& records_per_partition,
      std::optional<model::timestamp> ts);

    kafka::client::transport _transport;
};

class kafka_consume_transport {
public:
    explicit kafka_consume_transport(kafka::client::transport&& t)
      : _transport(std::move(t)) {}

    ss::future<> start() { return _transport.connect(); }

    ss::future<pid_to_kvs_map_t> consume(
      model::topic topic_name,
      std::vector<model::partition_id> pids,
      model::offset kafka_offset_inclusive);

    ss::future<std::vector<kv_t>> consume_from_partition(
      model::topic topic_name,
      model::partition_id pid,
      model::offset kafka_offset_inclusive) {
        auto m = co_await consume(topic_name, {pid}, kafka_offset_inclusive);
        if (m.empty()) {
            throw std::runtime_error(
              fmt::format("empty fetch {}/{}", topic_name(), pid()));
        }
        auto it = m.find(pid);
        if (it == m.end()) {
            throw std::runtime_error(fmt::format(
              "fetch result missing partition {}/{}", topic_name(), pid()));
        }
        co_return it->second;
    }

private:
    kafka::client::transport _transport;
};

} // namespace tests
