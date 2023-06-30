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

#include "kafka/client/transport.h"

#include <seastar/core/coroutine.hh>

namespace tests {

// Wrapper around a Kafka transport that encapsulates listing offsets.
//
// The primary goal of this is to allow tests to delete without dealing
// explicitly with the Kafka schemata. To that end, it exposes a
// protocol-agnostic API.
class kafka_list_offsets_transport {
public:
    // NOTE: returned offsets are kafka offsets
    using pid_to_timestamp_map_t
      = absl::flat_hash_map<model::partition_id, model::timestamp>;
    using pid_to_offset_map_t
      = absl::flat_hash_map<model::partition_id, model::offset>;

    explicit kafka_list_offsets_transport(kafka::client::transport&& t)
      : _transport(std::move(t)) {}

    ss::future<> start() { return _transport.connect(); }

    ss::future<model::offset> start_offset_for_partition(
      model::topic topic_name, model::partition_id pid);

    ss::future<model::offset> high_watermark_for_partition(
      model::topic topic_name, model::partition_id pid);

    ss::future<pid_to_offset_map_t> list_offsets(
      model::topic topic_name, pid_to_timestamp_map_t ts_per_partition);

    ss::future<model::offset> list_offset_for_partition(
      model::topic topic_name, model::partition_id pid, model::timestamp ts) {
        pid_to_timestamp_map_t m;
        m.emplace(pid, ts);
        auto out_map = co_await list_offsets(
          std::move(topic_name), std::move(m));
        co_return out_map[pid];
    }

private:
    kafka::client::transport _transport;
};

} // namespace tests
