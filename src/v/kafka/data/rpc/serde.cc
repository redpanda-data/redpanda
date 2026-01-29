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

fmt::iterator kafka_topic_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ tp: {}, batches_size: {} }}", tp, batches.size());
}

fmt::iterator produce_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ topic_data: {}, timeout: {} }}",
      fmt::join(topic_data, ", "),
      timeout);
}

fmt::iterator kafka_topic_data_result::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ tp: {}, err: {} }}", tp, err);
}

fmt::iterator produce_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ results: {} }}", fmt::join(results, ", "));
}

fmt::iterator topic_partitions::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ topic: {}, partitions: {} }}",
      topic,
      fmt::join(partitions, ", "));
}

fmt::iterator partition_offsets::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ high_watermark: {}, last_stable_offset: {} }}",
      high_watermark,
      last_stable_offset);
}

fmt::iterator partition_offset_result::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ err: {}, offsets: {} }}", err, offsets);
}

fmt::iterator get_offsets_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ topics: {} }}", fmt::join(topics, ", "));
}

fmt::iterator get_offsets_reply::format_to(fmt::iterator it) const {
    fmt::format_to(it, "{{ partition_offsets: [");
    bool first_topic = true;
    for (const auto& [topic, partitions] : partition_offsets) {
        if (!first_topic) {
            fmt::format_to(it, ", ");
        }
        first_topic = false;
        fmt::format_to(it, "{{ topic: {}, partitions: [", topic);
        bool first_partition = true;
        for (const auto& [partition_id, result] : partitions) {
            if (!first_partition) {
                fmt::format_to(it, ", ");
            }
            first_partition = false;
            fmt::format_to(
              it, "{{ partition: {}, result: {} }}", partition_id, result);
        }
        fmt::format_to(it, "] }}");
    }
    return fmt::format_to(it, "] }}");
}

fmt::iterator consume_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ tp: {}, start_offset: {}, max_offset: {}, min_bytes: {}, "
      "max_bytes: {}, timeout: {} }}",
      tp,
      start_offset,
      max_offset,
      min_bytes,
      max_bytes,
      timeout);
}

fmt::iterator consume_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ tp: {}, err: {}, batches_size: {} }}", tp, err, batches.size());
}

} // namespace kafka::data::rpc
