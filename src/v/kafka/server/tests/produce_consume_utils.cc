/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "kafka/server/tests/produce_consume_utils.h"

#include "bytes/iobuf.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "storage/record_batch_builder.h"
#include "vlog.h"

#include <seastar/util/log.hh>

namespace tests {

static ss::logger test_log("produce_consume_logger");

// Produces the given records per partition to the given topic.
// NOTE: inputs must remain valid for the duration of the call.
ss::future<kafka_produce_transport::pid_to_offset_map_t>
kafka_produce_transport::produce(
  model::topic topic_name,
  pid_to_kvs_map_t records_per_partition,
  std::optional<model::timestamp> ts) {
    kafka::produce_request::topic tp;
    tp.name = topic_name;
    tp.partitions = produce_partition_requests(records_per_partition, ts);
    std::vector<kafka::produce_request::topic> topics;
    topics.push_back(std::move(tp));
    kafka::produce_request req(std::nullopt, -1, std::move(topics));
    req.data.timeout_ms = std::chrono::seconds(10);
    req.has_idempotent = false;
    req.has_transactional = false;
    auto resp = co_await _transport.dispatch(std::move(req));

    pid_to_offset_map_t ret;
    for (auto& data_resp : resp.data.responses) {
        for (auto& prt_resp : data_resp.partitions) {
            if (prt_resp.error_code != kafka::error_code::none) {
                throw std::runtime_error(fmt::format(
                  "produce error: {}, message:{}",
                  prt_resp.error_code,
                  prt_resp.error_message));
            }
            ret.emplace(prt_resp.partition_index, prt_resp.base_offset);
        }
    }
    co_return ret;
}

std::vector<kafka::partition_produce_data>
kafka_produce_transport::produce_partition_requests(
  const pid_to_kvs_map_t& records_per_partition,
  std::optional<model::timestamp> ts) {
    std::vector<kafka::partition_produce_data> ret;
    ret.reserve(records_per_partition.size());
    for (const auto& [pid, records] : records_per_partition) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        kafka::produce_request::partition partition;
        for (auto& [k, v] : records) {
            iobuf key_buf;
            key_buf.append(k.data(), k.size());
            iobuf val_buf;
            val_buf.append(v.data(), v.size());
            builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
        }
        if (ts.has_value()) {
            builder.set_timestamp(ts.value());
        }
        partition.partition_index = pid;
        partition.records.emplace(std::move(builder).build());
        ret.emplace_back(std::move(partition));
    }
    return ret;
}

ss::future<pid_to_kvs_map_t> kafka_consume_transport::consume(
  model::topic topic_name,
  std::vector<model::partition_id> pids,
  model::offset offset_inclusive) {
    kafka::fetch_request::topic topic;
    topic.name = topic_name;
    topic.fetch_partitions.reserve(pids.size());
    for (const auto& pid : pids) {
        kafka::fetch_request::partition partition;
        partition.fetch_offset = offset_inclusive;
        partition.partition_index = pid;
        partition.log_start_offset = model::offset(0);
        partition.max_bytes = 1_MiB;
        topic.fetch_partitions.emplace_back(std::move(partition));
    }

    kafka::fetch_request req;
    req.data.min_bytes = 1;
    req.data.max_bytes = 10_MiB;
    req.data.max_wait_ms = 1000ms;
    req.data.topics.push_back(std::move(topic));
    auto fetch_resp = co_await _transport.dispatch(
      std::move(req), kafka::api_version(4));
    if (fetch_resp.data.error_code != kafka::error_code::none) {
        throw std::runtime_error(
          fmt::format("fetch error: {}", fetch_resp.data.error_code));
    }
    vlog(test_log.debug, "Received response from the kafka api");
    pid_to_kvs_map_t ret;
    for (const auto& pid : pids) {
        ret.emplace(pid, std::vector<kv_t>{});
    }
    auto& data = fetch_resp.data;
    for (auto& topic : data.topics) {
        vlog(
          test_log.trace,
          "Processing topic {} from the fetch response",
          topic.name);
        for (auto& partition : topic.partitions) {
            if (partition.error_code != kafka::error_code::none) {
                throw std::runtime_error(fmt::format(
                  "fetch partition error: {}", partition.error_code));
            }
            vlog(
              test_log.trace,
              "Processing ntp {}/{} from the fetch response",
              topic.name,
              partition.partition_index);
            if (!partition.records.has_value()) {
                vlog(
                  test_log.trace,
                  "No data in ntp {}/{}",
                  topic.name,
                  partition.partition_index);
                continue;
            }
            while (!partition.records->is_end_of_stream()) {
                auto batch_adapter = partition.records.value().consume_batch();
                if (!batch_adapter.batch.has_value()) {
                    vlog(
                      test_log.trace,
                      "EOS ntp {}/{}",
                      topic.name,
                      partition.partition_index);
                    break;
                }
                auto records = batch_adapter.batch->copy_records();
                vlog(
                  test_log.trace,
                  "Reading {} records, ntp {}/{}",
                  records.size(),
                  topic.name,
                  partition.partition_index);
                auto& records_for_partition = ret[partition.partition_index];
                for (auto& r : records) {
                    iobuf_const_parser key_buf(r.key());
                    iobuf_const_parser val_buf(r.value());
                    records_for_partition.emplace_back(kv_t{
                      key_buf.read_string(key_buf.bytes_left()),
                      val_buf.read_string(val_buf.bytes_left())});
                }
            }
        }
    }
    co_return ret;
}

} // namespace tests
