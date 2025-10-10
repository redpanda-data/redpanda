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

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/protocol/types.h"
#include "model/metadata.h"
#include "storage/record_batch_builder.h"
#include "utils/to_string.h"

#include <seastar/util/log.hh>

#include <chrono>
#include <stdexcept>
#include <utility>

using namespace std::chrono_literals;

namespace tests {

static ss::logger test_log("produce_consume_logger");

std::ostream& operator<<(std::ostream& o, const kv_t& kv) {
    o << ssx::sformat("{{k=\"{}\", v=\"{}\"}}", kv.key, kv.val);
    return o;
}

model::record_batch batch_from_kvs(
  const std::vector<kv_t>& records,
  model::offset base_offset,
  std::optional<model::timestamp> ts,
  model::compression compression_type) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, base_offset);
    builder.set_compression(compression_type);
    for (auto& kv : records) {
        const auto& k = kv.key;
        const auto& v_opt = kv.val;
        iobuf key_buf;
        key_buf.append(k.data(), k.size());
        std::optional<iobuf> val_buf;
        if (v_opt.has_value()) {
            const auto& v = v_opt.value();
            val_buf = iobuf::from({v.data(), v.size()});
        }
        builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
    }
    if (ts.has_value()) {
        builder.set_timestamp(ts.value());
    }
    return std::move(builder).build();
}

// Produces the given records per partition to the given topic.
// NOTE: inputs must remain valid for the duration of the call.
ss::future<kafka_produce_transport::pid_to_offset_map_t>
kafka_produce_transport::produce(
  model::topic topic_name,
  pid_to_kvs_map_t records_per_partition,
  std::optional<model::timestamp> ts,
  model::compression compression_type) {
    kafka::produce_request::topic tp;
    tp.name = topic_name;
    tp.partitions = produce_partition_requests(
      records_per_partition, ts, compression_type);
    chunked_vector<kafka::produce_request::topic> topics;
    topics.push_back(std::move(tp));
    kafka::produce_request req(std::nullopt, -1, std::move(topics));
    req.data.timeout_ms = std::chrono::seconds(10);
    req.has_idempotent = false;
    req.has_transactional = false;
    auto resp = co_await _transport.dispatch(
      std::move(req), kafka::api_version(7));

    pid_to_offset_map_t ret;
    for (auto& data_resp : resp.data.responses) {
        for (auto& prt_resp : data_resp.partitions) {
            if (prt_resp.error_code != kafka::error_code::none) {
                throw std::runtime_error(
                  fmt::format(
                    "produce error: {}, message:{}",
                    prt_resp.error_code,
                    prt_resp.error_message));
            }
            ret.emplace(prt_resp.partition_index, prt_resp.base_offset);
        }
    }
    co_return ret;
}

ss::future<model::offset> kafka_produce_transport::produce_to_partition(
  model::topic topic_name,
  model::partition_id pid,
  std::vector<kv_t> records,
  std::optional<model::timestamp> ts,
  model::compression compression_type) {
    pid_to_kvs_map_t m;
    m.emplace(pid, std::move(records));
    auto ret_m = co_await produce(
      topic_name, std::move(m), ts, compression_type);
    if (ret_m.size() != 1) {
        throw std::runtime_error(
          fmt::format(
            "unexpected produce results {}/{}: {} results",
            topic_name(),
            pid(),
            ret_m.size()));
    }
    auto it = ret_m.find(pid);
    if (it == ret_m.end()) {
        throw std::runtime_error(
          fmt::format(
            "produce result missing partition {}/{}", topic_name(), pid()));
    }
    co_return it->second;
}

chunked_vector<kafka::partition_produce_data>
kafka_produce_transport::produce_partition_requests(
  const pid_to_kvs_map_t& records_per_partition,
  std::optional<model::timestamp> ts,
  model::compression compression_type) {
    chunked_vector<kafka::partition_produce_data> ret;
    ret.reserve(records_per_partition.size());
    for (const auto& [pid, records] : records_per_partition) {
        auto batch = batch_from_kvs(
          records, model::offset(0), ts, compression_type);
        kafka::produce_request::partition partition;
        partition.partition_index = pid;
        partition.records.emplace(std::move(batch));
        ret.emplace_back(std::move(partition));
    }
    return ret;
}

ss::future<kafka::offset> kafka_produce_transport::produce_to_partition(
  model::topic topic_name, model::partition_id pid, model::record_batch batch) {
    chunked_vector<kafka::partition_produce_data> partition_data;
    kafka::produce_request::partition partition;
    auto num_records = batch.record_count();
    partition.partition_index = pid;
    partition.records.emplace(std::move(batch));
    partition_data.emplace_back(std::move(partition));

    kafka::produce_request::topic tp;
    tp.name = topic_name;
    tp.partitions = std::move(partition_data);
    chunked_vector<kafka::produce_request::topic> topics;
    topics.push_back(std::move(tp));
    kafka::produce_request req(std::nullopt, -1, std::move(topics));
    req.data.timeout_ms = std::chrono::seconds(10);
    req.has_idempotent = false;
    req.has_transactional = false;
    auto resp = co_await _transport.dispatch(
      std::move(req), kafka::api_version(7));

    for (auto& data_resp : resp.data.responses) {
        if (data_resp.name != topic_name) {
            continue;
        }
        for (auto& prt_resp : data_resp.partitions) {
            if (prt_resp.partition_index != pid) {
                continue;
            }
            if (prt_resp.error_code != kafka::error_code::none) {
                throw std::runtime_error(
                  fmt::format(
                    "produce error: {}, message:{}",
                    prt_resp.error_code,
                    prt_resp.error_message));
            }
            co_return model::offset_cast(
              prt_resp.base_offset + model::offset_delta(num_records - 1));
        }
    }
    // unreachable
    throw std::runtime_error(
      fmt::format("missing produce result {}/{}", topic_name(), pid()));
}

ss::future<kafka::fetch_response> kafka_consume_transport::raw_consume(
  model::topic topic_name,
  std::vector<model::partition_id> pids,
  std::vector<model::offset> kafka_offsets_inclusive) {
    kafka::fetch_request::topic topic;
    topic.topic = topic_name;
    topic.partitions.reserve(pids.size());
    for (const auto& [pid, offset_inclusive] :
         std::ranges::zip_view(pids, kafka_offsets_inclusive)) {
        kafka::fetch_request::partition partition;
        partition.fetch_offset = offset_inclusive;
        partition.partition = pid;
        partition.log_start_offset = model::offset(0);
        partition.partition_max_bytes = 1_MiB;
        topic.partitions.emplace_back(std::move(partition));
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
    co_return fetch_resp;
}

ss::future<pid_to_kvs_map_t> kafka_consume_transport::consume(
  model::topic topic_name,
  std::vector<model::partition_id> pids,
  model::offset offset_inclusive) {
    std::vector<model::offset> offsets(pids.size(), offset_inclusive);
    auto fetch_resp = co_await raw_consume(
      topic_name, pids, std::move(offsets));
    pid_to_kvs_map_t ret;
    for (const auto& pid : pids) {
        ret.emplace(pid, std::vector<kv_t>{});
    }
    auto& data = fetch_resp.data;
    for (auto& topic : data.responses) {
        vlog(
          test_log.trace,
          "Processing topic {} from the fetch response",
          topic.topic);
        for (auto& partition : topic.partitions) {
            if (partition.error_code != kafka::error_code::none) {
                throw std::runtime_error(
                  fmt::format(
                    "fetch partition error: {}", partition.error_code));
            }
            vlog(
              test_log.trace,
              "Processing ntp {}/{} from the fetch response",
              topic.topic,
              partition.partition_index);
            if (!partition.records.has_value()) {
                vlog(
                  test_log.trace,
                  "No data in ntp {}/{}",
                  topic.topic,
                  partition.partition_index);
                continue;
            }
            while (!partition.records->is_end_of_stream()) {
                auto batch_adapter = partition.records.value().consume_batch();
                if (!batch_adapter.batch.has_value()) {
                    vlog(
                      test_log.trace,
                      "EOS ntp {}/{}",
                      topic.topic,
                      partition.partition_index);
                    break;
                }
                auto records = batch_adapter.batch->copy_records();
                vlog(
                  test_log.trace,
                  "Reading {} records, ntp {}/{}",
                  records.size(),
                  topic.topic,
                  partition.partition_index);
                auto& records_for_partition = ret[partition.partition_index];
                for (auto& r : records) {
                    iobuf_const_parser key_buf(r.key());
                    auto key_str = key_buf.read_string(key_buf.bytes_left());
                    if (r.is_tombstone()) {
                        records_for_partition.emplace_back(key_str);
                    } else {
                        iobuf_const_parser val_buf(r.value());
                        auto val_str = val_buf.read_string(
                          val_buf.bytes_left());
                        records_for_partition.emplace_back(key_str, val_str);
                    }
                }
            }
        }
    }
    co_return ret;
}

ss::future<std::vector<kv_t>> kafka_consume_transport::consume_from_partition(
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
        throw std::runtime_error(
          fmt::format(
            "fetch result missing partition {}/{}", topic_name(), pid()));
    }
    co_return it->second;
}

ss::future<chunked_vector<model::record>>
kafka_consume_transport::raw_consume_from_partition(
  model::topic topic_name,
  model::partition_id pid,
  model::offset kafka_offset_inclusive) {
    std::vector<model::offset> offsets(1, kafka_offset_inclusive);
    auto fetch_resp = co_await raw_consume(
      topic_name, {pid}, std::move(offsets));
    if (fetch_resp.data.error_code != kafka::error_code::none) {
        throw std::runtime_error(
          fmt::format("fetch error: {}", fetch_resp.data.error_code));
    }
    vassert(
      fetch_resp.data.responses.size() == 1,
      "fetch response not populated correctly");
    auto& topic = fetch_resp.data.responses[0];
    vassert(
      topic.topic == topic_name,
      "fetch response topic mismatch, expected: {}, got: {}",
      topic_name,
      topic.topic);
    vassert(
      topic.partitions.size() == 1, "fetch response not populated correctly");
    auto& partition = topic.partitions[0];
    vassert(
      partition.partition_index == pid,
      "fetch response partition mismatch, expected: {}, got: {}",
      pid,
      partition.partition_index);

    chunked_vector<model::record> records;
    if (!partition.records.has_value()) {
        vlog(
          test_log.trace,
          "No data in ntp {}/{}",
          topic.topic,
          partition.partition_index);
        co_return records;
    }
    while (!partition.records->is_end_of_stream()) {
        auto batch_adapter = partition.records.value().consume_batch();
        if (!batch_adapter.batch.has_value()) {
            vlog(
              test_log.trace,
              "EOS ntp {}/{}",
              topic.topic,
              partition.partition_index);
            break;
        }
        for (auto& record : batch_adapter.batch->copy_records()) {
            records.push_back(std::move(record));
        }
    }
    co_return records;
}

ss::future<kafka::offset> kafka_produce_transport::produce_to_partition(
  const model::ntp& ntp, model::record_batch batch) {
    if (ntp.ns != model::kafka_namespace) {
        throw std::runtime_error(
          fmt::format("cannot produce to ntp with namespace {}", ntp.ns));
    }
    return produce_to_partition(
      ntp.tp.topic, ntp.tp.partition, std::move(batch));
}

} // namespace tests
