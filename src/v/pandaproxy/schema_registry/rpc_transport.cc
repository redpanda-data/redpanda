/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/rpc_transport.h"

#include "kafka/data/rpc/client.h"
#include "kafka/protocol/exceptions.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandaproxy/schema_registry/exceptions.h"

#include <seastar/core/coroutine.hh>

namespace pandaproxy::schema_registry {

namespace {

/// Translate cluster::errc from the rpc layer into a pp::sr or kafka
/// exception with the goal of fitting directly into existing error
/// handling logic in service & seq_writer. timeout & not_leader are
/// of particular interest since most error paths will treat those
/// as retryable. unknown_server_error is the catchall for other faults
/// and is generally surfaced to the API. For that reason, faults
/// unique to the RPC layer may now appear in 500 error bodies.
/// TODO: get rid of the kafka::exception dependency if possible and
/// make error handling at the next layer up generic to "transport".
[[noreturn]] void
throw_as_kafka_error(std::string_view context, cluster::errc ec) {
    switch (ec) {
    case cluster::errc::topic_not_exists:
        throw exception(
          kafka::error_code::unknown_server_error,
          "_schemas topic does not exist");
    case cluster::errc::not_leader:
        throw kafka::exception(
          kafka::error_code::not_leader_for_partition,
          fmt::format("{}: {}", context, ec));
    case cluster::errc::timeout:
        throw kafka::exception(
          kafka::error_code::request_timed_out,
          fmt::format("{}: {}", context, ec));
    default:
        throw kafka::exception(
          kafka::error_code::unknown_server_error,
          fmt::format("{}: {}", context, ec));
    }
}

} // namespace

rpc_transport::rpc_transport(kafka::data::rpc::client& client)
  : _client(client) {}

ss::future<produce_result> rpc_transport::produce(model::record_batch batch) {
    auto res = co_await _client.produce_with_leader_mitigation(
      model::schema_registry_internal_tp, std::move(batch));
    if (res.ec != cluster::errc::success) {
        throw_as_kafka_error("RPC produce failed", res.ec);
    }
    vassert(
      res.base_offset.has_value(),
      "Possible RPC produce version mismatch (response incomplete)");
    co_return produce_result{.base_offset = *res.base_offset};
}

ss::future<model::offset> rpc_transport::get_high_watermark() {
    auto result = co_await _client.get_single_partition_offsets(
      model::schema_registry_internal_tp);
    if (result.has_error()) {
        throw_as_kafka_error(
          "RPC get_partition_offsets failed", result.error());
    }
    co_return kafka::offset_cast(result.value().high_watermark);
}

ss::future<> rpc_transport::consume_range(
  model::offset start,
  model::offset end,
  ss::noncopyable_function<ss::future<ss::stop_iteration>(model::record_batch)>
    consumer) {
    // The RPC consume API may not return all records in a single call,
    // so loop until we've consumed up to the desired end offset.

    // Matches the Kafka transport default. KIP-74 obligatory batch
    // makes oversized batch handling identical across transports for
    // the single-partition _schemas topic.
    constexpr size_t consume_max_bytes = 1_MiB;
    constexpr auto consume_timeout = 5s;
    auto current = start;
    // Adapt the existing exclusive upper bound semantics of the old kafka
    // reader to the inclusive upper bound semantics of the record batch reader
    // in the RPC service.
    auto end_inclusive = model::prev_offset(end);
    while (current < end) {
        auto result = co_await _client.consume(
          model::schema_registry_internal_tp,
          offset_cast(current),
          offset_cast(end_inclusive),
          1,
          consume_max_bytes,
          consume_timeout);
        if (result.has_error()) {
            throw_as_kafka_error("RPC consume failed", result.error());
        }
        auto& reply = result.value();
        if (reply.err != cluster::errc::success) {
            throw_as_kafka_error("RPC consume error", reply.err);
        }
        if (reply.batches.empty()) {
            // TODO: map to a retryable error_code (e.g. leader_not_available)
            // so service::do_start retries this. Note that the kclient version
            // has this issue in client_fetch_batch_reader.cc. Fix both
            // together.
            throw kafka::exception(
              kafka::error_code::unknown_server_error,
              fmt::format(
                "No records returned in range [{}, {})", current, end));
        }
        for (auto& batch : reply.batches) {
            auto last = batch.last_offset();
            auto stop = co_await consumer(std::move(batch));
            current = model::next_offset(last);
            if (stop == ss::stop_iteration::yes) {
                co_return;
            }
        }
    }
}

ss::future<cluster::errc> rpc_transport::create_topic(
  model::topic_namespace_view tp_ns,
  int32_t partition_count,
  cluster::topic_properties properties,
  int16_t replication_factor) {
    co_return co_await _client.create_topic(
      tp_ns, std::move(properties), partition_count, replication_factor);
}

} // namespace pandaproxy::schema_registry
