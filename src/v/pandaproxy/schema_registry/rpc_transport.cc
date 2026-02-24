/*
 * Copyright 2025 Redpanda Data, Inc.
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

/// Throw a structured exception matching what kafka_client_transport would
/// throw in the equivalent situation. topic_not_exists maps to the SR
/// exception type (matching the kafka client's partition_error catch for
/// unknown_topic_or_partition); all other errors map to kafka::exception.
[[noreturn]] void
throw_as_kafka_error(std::string_view context, cluster::errc ec) {
    if (ec == cluster::errc::topic_not_exists) {
        throw exception(
          kafka::error_code::unknown_topic_or_partition,
          "_schemas topic does not exist");
    }
    throw kafka::exception(
      kafka::error_code::unknown_server_error,
      fmt::format("{}: {}", context, ec));
}

} // namespace

rpc_transport::rpc_transport(kafka::data::rpc::client& client)
  : _client(client) {}

ss::future<produce_result> rpc_transport::produce(model::record_batch batch) {
    auto res = co_await _client.produce_with_offset(
      model::schema_registry_internal_tp, std::move(batch));
    if (res.ec != cluster::errc::success) {
        throw_as_kafka_error("RPC produce failed", res.ec);
    }
    if (!res.base_offset.has_value()) {
        throw kafka::exception(
          kafka::error_code::unknown_server_error,
          "RPC produce succeeded but base_offset not available");
    }
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
  ss::noncopyable_function<ss::future<>(model::record_batch)> consumer) {
    // The RPC consume API may not return all records in a single call,
    // so loop until we've consumed up to the desired end offset.
    constexpr size_t max_bytes = 1 << 20; // 1 MiB per fetch
    auto current = start;
    while (current < end) {
        auto result = co_await _client.consume(
          model::schema_registry_internal_tp,
          offset_cast(current),
          offset_cast(end),
          1,
          max_bytes,
          std::chrono::seconds(5));
        if (result.has_error()) {
            throw_as_kafka_error("RPC consume failed", result.error());
        }
        auto& reply = result.value();
        if (reply.err != cluster::errc::success) {
            throw_as_kafka_error("RPC consume error", reply.err);
        }
        if (reply.batches.empty()) {
            throw kafka::exception(
              kafka::error_code::unknown_server_error, "No records returned");
        }
        for (auto& batch : reply.batches) {
            auto last = batch.last_offset();
            co_await consumer(std::move(batch));
            current = last + model::offset{1};
        }
    }
}

} // namespace pandaproxy::schema_registry
