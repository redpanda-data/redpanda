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

#include "transform/logging/rpc_client.h"

#include "cluster/metadata_cache.h"
#include "hashing/murmur.h"
#include "logger.h"
#include "model/namespace.h"
#include "record_batcher.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <absl/algorithm/container.h>

namespace transform::logging {

rpc_client::rpc_client(
  rpc::client* rpc_client, cluster::metadata_cache* metadata_cache)
  : _rpc_client(rpc_client)
  , _metadata_cache(metadata_cache) {}

ss::future<errc>
rpc_client::write(model::partition_id pid, io::json_batches batches) {
    model::topic_namespace_view ns_tp{model::transform_log_internal_nt};
    auto cfg = _metadata_cache->get_topic_cfg(ns_tp);
    if (!cfg.has_value()) {
        co_return errc::topic_not_found;
    }

    auto max_batch_size = cfg->properties.batch_max_bytes.value_or(
      config::shard_local_cfg().kafka_batch_max_bytes());

    record_batcher batcher{max_batch_size};

    while (!batches.empty()) {
        auto json_batch = std::move(batches.front());
        batches.pop_front();
        while (!json_batch.events.empty()) {
            auto ev = std::move(json_batch.events.front());
            json_batch.events.pop_front();
            batcher.append(json_batch.name.copy(), std::move(ev));
            co_await ss::coroutine::maybe_yield();
        }
    }

    auto record_batches = batcher.finish();

    size_t total_request_size = absl::c_accumulate(
      record_batches, size_t(0), [](size_t acc, const auto& b) {
          return acc + b.size_bytes();
      });

    model::topic_partition tp{model::transform_log_internal_topic, pid};
    vlog(
      tlg_log.debug,
      "Producing logs for {{ {} }} total size: {}B",
      tp,
      total_request_size);

    auto res = co_await _rpc_client->produce(
      std::move(tp), std::move(record_batches));
    if (res != cluster::errc::success) {
        vlog(
          tlg_log.debug, "Produce error: {}", std::error_code{res}.message());
        co_return errc::write_failure;
    }
    co_return errc::success;
}

result<model::partition_id, errc>
rpc_client::compute_output_partition(model::transform_name_view name) {
    model::topic_namespace_view ns_tp{model::transform_log_internal_nt};
    const auto& config = _metadata_cache->get_topic_cfg(ns_tp);
    if (!config) {
        return errc::topic_not_found;
    }
    auto n_partitions = static_cast<uint32_t>(config->partition_count);
    if (n_partitions == 0) {
        return errc::partition_lookup_failure;
    }
    return model::partition_id{static_cast<int32_t>(
      murmur2(name().data(), name().size()) % n_partitions)};
}

ss::future<errc> rpc_client::initialize() {
    auto fut = co_await ss::coroutine::as_future<errc>(create_topic());
    if (fut.failed()) {
        vlog(tlg_log.warn, "Init error: {}", fut.get_exception());
        co_return errc::topic_creation_failure;
    }
    co_return fut.get();
}

ss::future<errc> rpc_client::create_topic() {
    auto ec = co_await _rpc_client->create_transform_logs_topic();
    co_return (
      ec == cluster::errc::success || ec == cluster::errc::topic_already_exists
        ? errc::success
        : errc::topic_creation_failure);
}

} // namespace transform::logging
