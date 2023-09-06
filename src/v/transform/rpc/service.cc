/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/rpc/service.h"

#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "transform/rpc/serde.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>

#include <algorithm>
#include <iterator>
#include <system_error>
#include <vector>

namespace transform::rpc {
namespace {
raft::replicate_options
make_replicate_options(model::timeout_clock::duration timeout) {
    return {
      raft::consistency_level::quorum_ack,
      std::chrono::duration_cast<std::chrono::milliseconds>(timeout)};
}
cluster::errc map_errc(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return static_cast<cluster::errc>(ec.value());
    }
    if (ec.category() == raft::error_category()) {
        switch (static_cast<raft::errc>(ec.value())) {
        case raft::errc::not_leader:
        case raft::errc::replicated_entry_truncated:
            return cluster::errc::not_leader;
        case raft::errc::shutting_down:
        default:
            return cluster::errc::timeout;
        }
    }
    return cluster::errc::replication_error;
}
} // namespace

local_service::local_service(
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  ss::sharded<cluster::partition_manager>* partition_manager)
  : _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _partition_manager(partition_manager) {}

ss::future<ss::chunked_fifo<transformed_topic_data_result>>
local_service::produce(
  ss::chunked_fifo<transformed_topic_data> topic_data,
  model::timeout_clock::duration timeout) {
    ss::chunked_fifo<transformed_topic_data_result> results;
    constexpr size_t max_concurrent_produces = 10;
    ss::semaphore sem(max_concurrent_produces);
    co_await ss::parallel_for_each(
      std::make_move_iterator(topic_data.begin()),
      std::make_move_iterator(topic_data.end()),
      [this, timeout, &results, &sem](transformed_topic_data data) {
          return ss::with_semaphore(
            sem,
            1,
            [this, timeout, &results, data = std::move(data)]() mutable {
                return produce(std::move(data), timeout)
                  .then([&results](transformed_topic_data_result r) {
                      results.push_back(std::move(r));
                  });
            });
      });
    co_return results;
}

ss::future<transformed_topic_data_result> local_service::produce(
  transformed_topic_data data, model::timeout_clock::duration timeout) {
    auto ktp = model::ktp(data.tp.topic, data.tp.partition);
    auto shard = _shard_table->local().shard_for(ktp);
    if (!shard) {
        co_return transformed_topic_data_result(
          data.tp, cluster::errc::not_leader);
    }

    auto topic_cfg = _metadata_cache->local().get_topic_cfg(ktp.as_tn_view());
    if (!topic_cfg) {
        co_return transformed_topic_data_result(
          data.tp, cluster::errc::topic_not_exists);
    }
    // TODO: More validation of the batches, such as null record rejection and
    // crc checks.
    uint32_t max_batch_size = topic_cfg->properties.batch_max_bytes.value_or(
      _metadata_cache->local().get_default_batch_max_bytes());
    for (const auto& batch : data.batches) {
        if (uint32_t(batch.size_bytes()) > max_batch_size) [[unlikely]] {
            co_return transformed_topic_data_result(
              data.tp, cluster::errc::invalid_request);
        }
    }
    auto rdr = model::make_foreign_fragmented_memory_record_batch_reader(
      std::move(data.batches));
    // TODO: schema validation
    auto ec = co_await _partition_manager->invoke_on(
      *shard,
      [&ktp, timeout](
        cluster::partition_manager& mgr,
        model::record_batch_reader rdr) mutable {
          auto partition = mgr.get(ktp);
          if (!partition || !partition->is_leader()) {
              return ss::make_ready_future<cluster::errc>(
                cluster::errc::not_leader);
          }
          if (partition->is_read_replica_mode_enabled()) {
              return ss::make_ready_future<cluster::errc>(
                cluster::errc::invalid_request);
          }
          return partition
            ->replicate(std::move(rdr), make_replicate_options(timeout))
            .then([](result<cluster::kafka_result> result) {
                if (result.has_error()) {
                    return map_errc(result.assume_error());
                }
                return cluster::errc::success;
            });
      },
      std::move(rdr));

    co_return transformed_topic_data_result(data.tp, ec);
}

ss::future<produce_reply>
network_service::produce(produce_request&& req, ::rpc::streaming_context&) {
    auto results = co_await _service->local().produce(
      std::move(req.topic_data), req.timeout);
    co_return produce_reply(std::move(results));
}

} // namespace transform::rpc
