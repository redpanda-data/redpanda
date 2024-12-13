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

#include "kafka/data/rpc/service.h"

#include "kafka/data/partition_proxy.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "utils/uuid.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/switch_to.hh>

#include <iterator>
#include <memory>
#include <system_error>
#include <utility>

namespace kafka::data::rpc {
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
  std::unique_ptr<kafka::data::rpc::topic_metadata_cache> metadata_cache,
  std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager)
  : _metadata_cache(std::move(metadata_cache))
  , _partition_manager(std::move(partition_manager)) {}

ss::future<ss::chunked_fifo<kafka_topic_data_result>> local_service::produce(
  ss::chunked_fifo<kafka_topic_data> topic_data,
  model::timeout_clock::duration timeout) {
    ss::chunked_fifo<kafka_topic_data_result> results;
    constexpr size_t max_concurrent_produces = 10;
    ss::semaphore sem(max_concurrent_produces);
    co_await ss::parallel_for_each(
      std::make_move_iterator(topic_data.begin()),
      std::make_move_iterator(topic_data.end()),
      [this, timeout, &results, &sem](kafka_topic_data data) {
          return ss::with_semaphore(
            sem,
            1,
            [this, timeout, &results, data = std::move(data)]() mutable {
                return produce(std::move(data), timeout)
                  .then([&results](kafka_topic_data_result r) {
                      results.push_back(std::move(r));
                  });
            });
      });
    co_return results;
}

ss::future<kafka_topic_data_result> local_service::produce(
  kafka_topic_data data, model::timeout_clock::duration timeout) {
    auto ktp = model::ktp(data.tp.topic, data.tp.partition);
    auto result = co_await produce(ktp, std::move(data.batches), timeout);
    auto ec = result.has_error() ? result.error() : cluster::errc::success;
    co_return kafka_topic_data_result(data.tp, ec);
}

ss::future<result<model::offset, cluster::errc>> local_service::produce(
  model::any_ntp auto ntp,
  ss::chunked_fifo<model::record_batch> batches,
  model::timeout_clock::duration timeout) {
    auto shard = _partition_manager->shard_owner(ntp);
    if (!shard) {
        co_return cluster::errc::not_leader;
    }

    auto topic_cfg = _metadata_cache->find_topic_cfg(
      model::topic_namespace_view(ntp));
    if (!topic_cfg) {
        co_return cluster::errc::topic_not_exists;
    }
    // TODO: More validation of the batches, such as null record rejection and
    // crc checks.
    uint32_t max_batch_size = topic_cfg->properties.batch_max_bytes.value_or(
      _metadata_cache->get_default_batch_max_bytes());
    for (const auto& batch : batches) {
        if (uint32_t(batch.size_bytes()) > max_batch_size) [[unlikely]] {
            co_return cluster::errc::invalid_request;
        }
    }
    auto rdr = model::make_foreign_fragmented_memory_record_batch_reader(
      std::move(batches));
    co_return co_await _partition_manager->invoke_on_shard(
      *shard,
      ntp,
      [timeout, r = std::move(rdr)](kafka::partition_proxy* partition) mutable {
          return partition
            ->replicate(std::move(r), make_replicate_options(timeout))
            .then(
              [](result<model::offset> r)
                -> result<model::offset, cluster::errc> {
                  if (r.has_error()) {
                      return map_errc(r.assume_error());
                  }
                  return r.value();
              });
      });
}

ss::future<produce_reply>
network_service::produce(produce_request req, ::rpc::streaming_context&) {
    co_await ss::coroutine::switch_to(get_scheduling_group());
    auto results = co_await _service->local().produce(
      std::move(req.topic_data), req.timeout);
    co_return produce_reply(std::move(results));
}

} // namespace kafka::data::rpc
