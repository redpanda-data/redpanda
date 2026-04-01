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

#include "cluster/errc.h"
#include "kafka/data/log_reader_config.h"
#include "kafka/data/partition_proxy.h"
#include "logger.h"
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

using namespace std::chrono_literals;

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
  std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager,
  std::unique_ptr<kafka::data::rpc::shadow_link_registry> shadow_link_registry)
  : _metadata_cache(std::move(metadata_cache))
  , _partition_manager(std::move(partition_manager))
  , _shadow_link_registry(std::move(shadow_link_registry)) {}

ss::future<> local_service::stop() {
    _as.request_abort();
    co_await _gate.close();
}

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

ss::future<partition_offsets_map>
local_service::get_offsets(chunked_vector<topic_partitions> topics) {
    static constexpr int concurrency_limit = 32;
    partition_offsets_map results;
    for (auto& t : topics) {
        results.reserve(topics.size());
        results[t.topic].reserve(t.partitions.size());
        auto& partition_results = results[t.topic];
        co_await ss::max_concurrent_for_each(
          t.partitions.begin(),
          t.partitions.end(),
          concurrency_limit,
          [this, &partition_results, topic = t.topic](model::partition_id p) {
              return get_partition_offsets(topic, p).then(
                [&partition_results, topic, p](
                  result<partition_offsets, cluster::errc> r) {
                    if (r.has_error()) {
                        partition_results.emplace(
                          p, partition_offset_result(r.error()));
                        return;
                    }
                    partition_results.emplace(
                      p, partition_offset_result(r.value()));
                });
          });
    }

    co_return results;
}

ss::future<result<partition_offsets, cluster::errc>>
local_service::get_partition_offsets(
  model::topic topic, model::partition_id p_id) {
    model::ktp ktp(topic, p_id);
    auto shard = _partition_manager->shard_owner(ktp);
    if (!shard) {
        co_return cluster::errc::not_leader;
    }

    auto topic_cfg = _metadata_cache->find_topic_cfg(
      ::model::topic_namespace_view(model::kafka_namespace, topic));
    if (!topic_cfg) {
        co_return cluster::errc::topic_not_exists;
    }
    co_return co_await _partition_manager->get_offsets_from_shard(
      *shard, ktp, [](kafka::partition_proxy* partition) {
          using ret_t = result<partition_offsets, cluster::errc>;
          if (!partition->is_leader()) {
              return ssx::now<ret_t>(cluster::errc::not_leader);
          }
          auto lso_r = partition->last_stable_offset();
          if (lso_r.has_error()) {
              return ssx::now<ret_t>(cluster::errc::partition_operation_failed);
          }

          return ssx::now<ret_t>(partition_offsets{
            .high_watermark = model::offset_cast(partition->high_watermark()),
            .last_stable_offset = model::offset_cast(lso_r.value()),
          });
      });
}

ss::future<consume_reply> local_service::consume(consume_request req) {
    auto ktp = model::ktp(req.tp.topic, req.tp.partition);
    auto result = co_await consume(
      ktp,
      req.start_offset,
      req.max_offset,
      req.min_bytes,
      req.max_bytes,
      req.timeout);

    if (result.has_error()) {
        co_return consume_reply(req.tp, result.error(), {});
    }
    co_return consume_reply(
      req.tp, cluster::errc::success, std::move(result.value()));
}

ss::future<result<chunked_vector<model::record_batch>, cluster::errc>>
local_service::consume(
  const model::ktp& ktp,
  kafka::offset start_offset,
  kafka::offset max_offset,
  size_t min_bytes,
  size_t max_bytes,
  model::timeout_clock::duration timeout) {
    auto gh = _gate.hold();

    auto topic_cfg = _metadata_cache->find_topic_cfg(
      model::topic_namespace_view(model::kafka_namespace, ktp.get_topic()));
    if (!topic_cfg) {
        co_return cluster::errc::topic_not_exists;
    }

    auto shard = _partition_manager->shard_owner(ktp);
    if (!shard) {
        co_return cluster::errc::not_leader;
    }

    co_return co_await _partition_manager->consume_from_shard(
      *shard,
      ktp,
      [this, start_offset, max_offset, min_bytes, max_bytes, timeout](
        this auto, kafka::partition_proxy* partition)
        -> ss::future<
          result<chunked_vector<model::record_batch>, cluster::errc>> {
          if (!partition->is_leader()) {
              co_return cluster::errc::not_leader;
          }

          // Create log reader config
          kafka::log_reader_config reader_cfg(
            start_offset,
            max_offset,
            min_bytes,
            max_bytes,
            std::nullopt,  // first_timestamp
            std::ref(_as), // abort_source
            std::nullopt,  // client_address
            false);        // strict_max_bytes

          auto deadline = model::timeout_clock::now() + timeout;

          // Create reader
          auto translating_reader = co_await partition->make_reader(reader_cfg);

          // Consume batches from reader
          try {
              co_return co_await model::consume_reader_to_chunked_vector(
                std::move(translating_reader.reader), deadline);
          } catch (const ss::timed_out_error&) {
              co_return cluster::errc::timeout;
          } catch (...) {
              vlog(
                log.warn,
                "Error consuming from partition {}: {}",
                partition->ntp(),
                std::current_exception());
              co_return cluster::errc::partition_operation_failed;
          }
      });
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
    if constexpr (std::is_same_v<decltype(ntp), const model::ntp&>) {
        if (!_shadow_link_registry->is_topic_mutable(ntp.tp.topic))
          [[unlikely]] {
            co_return cluster::errc::partition_operation_failed;
        }
    } else if constexpr (std::derived_from<decltype(ntp), model::ktp>) {
        if (!_shadow_link_registry->is_topic_mutable(ntp.get_topic()))
          [[unlikely]] {
            co_return cluster::errc::partition_operation_failed;
        }
    } else {
        static_assert(false, "ntp must be model::ntp or model::ktp");
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
    co_return co_await _partition_manager->invoke_on_shard(
      *shard,
      ntp,
      [timeout,
       batches = chunked_vector<model::record_batch>(
         std::from_range, std::move(batches) | std::views::as_rvalue)](
        kafka::partition_proxy* partition) mutable {
          return partition
            ->replicate(std::move(batches), make_replicate_options(timeout))
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
    static constexpr size_t memory_pressure_denominator = 10;
    if (_server_memory != nullptr) {
        auto available = _server_memory->current();
        if (available <= _server_memory_total / memory_pressure_denominator) {
            thread_local static ss::logger::rate_limit rate(1s);
            log.log(
              ss::log_level::warn,
              rate,
              "Rejecting produce request: RPC server memory pressure "
              "(available={}, total={})",
              available,
              _server_memory_total);
            produce_reply reply;
            for (auto& td : req.topic_data) {
                // errc::timeout is used here becuase clients already treat it
                // as a retryable error. for a future major release, it would be
                // better to add some explicit backoff advice to the produce
                // response, similar to how the real kafka API works. for a
                // serde-compatible OOM backstop, this is good enough.
                reply.results.emplace_back(td.tp, cluster::errc::timeout);
            }
            co_return reply;
        }
    }
    co_await ss::coroutine::switch_to(get_scheduling_group());
    auto results = co_await _service->local().produce(
      std::move(req.topic_data), req.timeout);
    co_return produce_reply(std::move(results));
}

ss::future<get_offsets_reply> network_service::get_offsets(
  get_offsets_request req, ::rpc::streaming_context&) {
    co_await ss::coroutine::switch_to(get_scheduling_group());

    auto results = co_await _service->local().get_offsets(
      std::move(req.topics));
    co_return get_offsets_reply(std::move(results));
}

ss::future<consume_reply>
network_service::consume(consume_request req, ::rpc::streaming_context&) {
    co_await ss::coroutine::switch_to(get_scheduling_group());
    co_return co_await _service->local().consume(std::move(req));
}

} // namespace kafka::data::rpc
