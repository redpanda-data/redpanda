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
#include "kafka/server/partition_proxy.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "resource_mgmt/io_priority.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"
#include "transform/rpc/deps.h"
#include "transform/rpc/serde.h"
#include "utils/uuid.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>

#include <algorithm>
#include <iterator>
#include <system_error>
#include <utility>
#include <vector>

namespace transform::rpc {
namespace {

static constexpr auto coordinator_partition = model::partition_id{0};

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

iobuf make_iobuf(ss::sstring str) {
    iobuf b;
    b.append(str.data(), str.size());
    return b;
}

iobuf make_iobuf(uuid_t uuid) {
    iobuf b;
    b.append(uuid.mutable_uuid().begin(), uuid.length);
    return b;
}
model::record_header make_header(ss::sstring k, ss::sstring v) {
    auto key = make_iobuf(std::move(k));
    auto ks = int32_t(key.size_bytes());
    auto value = make_iobuf(std::move(v));
    auto vs = int32_t(value.size_bytes());
    return {ks, std::move(key), vs, std::move(value)};
}
} // namespace

local_service::local_service(
  std::unique_ptr<topic_metadata_cache> metadata_cache,
  std::unique_ptr<partition_manager> partition_manager)
  : _metadata_cache(std::move(metadata_cache))
  , _partition_manager(std::move(partition_manager)) {}

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
    auto result = co_await produce(ktp, std::move(data.batches), timeout);
    auto ec = result.has_error() ? result.error() : cluster::errc::success;
    co_return transformed_topic_data_result(data.tp, ec);
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
    // TODO: schema validation
    model::offset produced_offset;
    auto ec = co_await _partition_manager->invoke_on_shard(
      *shard,
      ntp,
      [timeout, r = std::move(rdr), &produced_offset](
        kafka::partition_proxy* partition) mutable {
          return partition
            ->replicate(std::move(r), make_replicate_options(timeout))
            .then([&produced_offset](result<model::offset> r) {
                if (r.has_error()) {
                    return map_errc(r.assume_error());
                }
                produced_offset = r.value();
                return cluster::errc::success;
            });
      });
    if (ec == cluster::errc::success) {
        co_return produced_offset;
    }
    co_return ec;
}

ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
local_service::store_wasm_binary(
  iobuf data, model::timeout_clock::duration timeout) {
    uuid_t key = uuid_t::create();
    storage::record_batch_builder b(
      model::record_batch_type::raft_data, model::offset(0));
    std::vector<model::record_header> headers;
    headers.push_back(make_header("state", "live"));
    b.add_raw_kw(make_iobuf(key), std::move(data), std::move(headers));
    ss::chunked_fifo<model::record_batch> batches;
    batches.push_back(std::move(b).build());
    auto r = co_await produce(
      model::wasm_binaries_internal_ntp, std::move(batches), timeout);
    using result = result<stored_wasm_binary_metadata, cluster::errc>;
    if (r.has_error()) {
        co_return result(r.error());
    }
    co_return result(stored_wasm_binary_metadata(key, r.value()));
}

ss::future<cluster::errc> local_service::delete_wasm_binary(
  uuid_t key, model::timeout_clock::duration timeout) {
    storage::record_batch_builder b(
      model::record_batch_type::raft_data, model::offset(0));
    std::vector<model::record_header> headers;
    headers.push_back(make_header("state", "tombstone"));
    b.add_raw_kw(make_iobuf(key), std::nullopt, std::move(headers));
    ss::chunked_fifo<model::record_batch> batches;
    batches.push_back(std::move(b).build());
    auto r = co_await produce(
      model::wasm_binaries_internal_ntp, std::move(batches), timeout);
    co_return r.has_error() ? r.error() : cluster::errc::success;
}

ss::future<result<iobuf, cluster::errc>> local_service::load_wasm_binary(
  model::offset offset, model::timeout_clock::duration timeout) {
    auto shard = _partition_manager->shard_owner(
      model::wasm_binaries_internal_ntp);
    if (!shard) {
        co_return cluster::errc::not_leader;
    }
    iobuf data;
    auto ec = co_await _partition_manager->invoke_on_shard(
      *shard,
      model::wasm_binaries_internal_ntp,
      [this, offset, timeout, &data](
        kafka::partition_proxy* partition) mutable {
          storage::log_reader_config reader_config(
            /*start_offset=*/offset,
            /*max_offset=*/model::next_offset(offset),
            /*min_bytes=*/0,
            /*max_bytes=*/1,
            /*prio=*/wasm_read_priority(),
            /*type_filter=*/std::nullopt,
            /*time=*/std::nullopt,
            /*as=*/std::nullopt);
          model::timeout_clock::time_point deadline
            = model::timeout_clock::now() + timeout;
          return partition->make_reader(reader_config, deadline)
            .then([this, timeout](storage::translating_reader rdr) {
                return consume_wasm_binary_reader(
                  std::move(rdr.reader), timeout);
            })
            .then([&data](result<iobuf, cluster::errc> r) {
                if (r.has_error()) {
                    return r.error();
                }
                data = std::move(r).value();
                return cluster::errc::success;
            });
      });
    if (ec != cluster::errc::success) {
        co_return ec;
    }
    co_return data;
}

ss::future<result<iobuf, cluster::errc>>
local_service::consume_wasm_binary_reader(
  model::record_batch_reader rdr, model::timeout_clock::duration timeout) {
    model::timeout_clock::time_point deadline = model::timeout_clock::now()
                                                + timeout;
    auto batches = co_await model::consume_reader_to_memory(
      std::move(rdr), deadline);
    if (batches.empty()) {
        co_return cluster::errc::invalid_request;
    }
    auto& batch = batches.front();
    auto records = batch.copy_records();
    if (records.empty()) {
        co_return cluster::errc::invalid_request;
    }
    iobuf data = records.front().release_value();
    if (data.empty()) {
        co_return cluster::errc::invalid_request;
    }
    co_return data;
}

ss::future<find_coordinator_response>
local_service::find_coordinator(find_coordinator_request&& request) {
    model::ntp ntp(
      model::kafka_internal_namespace,
      model::transform_offsets_topic,
      coordinator_partition);
    auto shard = _partition_manager->shard_owner(ntp);
    if (!shard) {
        find_coordinator_response response;
        response.ec = cluster::errc::not_leader;
        co_return response;
    }
    co_return co_await _partition_manager->invoke_on_shard(
      *shard,
      [this, req = std::move(request), ntp](
        cluster::partition_manager& local) mutable {
          return do_find_coordinator(local.get(ntp), std::move(req));
      });
}

ss::future<find_coordinator_response> local_service::do_find_coordinator(
  ss::lw_shared_ptr<cluster::partition> partition,
  find_coordinator_request&& request) {
    find_coordinator_response response;
    if (!partition) {
        response.ec = cluster::errc::not_leader;
        co_return response;
    }
    auto stm = partition->transform_offsets_stm();
    if (partition->ntp().tp.partition != coordinator_partition) {
        response.ec = cluster::errc::not_leader;
        co_return response;
    }
    for (auto& key : request.keys) {
        auto coordinator = co_await stm->coordinator(key);
        if (!coordinator) {
            response.ec = map_errc(coordinator.error());
            response.coordinators.clear();
            co_return response;
        }
        response.coordinators[key] = coordinator.value();
    }
    response.ec = cluster::errc::success;
    co_return response;
}

ss::future<offset_commit_response>
local_service::offset_commit(offset_commit_request request) {
    model::ntp ntp(
      model::kafka_internal_namespace,
      model::transform_offsets_topic,
      request.coordinator);
    auto shard = _partition_manager->shard_owner(ntp);
    if (!shard) {
        offset_commit_response response{};
        response.errc = cluster::errc::not_leader;
        co_return response;
    }
    co_return co_await _partition_manager->invoke_on_shard(
      *shard, [this, request, ntp](cluster::partition_manager& local) mutable {
          return do_local_offset_commit(local.get(ntp), request);
      });
}

ss::future<offset_commit_response> local_service::do_local_offset_commit(
  ss::lw_shared_ptr<cluster::partition> partition, offset_commit_request req) {
    offset_commit_response response{};
    if (req.kvs.empty()) {
        response.errc = cluster::errc::success;
        co_return response;
    }
    if (!partition) {
        response.errc = cluster::errc::not_leader;
        co_return response;
    }
    auto stm = partition->transform_offsets_stm();
    response.errc = co_await stm->put(std::move(req.kvs));
    co_return response;
}

ss::future<offset_fetch_response>
local_service::offset_fetch(offset_fetch_request request) {
    model::ntp ntp(
      model::kafka_internal_namespace,
      model::transform_offsets_topic,
      request.coordinator);
    offset_fetch_response response{};
    auto shard = _partition_manager->shard_owner(ntp);
    if (!shard) {
        response.errc = cluster::errc::not_leader;
        co_return response;
    }
    co_return co_await _partition_manager->invoke_on_shard(
      *shard, [this, request, ntp](cluster::partition_manager& local) mutable {
          return do_local_offset_fetch(local.get(ntp), request);
      });
}

ss::future<offset_fetch_response> local_service::do_local_offset_fetch(
  ss::lw_shared_ptr<cluster::partition> partition,
  offset_fetch_request request) {
    offset_fetch_response response{};
    if (!partition) {
        response.errc = cluster::errc::not_leader;
        co_return response;
    }
    auto stm = partition->transform_offsets_stm();
    auto result = co_await stm->get(request.key);
    if (!result) {
        response.errc = map_errc(result.error());
        co_return response;
    }
    response.errc = cluster::errc::success;
    response.result = result.value();
    co_return response;
}

ss::future<produce_reply>
network_service::produce(produce_request&& req, ::rpc::streaming_context&) {
    auto results = co_await _service->local().produce(
      std::move(req.topic_data), req.timeout);
    co_return produce_reply(std::move(results));
}

ss::future<delete_wasm_binary_reply> network_service::delete_wasm_binary(
  delete_wasm_binary_request&& req, ::rpc::streaming_context&) {
    auto results = co_await _service->local().delete_wasm_binary(
      req.key, req.timeout);
    co_return delete_wasm_binary_reply(results);
}

ss::future<load_wasm_binary_reply> network_service::load_wasm_binary(
  load_wasm_binary_request&& req, ::rpc::streaming_context&) {
    auto results = co_await _service->local().load_wasm_binary(
      req.offset, req.timeout);
    if (results.has_error()) {
        co_return load_wasm_binary_reply(results.error(), {});
    }
    co_return load_wasm_binary_reply(
      cluster::errc::success, std::move(results.value()));
}

ss::future<store_wasm_binary_reply> network_service::store_wasm_binary(
  store_wasm_binary_request&& req, ::rpc::streaming_context&) {
    auto results = co_await _service->local().store_wasm_binary(
      std::move(req.data), req.timeout);
    if (results.has_error()) {
        co_return store_wasm_binary_reply(results.error(), {});
    }
    co_return store_wasm_binary_reply(cluster::errc::success, results.value());
}

ss::future<find_coordinator_response> network_service::find_coordinator(
  find_coordinator_request&& req, ::rpc::streaming_context&) {
    co_return co_await _service->local().find_coordinator(std::move(req));
}

ss::future<offset_fetch_response> network_service::offset_fetch(
  offset_fetch_request&& req, ::rpc::streaming_context&) {
    co_return co_await _service->local().offset_fetch(req);
}

ss::future<offset_commit_response> network_service::offset_commit(
  offset_commit_request&& req, ::rpc::streaming_context&) {
    co_return co_await _service->local().offset_commit(req);
}

} // namespace transform::rpc
