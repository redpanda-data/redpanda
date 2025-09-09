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
#include "kafka/data/cloud_topic_partition.h"

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/frontend/errc.h"
#include "cloud_topics/frontend/frontend.h"
#include "cloud_topics/level_zero/frontend_reader/reader.h"
#include "cloud_topics/log_reader_config.h"
#include "cluster/partition.h"
#include "cluster/rm_stm.h"
#include "cluster/types.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/types.h"
#include "logger.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/replicate.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>

#include <optional>
#include <system_error>

namespace {

cloud_topics::cloud_topic_log_reader_config
kafka_to_cloud_topic_log_reader_config(kafka::log_reader_config cfg) {
    return {/*start_offset=*/cfg.start_offset,
            /*max_offset=*/cfg.max_offset,
            /*min_bytes=*/cfg.min_bytes,
            /*max_bytes=*/cfg.max_bytes,
            /*type_filter=*/std::nullopt,
            /*time=*/cfg.first_timestamp,
            /*as=*/cfg.abort_source,
            /*client_addr=*/cfg.client_address,
            /*strict_max_bytes=*/cfg.strict_max_bytes};
}

using frontend_errc = cloud_topics::frontend_errc;

kafka::error_code map_errc(frontend_errc errc) {
    switch (errc) {
    case frontend_errc::offset_not_available:
        return kafka::error_code::offset_not_available;
    case frontend_errc::invalid_topic_exception:
        return kafka::error_code::invalid_topic_exception;
    case frontend_errc::not_leader_for_partition:
        return kafka::error_code::not_leader_for_partition;
    case frontend_errc::offset_out_of_range:
        return kafka::error_code::offset_out_of_range;
    default:
        return kafka::error_code::unknown_server_error;
    }
}

} // namespace

namespace kafka {

cloud_topic_partition::cloud_topic_partition(
  ss::lw_shared_ptr<cluster::partition> p,
  std::unique_ptr<cloud_topics::frontend> fe) noexcept
  : _partition(std::move(p))
  , _fe(std::move(fe)) {}

const model::ntp& cloud_topic_partition::ntp() const { return _fe->ntp(); }

model::offset cloud_topic_partition::local_start_offset() const {
    return kafka::offset_cast(_fe->local_start_offset());
}

model::offset cloud_topic_partition::start_offset() const {
    return kafka::offset_cast(_fe->start_offset());
}

ss::future<result<model::offset, error_code>>
cloud_topic_partition::sync_effective_start(model::timeout_clock::duration d) {
    auto res = co_await _fe->sync_effective_start(d);
    if (!res.has_value()) {
        co_return map_errc(res.error());
    }
    co_return kafka::offset_cast(res.value());
}

model::offset cloud_topic_partition::high_watermark() const {
    return kafka::offset_cast(_fe->high_watermark());
}

checked<model::offset, error_code>
cloud_topic_partition::last_stable_offset() const {
    auto res = _fe->last_stable_offset();
    if (!res.has_value()) {
        return map_errc(res.error());
    }
    return kafka::offset_cast(res.value());
}

bool cloud_topic_partition::is_leader() const { return _fe->is_leader(); }

ss::future<std::error_code> cloud_topic_partition::linearizable_barrier() {
    return _fe->linearizable_barrier();
}

cluster::partition_probe& cloud_topic_partition::probe() {
    // TODO: implement probe for cloud topics
    // This probe reflects only the state of the L0 metadata storage.
    return _partition->probe();
}

kafka::leader_epoch cloud_topic_partition::leader_epoch() const {
    auto term = _fe->leader_epoch();
    return kafka::leader_epoch(static_cast<int32_t>(term()));
}

ss::future<storage::translating_reader> cloud_topic_partition::make_reader(
  kafka::log_reader_config cfg,
  std::optional<model::timeout_clock::time_point> deadline) {
    auto config = kafka_to_cloud_topic_log_reader_config(cfg);
    return _fe->make_reader(config, deadline);
}

ss::future<std::vector<cluster::tx::tx_range>>
cloud_topic_partition::aborted_transactions(
  model::offset base,
  model::offset last,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    // The base and last offsets are kafka offsets here.
    return _fe->aborted_transactions(
      model::offset_cast(base), model::offset_cast(last), std::move(ot_state));
}

ss::future<std::optional<storage::timequery_result>>
cloud_topic_partition::timequery(storage::timequery_config cfg) {
    return _fe->timequery(cfg);
}

ss::future<result<model::offset>> cloud_topic_partition::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {
    auto res = co_await _fe->replicate(std::move(batches), opts);
    if (!res.has_value()) {
        co_return res.error();
    }
    co_return kafka::offset_cast(res.value());
}

ss::future<result<model::offset>> cloud_topic_partition::replicate(
  model::record_batch batch, raft::replicate_options opts) {
    return replicate(
      chunked_vector<model::record_batch>::single(std::move(batch)), opts);
}

raft::replicate_stages cloud_topic_partition::replicate(
  model::batch_identity batch_id,
  model::record_batch batch,
  raft::replicate_options opts) {
    return _fe->replicate(batch_id, std::move(batch), opts);
}

ss::future<std::optional<model::offset>>
cloud_topic_partition::get_leader_epoch_last_offset(
  kafka::leader_epoch epoch) const {
    return _fe->get_leader_epoch_last_offset(model::term_id(epoch()))
      .then([](std::optional<kafka::offset> o) -> std::optional<model::offset> {
          return o.transform(&kafka::offset_cast);
      });
}

ss::future<error_code> cloud_topic_partition::prefix_truncate(
  model::offset, ss::lowres_clock::time_point) {
    /// DeleteRecords API is not supported in cloud topics yet.
    co_return error_code::invalid_topic_exception;
}

ss::future<error_code> cloud_topic_partition::validate_fetch_offset(
  model::offset fetch_offset,
  bool reading_from_follower,
  model::timeout_clock::time_point deadline) {
    auto res = co_await _fe->validate_fetch_offset(
      model::offset_cast(fetch_offset), reading_from_follower, deadline);
    if (!res.has_value()) {
        co_return map_errc(res.error());
    }
    co_return error_code::none;
}

result<partition_info> cloud_topic_partition::get_partition_info() const {
    auto res = _fe->get_partition_info();
    if (!res.has_value()) {
        return map_errc(res.error());
    }
    const auto& ct_info = res.value();
    partition_info info;
    info.replicas.reserve(ct_info.replicas.size());
    for (const auto& ct_replica : ct_info.replicas) {
        replica_info replica;
        replica.id = ct_replica.id;
        replica.high_watermark = kafka::offset_cast(ct_replica.high_watermark);
        replica.log_end_offset = kafka::offset_cast(ct_replica.log_end_offset);
        replica.is_alive = ct_replica.is_alive;
        info.replicas.push_back(replica);
    }
    info.leader = ct_info.leader;
    return info;
}

size_t cloud_topic_partition::estimate_size_between(
  kafka::offset base, kafka::offset last) const {
    return _fe->estimate_size_between(base, last);
}

} // namespace kafka
