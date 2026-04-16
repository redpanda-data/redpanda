/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "kafka/data/cloud_topic_read_replica.h"

#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/log_reader_config.h"
#include "cloud_topics/read_replica/metadata_provider.h"
#include "cloud_topics/read_replica/snapshot_metastore.h"
#include "cloud_topics/read_replica/snapshot_provider.h"
#include "cloud_topics/read_replica/stm.h"
#include "cloud_topics/state_accessors.h"
#include "cluster/partition.h"
#include "kafka/protocol/errors.h"
#include "model/offset_interval.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "storage/log_reader.h"
#include "storage/translating_reader.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::read_replica {

namespace {

// A record_batch_reader::impl that wraps level_one_log_reader_impl and
// owns the snapshot_metastore to ensure proper lifetime management.
class snapshot_level_one_reader : public model::record_batch_reader::impl {
public:
    snapshot_level_one_reader(
      std::unique_ptr<snapshot_metastore> metastore,
      std::unique_ptr<level_one_log_reader_impl> reader)
      : metastore_(std::move(metastore))
      , reader_(std::move(reader)) {}

    bool is_end_of_stream() const override {
        return reader_->is_end_of_stream();
    }

    ss::future<model::record_batch_reader::storage_t>
    do_load_slice(model::timeout_clock::time_point deadline) override {
        return reader_->do_load_slice(deadline);
    }

    void print(std::ostream& o) override { reader_->print(o); }

private:
    // The metastore must be kept alive for the lifetime of the reader
    std::unique_ptr<snapshot_metastore> metastore_;
    std::unique_ptr<level_one_log_reader_impl> reader_;
};

kafka::error_code map_stm_errc(stm::errc e) {
    switch (e) {
    case stm::errc::not_leader:
        return kafka::error_code::not_leader_for_partition;
    case stm::errc::raft_error:
        return kafka::error_code::unknown_server_error;
    case stm::errc::shutting_down:
        return kafka::error_code::not_leader_for_partition;
    }
}

} // namespace

partition_proxy::partition_proxy(
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::shared_ptr<stm> stm,
  cloud_topics::state_accessors* state)
  : partition_(std::move(partition))
  , stm_(std::move(stm))
  , metadata_provider_(state->get_rr_metadata_provider())
  , snapshot_provider_(state->get_rr_snapshot_provider())
  , l1_reader_probe_(state->get_l1_reader_probe())
  , l1_reader_cache_(state->get_l1_reader_cache()) {}

const model::ntp& partition_proxy::ntp() const { return partition_->ntp(); }

ss::future<result<model::offset, kafka::error_code>>
partition_proxy::sync_effective_start(model::timeout_clock::duration timeout) {
    auto sync_result = co_await stm_->sync(timeout);
    if (!sync_result) {
        co_return map_stm_errc(sync_result.error().e);
    }
    co_return kafka::offset_cast(stm_->get_state().start_offset);
}

model::offset partition_proxy::local_start_offset() const {
    // Not applicable for read replicas, return start_offset instead.
    return kafka::offset_cast(stm_->get_state().start_offset);
}

model::offset partition_proxy::start_offset() const {
    return kafka::offset_cast(stm_->get_state().start_offset);
}

model::offset partition_proxy::high_watermark() const {
    return kafka::offset_cast(stm_->get_state().next_offset);
}

checked<model::offset, kafka::error_code>
partition_proxy::last_stable_offset() const {
    return kafka::offset_cast(stm_->get_state().next_offset);
}

kafka::leader_epoch partition_proxy::leader_epoch() const {
    // Return the latest Raft term.
    //
    // NOTE: we aren't mirroring the source cluster here. Within the broker
    // this is checked against the term in the metadata cache, which contains
    // the partition Raft term. This is what we do for tiered storage also.
    auto term = stm_->raft()->confirmed_term();
    return kafka::leader_epoch(static_cast<int32_t>(term()));
}

ss::future<std::optional<model::offset>>
partition_proxy::get_leader_epoch_last_offset(kafka::leader_epoch epoch) const {
    auto snap_res = co_await get_snapshot();
    if (!snap_res.has_value()) {
        co_return std::nullopt;
    }
    auto result = co_await snap_res->metastore->get_end_offset_for_term(
      snap_res->metadata.tidp, model::term_id(epoch()));
    if (!result.has_value()) {
        co_return std::nullopt;
    }

    co_return kafka::offset_cast(result.value());
}

bool partition_proxy::is_leader() const {
    return partition_->raft()->is_leader();
}

ss::future<std::error_code> partition_proxy::linearizable_barrier() {
    auto r = co_await partition_->linearizable_barrier();
    if (r) {
        co_return raft::errc::success;
    }
    co_return r.error();
}

ss::future<kafka::error_code>
partition_proxy::prefix_truncate(model::offset, ss::lowres_clock::time_point) {
    co_return kafka::error_code::operation_not_attempted;
}

ss::future<storage::translating_reader>
partition_proxy::make_reader(kafka::log_reader_config cfg) {
    auto snap_res = co_await get_snapshot();
    if (!snap_res.has_value()) {
        throw std::runtime_error(
          fmt::format("make_reader failed: {}", snap_res.error()));
    }
    co_return co_await make_reader(std::move(snap_res.value()), cfg);
}

ss::future<storage::translating_reader> partition_proxy::make_reader(
  partition_proxy::snapshot snap, kafka::log_reader_config cfg) {
    cloud_topic_log_reader_config cloud_cfg(
      cfg.start_offset,
      cfg.max_offset,
      cfg.min_bytes,
      cfg.max_bytes,
      std::nullopt,
      cfg.first_timestamp,
      cfg.abort_source,
      cfg.client_address,
      cfg.strict_max_bytes);

    // Create the level one reader using the snapshot's bucket-specific IO.
    auto reader_impl = std::make_unique<level_one_log_reader_impl>(
      cloud_cfg,
      partition_->ntp(),
      snap.metadata.tidp,
      snap.metastore.get(),
      snap.io,
      l1_reader_probe_,
      l1_reader_cache_);

    // Create an owning reader that keeps metastore alive for the reader
    // lifetime.
    auto reader = model::make_record_batch_reader<snapshot_level_one_reader>(
      std::move(snap.metastore), std::move(reader_impl));

    co_return storage::translating_reader(std::move(reader));
}

ss::future<std::optional<storage::timequery_result>>
partition_proxy::timequery(storage::timequery_config cfg) {
    auto snap_res = co_await get_snapshot();
    if (!snap_res.has_value()) {
        co_return std::nullopt;
    }
    auto& metastore = *snap_res->metastore;

    // Find the first extent with max_timestamp >= cfg.time.
    auto extent_res = co_await metastore.get_first_ge(
      snap_res->metadata.tidp, model::offset_cast(cfg.min_offset), cfg.time);
    if (!extent_res.has_value()) {
        co_return std::nullopt;
    }
    const auto& extent = extent_res.value();

    // Refine further by reading the extent's batches and scanning
    // record-by-record to find the exact offset whose timestamp >= cfg.time.
    // Clamp the reader range to respect cfg bounds so batches before
    // start_offset (e.g. after delete_records) are not yielded.
    auto read_start = std::max(
      extent.first_offset, model::offset_cast(cfg.min_offset));
    auto read_end = std::min(
      extent.last_offset, model::offset_cast(cfg.max_offset));
    kafka::log_reader_config reader_cfg(read_start, read_end, cfg.abort_source);
    auto reader = co_await make_reader(std::move(snap_res.value()), reader_cfg);
    auto generator = std::move(reader.reader).generator(model::no_timeout);
    while (auto batch_opt = co_await generator()) {
        auto& batch = batch_opt->get();
        if (cfg.time > batch.header().max_timestamp) {
            continue;
        }
        co_return co_await storage::batch_timequery(
          std::move(batch), cfg.min_offset, cfg.time, cfg.max_offset);
    }
    co_return std::nullopt;
}

ss::future<std::vector<model::tx_range>> partition_proxy::aborted_transactions(
  model::offset,
  model::offset,
  ss::lw_shared_ptr<const storage::offset_translator_state>) {
    // Data in L1 is all committed.
    co_return std::vector<model::tx_range>{};
}

ss::future<kafka::error_code> partition_proxy::validate_fetch_offset(
  model::offset fetch_offset, bool, model::timeout_clock::time_point) {
    auto start = stm_->get_state().start_offset;
    auto hwm = stm_->get_state().next_offset;

    auto kafka_fetch_offset = model::offset_cast(fetch_offset);

    if (kafka_fetch_offset < start) {
        co_return kafka::error_code::offset_out_of_range;
    }

    if (kafka_fetch_offset > hwm) {
        co_return kafka::error_code::offset_not_available;
    }
    co_return kafka::error_code::none;
}

ss::future<result<model::offset>> partition_proxy::replicate(
  chunked_vector<model::record_batch>, raft::replicate_options) {
    co_return kafka::error_code::invalid_topic_exception;
}

raft::replicate_stages partition_proxy::replicate(
  model::batch_identity, model::record_batch, raft::replicate_options) {
    return {
      ss::now(),
      ss::make_ready_future<result<raft::replicate_result>>(
        make_error_code(kafka::error_code::invalid_topic_exception))};
}

std::unique_ptr<kafka::exact_offset_replicator>
partition_proxy::make_exact_offset_replicator() && {
    return nullptr;
}

result<kafka::partition_info> partition_proxy::get_partition_info() const {
    kafka::partition_info ret;
    ret.leader = partition_->get_leader_id();
    ret.replicas.reserve(partition_->raft()->get_follower_count() + 1);
    auto followers = partition_->get_follower_metrics();
    if (followers.has_error()) {
        return followers.error();
    }
    auto hwm = high_watermark();
    for (const auto& follower_metric : followers.value()) {
        ret.replicas.push_back(
          kafka::replica_info{
            .id = follower_metric.id,
            .high_watermark = hwm,
            .log_end_offset = hwm,
            .is_alive = follower_metric.is_live,
          });
    }

    ret.replicas.push_back(
      kafka::replica_info{
        .id = partition_->raft()->self().id(),
        .high_watermark = hwm,
        .log_end_offset = hwm,
        .is_alive = true,
      });

    return {std::move(ret)};
}

size_t
partition_proxy::estimate_size_between(kafka::offset, kafka::offset) const {
    // Not supported for read replicas
    return 0;
}

cluster::partition_probe& partition_proxy::probe() {
    return partition_->probe();
}

size_t partition_proxy::local_size_bytes() const {
    return partition_->size_bytes();
};

ss::future<std::optional<cloud_topics::l1::metastore::size_response>>
partition_proxy::get_metastore_size() const {
    auto snap_res = co_await get_snapshot();
    if (!snap_res.has_value()) {
        co_return std::nullopt;
    }
    auto size_res = co_await snap_res->metastore->get_size(
      snap_res->metadata.tidp);
    if (!size_res.has_value()) {
        co_return std::nullopt;
    }
    co_return size_res.value();
}

ss::future<std::optional<size_t>> partition_proxy::cloud_size_bytes() const {
    auto size_res = co_await get_metastore_size();
    if (!size_res) {
        co_return std::nullopt;
    }
    co_return size_res->size;
}

model::offset partition_proxy::offset_lag() const { return model::offset(0); }

ss::future<cluster::partition_cloud_storage_status>
partition_proxy::get_cloud_storage_status() const {
    cluster::partition_cloud_storage_status status{};
    auto local_size = local_size_bytes();
    auto cloud_size = (co_await get_metastore_size())
                        .value_or(cloud_topics::l1::metastore::size_response{});
    status.mode = cluster::cloud_storage_mode::cloud_topic_read_replica;
    status.local_log_size_bytes = local_size;
    // A cloud topic read replica only has access to L1 data, and therefore only
    // reports L1 data size for both "cloud" and "total" log size bytes.
    status.cloud_log_size_bytes = cloud_size.size;
    status.total_log_size_bytes = cloud_size.size;
    status.stm_region_segment_count = cloud_size.num_extents;
    co_return status;
}

ss::future<std::expected<partition_proxy::snapshot, ss::sstring>>
partition_proxy::get_snapshot() const {
    auto metadata = metadata_provider_->get_metadata(ntp());
    if (!metadata) {
        co_return std::unexpected(
          fmt::format("metadata not found for {}", ntp()));
    }

    auto domain = stm_->domain();
    if (domain == l1::domain_uuid{}) {
        co_return std::unexpected(
          fmt::format("domain not discovered for {}", ntp()));
    }

    auto replicated_seqno = stm_->get_state().seqno;
    auto earliest_snap_time = metadata->earliest_snapshot_time();
    auto snapshot_res = co_await snapshot_provider_->get_snapshot(
      domain, metadata->bucket, earliest_snap_time, replicated_seqno, 30s);
    if (!snapshot_res) {
        co_return std::unexpected(
          fmt::format(
            "error getting snapshot for domain {} bucket {}, earliest refresh "
            "time: {}, min_seqno: {}: {}",
            domain,
            metadata->bucket,
            earliest_snap_time.time_since_epoch(),
            replicated_seqno,
            snapshot_res.error()));
    }
    co_return snapshot{
      .metadata = *metadata,
      .metastore = std::move(snapshot_res->metastore),
      .io = snapshot_res->io,
    };
}

} // namespace cloud_topics::read_replica
