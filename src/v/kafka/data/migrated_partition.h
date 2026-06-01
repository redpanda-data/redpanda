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
#pragma once

#include "kafka/data/log_reader_config.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "raft/replicate.h"

#include <seastar/core/coroutine.hh>

#include <memory>
#include <optional>
#include <system_error>

namespace kafka {

/// Composite partition_proxy for a partition mid-migration from tiered storage
/// to cloud topics.
///
/// Reads at or below the migration boundary are served from the tiered-storage
/// path (`_ts`, a replicated_partition's cloud reader); reads above it, and all
/// writes and metadata, go to the cloud-topics path (`_ct`). The boundary comes
/// from the archival_metadata_stm seal. Once the pre-migration TS data ages out
/// the seal is cleared and make_partition_proxy stops constructing this type --
/// the partition then becomes a plain cloud_topic_partition.
class migrated_partition final : public kafka::partition_proxy::impl {
public:
    migrated_partition(
      ss::lw_shared_ptr<cluster::partition> partition,
      kafka::offset boundary,
      std::unique_ptr<kafka::partition_proxy::impl> ts,
      std::unique_ptr<kafka::partition_proxy::impl> ct) noexcept;
    // Out-of-line so the lw_shared_ptr<cluster::partition> member can be
    // destroyed where cluster::partition is a complete type.
    ~migrated_partition() final;

    const model::ntp& ntp() const final;

    ss::future<result<model::offset, error_code>>
      sync_effective_start(model::timeout_clock::duration) final;

    model::offset local_start_offset() const final;

    model::offset start_offset() const final;

    model::offset high_watermark() const final;

    checked<model::offset, error_code> last_stable_offset() const final;

    kafka::leader_epoch leader_epoch() const final;

    ss::future<std::optional<model::offset>>
      get_leader_epoch_last_offset(kafka::leader_epoch) const final;

    bool is_leader() const final;

    ss::future<std::error_code> linearizable_barrier() final;

    ss::future<error_code>
      prefix_truncate(model::offset, ss::lowres_clock::time_point) final;

    ss::future<storage::translating_reader>
      make_reader(kafka::log_reader_config) final;

    ss::future<std::optional<storage::timequery_result>>
      timequery(storage::timequery_config) final;

    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final;

    ss::future<error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) final;

    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch>, raft::replicate_options) final;
    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch,
      raft::replicate_options) final;

    std::unique_ptr<exact_offset_replicator> make_exact_offset_replicator()
      && final;

    result<partition_info> get_partition_info() const final;

    size_t estimate_size_between(kafka::offset, kafka::offset) const final;

    cluster::partition_probe& probe() final;

    size_t local_size_bytes() const final;
    ss::future<std::optional<size_t>> cloud_size_bytes() const final;
    model::offset offset_lag() const final;
    ss::future<cluster::partition_cloud_storage_status>
    get_cloud_storage_status() const final;

private:
    // The underlying partition, used to read the tiered-storage cloud abort
    // index for the pre-migration range (aborted_transactions).
    ss::lw_shared_ptr<cluster::partition> _partition;
    // Last Kafka offset covered by the pre-migration tiered-storage data.
    kafka::offset _boundary;
    // Tiered-storage read path; serves offsets <= _boundary.
    std::unique_ptr<kafka::partition_proxy::impl> _ts;
    // Cloud-topics path; serves offsets > _boundary, all writes, and metadata.
    std::unique_ptr<kafka::partition_proxy::impl> _ct;
};

} // namespace kafka
